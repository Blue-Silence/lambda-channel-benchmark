use std::collections::BTreeMap;
use std::sync::Arc;

use futures::future::join_all;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::blob_store_factory::unique_resource_id;
use crate::config::{ExperimentSpec, InstanceConfig, InstancesConfig};
use crate::driver::paced::PacedTaskRunReport;
use crate::driver::sweep::{SweepDecision, SweepStopReason, ThroughputSweepPolicy};
use crate::experiments::blob::control::{
    begin_on_target, init_blob_store_on_target_with_request, prepare_blob_get_append_on_target,
    prepare_blob_get_begin_on_target, prepare_blob_get_finish_on_target,
    put_blob_batch_on_target_chunked_refs, reset_on_target, start_prepared_blob_get_on_target,
};
use crate::rpc::protocol::{
    BlobPutResult, InitBlobStoreRequest, PacedBlobGetResult, PrepareBlobGetAppendRequest,
    PrepareBlobGetBeginRequest, PrepareBlobGetFinishRequest, PutBlobBatchRequest,
    StartPreparedBlobGetRequest,
};
use crate::rpc::server::state::NodeRuntimeState;

const REF_CHUNK_SIZE: usize = 1024;

pub(super) async fn run(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    instances: &InstancesConfig,
    experiment: ExperimentSpec,
) -> Result<String, String> {
    let backend = experiment.benchmark.backend.trim().to_ascii_lowercase();
    if !matches!(backend.as_str(), "s3" | "p2p") {
        return Err("blob.single_getter requires benchmark.backend = s3 or p2p".to_string());
    }
    if backend == "p2p" && experiment.p2p.tracker_backend.trim() != "dynamodb" {
        return Err("blob.single_getter p2p requires p2p.tracker_backend = dynamodb".to_string());
    }
    let duration_seconds = experiment
        .benchmark
        .duration_seconds
        .ok_or_else(|| "blob.single_getter requires benchmark.duration_seconds".to_string())?;
    let getter_id = getter_id(&experiment)?;
    let putter_ids = putter_ids(&experiment, &getter_id)?;
    if putter_ids.is_empty() {
        return Err("blob.single_getter requires at least one putter participant".to_string());
    }

    let policy = sweep_policy(&experiment)?;
    let target_rates = target_rates(&experiment, &policy)?;
    let mut datapoints = Vec::new();
    let mut stop_reason = SweepStopReason::MaxPoints;
    for (index, target_rate) in target_rates.into_iter().enumerate() {
        let datapoint = run_datapoint(
            instance,
            runtime,
            instances,
            &experiment,
            &backend,
            &getter_id,
            &putter_ids,
            duration_seconds,
            target_rate,
            index,
        )
        .await?;
        let decision = policy.decide_after_metrics(
            datapoints.len() + 1,
            datapoint.paced.target_ops_per_s,
            datapoint.paced.successful_ops_per_s,
            datapoint.paced.failed_tasks,
        );
        datapoints.push(datapoint);
        if let SweepDecision::Stop { reason } = decision {
            stop_reason = reason;
            break;
        }
    }

    let report = SingleGetterReport {
        run_id: experiment.run.run_id.clone(),
        workload: experiment.run.workload.clone(),
        instance_id: getter_id.clone(),
        backend,
        operations_per_point: experiment.benchmark.operations,
        warmup_operations_per_point: experiment.benchmark.warmup_operations,
        duration_seconds,
        concurrency: experiment.benchmark.concurrency,
        object_size_bytes: experiment.benchmark.object_size_bytes,
        getter_instance_id: getter_id,
        putter_count: putter_ids.len(),
        putter_instance_ids: putter_ids,
        stop_reason,
        datapoints,
    };
    serde_json::to_string(&report).map_err(|err| format!("failed to serialize report: {err}"))
}

#[allow(clippy::too_many_arguments)]
async fn run_datapoint(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    instances: &InstancesConfig,
    experiment: &ExperimentSpec,
    backend: &str,
    getter_id: &str,
    putter_ids: &[String],
    duration_seconds: f64,
    target_rate: f64,
    datapoint_index: usize,
) -> Result<SingleGetterDatapointReport, String> {
    let total_refs = operations_for_rate(target_rate, duration_seconds)?;
    let distribution = even_distribution(total_refs, putter_ids);
    let resource_id = unique_resource_id(&experiment.run.run_id, getter_id);
    let run_id = experiment.run.run_id.clone();

    begin_on_target(
        instances,
        instance,
        runtime,
        getter_id,
        &run_id,
        experiment.coordination.force_reset_on_start,
    )
    .await?;
    init_blob_store_on_target_with_request(
        instances,
        instance,
        runtime,
        getter_id,
        InitBlobStoreRequest {
            run_id: run_id.clone(),
            backend: backend.to_string(),
            root_dir: None,
            force_reinit: true,
            experiment: Some(experiment.clone()),
            resource_id: Some(resource_id.clone()),
            create_remote_resources: true,
        },
    )
    .await?;

    let putter_init_futures = putter_ids.iter().map(|putter_id| async {
        begin_on_target(
            instances,
            instance,
            runtime,
            putter_id,
            &run_id,
            experiment.coordination.force_reset_on_start,
        )
        .await?;
        init_blob_store_on_target_with_request(
            instances,
            instance,
            runtime,
            putter_id,
            InitBlobStoreRequest {
                run_id: run_id.clone(),
                backend: backend.to_string(),
                root_dir: None,
                force_reinit: true,
                experiment: Some(experiment.clone()),
                resource_id: Some(resource_id.clone()),
                create_remote_resources: false,
            },
        )
        .await
    });
    for result in join_all(putter_init_futures).await {
        result?;
    }

    let preload_put_concurrency = preload_put_concurrency(experiment)?;
    let put_started = std::time::Instant::now();
    let put_futures =
        distribution
            .iter()
            .filter(|(_, count)| *count > 0)
            .map(|(putter_id, count)| {
                put_blob_batch_on_target_chunked_refs(
                    instances,
                    instance,
                    runtime,
                    putter_id,
                    experiment.coordination.barrier_timeout_ms,
                    PutBlobBatchRequest {
                        run_id: run_id.clone(),
                        count: *count,
                        object_size_bytes: experiment.benchmark.object_size_bytes,
                        max_in_flight: preload_put_concurrency,
                    },
                )
            });
    let put_results = join_all(put_futures).await;
    let preload_put_ms = put_started.elapsed().as_secs_f64() * 1000.0;

    let mut per_putter = Vec::new();
    let mut refs_by_putter = Vec::new();
    for ((putter_id, count), result) in distribution
        .iter()
        .filter(|(_, count)| *count > 0)
        .zip(put_results)
    {
        let result = result?;
        if result.refs.len() != *count {
            return Err(format!(
                "putter {putter_id} returned {} refs, expected {count}",
                result.refs.len()
            ));
        }
        refs_by_putter.push(result.refs.clone());
        per_putter.push(PutterReport::from_result(putter_id, *count, result));
    }
    let refs = round_robin_refs(&refs_by_putter);
    if refs.len() != total_refs {
        return Err(format!(
            "round-robin ref list has {} refs, expected {total_refs}",
            refs.len()
        ));
    }

    let plan_id = getter_plan_id(datapoint_index, getter_id);
    prepare_blob_get_begin_on_target(
        instances,
        instance,
        runtime,
        getter_id,
        PrepareBlobGetBeginRequest {
            run_id: run_id.clone(),
            plan_id: plan_id.clone(),
            expected_ref_count: refs.len(),
            target_ops_per_s: target_rate,
            max_in_flight: experiment.benchmark.concurrency,
        },
    )
    .await?;
    for (chunk_index, refs) in refs.chunks(REF_CHUNK_SIZE).enumerate() {
        prepare_blob_get_append_on_target(
            instances,
            instance,
            runtime,
            getter_id,
            PrepareBlobGetAppendRequest {
                run_id: run_id.clone(),
                plan_id: plan_id.clone(),
                chunk_index: chunk_index as u64,
                refs: refs.to_vec(),
            },
        )
        .await?;
    }
    prepare_blob_get_finish_on_target(
        instances,
        instance,
        runtime,
        getter_id,
        PrepareBlobGetFinishRequest {
            run_id: run_id.clone(),
            plan_id: plan_id.clone(),
        },
    )
    .await?;

    let get_result = start_prepared_blob_get_on_target(
        instances,
        instance,
        runtime,
        getter_id,
        experiment.coordination.barrier_timeout_ms,
        StartPreparedBlobGetRequest {
            run_id: run_id.clone(),
            plan_id,
            start_after_unix_ns: None,
        },
    )
    .await?;

    let mut getter = GetterReport::from_result(getter_id, get_result);
    let mut paced = getter.paced.clone();
    paced.samples.clear();
    getter.paced.samples.clear();

    let _ = reset_on_target(instances, instance, runtime, getter_id, &run_id, true).await;
    for putter_id in putter_ids {
        let _ = reset_on_target(instances, instance, runtime, putter_id, &run_id, true).await;
    }

    let refs_chunk_count = total_refs.div_ceil(REF_CHUNK_SIZE);
    Ok(SingleGetterDatapointReport {
        run_id,
        workload: experiment.run.workload.clone(),
        instance_id: getter_id.to_string(),
        backend: backend.to_string(),
        resource_id,
        datapoint_index,
        getter_instance_id: getter_id.to_string(),
        putter_count: putter_ids.len(),
        putter_instance_ids: putter_ids.to_vec(),
        operations: total_refs as u64,
        warmup_operations: 0,
        duration_seconds,
        object_size_bytes: experiment.benchmark.object_size_bytes,
        total_bytes: getter.total_bytes,
        working_set_size: total_refs as u64,
        refs_distribution: "even-round-robin".to_string(),
        getter_reuse_policy: "no-repeat".to_string(),
        cross_getter_reuse: false,
        refs_chunk_size: REF_CHUNK_SIZE as u64,
        refs_chunk_count: refs_chunk_count as u64,
        aggregate_target_ops_per_s: target_rate,
        preload_put_ms,
        preload_put_concurrency: preload_put_concurrency as u64,
        preload_put_ops_per_s: if preload_put_ms > 0.0 {
            total_refs as f64 / (preload_put_ms / 1000.0)
        } else {
            0.0
        },
        expected_working_set_bytes: total_refs as u64 * experiment.benchmark.object_size_bytes,
        store: BTreeMap::new(),
        paced,
        getter,
        per_putter,
    })
}

fn getter_plan_id(datapoint_index: usize, getter_id: &str) -> String {
    format!("datapoint-{datapoint_index:06}-{getter_id}")
}

fn getter_id(experiment: &ExperimentSpec) -> Result<String, String> {
    experiment
        .participants
        .iter()
        .find(|participant| {
            matches!(
                participant.label.as_deref(),
                Some("getter") | Some("orchestrator")
            )
        })
        .or_else(|| experiment.participants.first())
        .map(|participant| participant.instance_id.clone())
        .ok_or_else(|| "blob.single_getter requires participants".to_string())
}

fn putter_ids(experiment: &ExperimentSpec, getter_id: &str) -> Result<Vec<String>, String> {
    let labeled: Vec<String> = experiment
        .participants
        .iter()
        .filter(|participant| {
            matches!(
                participant.label.as_deref(),
                Some("putter") | Some("holder")
            )
        })
        .map(|participant| participant.instance_id.clone())
        .collect();
    if !labeled.is_empty() {
        return Ok(labeled);
    }
    Ok(experiment
        .participants
        .iter()
        .map(|participant| participant.instance_id.clone())
        .filter(|instance_id| instance_id != getter_id)
        .collect())
}

fn sweep_policy(experiment: &ExperimentSpec) -> Result<ThroughputSweepPolicy, String> {
    let mut policy = ThroughputSweepPolicy::paper_default(experiment.benchmark.offered_rate_per_s);
    let sweep = &experiment.throughput_sweep;
    if let Some(value) = sweep.start_ops_per_s {
        policy.start_ops_per_s = value;
    }
    if let Some(value) = sweep.step_multiplier {
        policy.step_multiplier = value;
    }
    if let Some(value) = sweep.max_ops_per_s {
        policy.max_ops_per_s = value;
    }
    if let Some(value) = sweep.max_points {
        policy.max_points = value;
    }
    if let Some(value) = sweep.saturation_achieved_ratio {
        policy.saturation_achieved_ratio = value;
    }
    if let Some(value) = sweep.stop_on_failure {
        policy.stop_on_failure = value;
    }
    policy.validate()?;
    Ok(policy)
}

fn target_rates(
    experiment: &ExperimentSpec,
    policy: &ThroughputSweepPolicy,
) -> Result<Vec<f64>, String> {
    if !experiment.throughput_sweep.points_ops_per_s.is_empty() {
        return Ok(experiment.throughput_sweep.points_ops_per_s.clone());
    }
    if let Some(rate) = experiment.benchmark.offered_rate_per_s {
        return Ok(vec![rate]);
    }
    let mut rates = Vec::with_capacity(policy.max_points);
    let mut rate = policy.start_ops_per_s;
    for _ in 0..policy.max_points {
        if rate > policy.max_ops_per_s {
            break;
        }
        rates.push(rate);
        rate *= policy.step_multiplier;
    }
    if rates.is_empty() {
        Err("blob.single_getter throughput sweep generated no target rates".to_string())
    } else {
        Ok(rates)
    }
}

fn operations_for_rate(rate: f64, duration_seconds: f64) -> Result<usize, String> {
    if !rate.is_finite() || rate <= 0.0 {
        return Err("target rate must be a finite positive number".to_string());
    }
    let operations = (rate * duration_seconds).ceil().max(1.0);
    if operations > usize::MAX as f64 {
        return Err("rate * duration is too large for this platform".to_string());
    }
    Ok(operations as usize)
}

fn preload_put_concurrency(experiment: &ExperimentSpec) -> Result<usize, String> {
    let value = experiment
        .env
        .get("LC_BENCH_BLOB_PRELOAD_CONCURRENCY")
        .or_else(|| {
            experiment
                .env
                .get("LC_BENCH_BLOB_SINGLE_GETTER_PRELOAD_CONCURRENCY")
        })
        .map(|raw| {
            raw.parse::<usize>()
                .map_err(|err| format!("invalid blob preload concurrency {raw:?}: {err}"))
        })
        .transpose()?
        .unwrap_or(experiment.benchmark.concurrency);
    if value == 0 {
        return Err("blob preload concurrency must be greater than zero".to_string());
    }
    Ok(value)
}

fn even_distribution(total_refs: usize, putter_ids: &[String]) -> Vec<(String, usize)> {
    let base = total_refs / putter_ids.len();
    let extra = total_refs % putter_ids.len();
    putter_ids
        .iter()
        .enumerate()
        .map(|(index, putter_id)| {
            let count = base + usize::from(index < extra);
            (putter_id.clone(), count)
        })
        .collect()
}

fn round_robin_refs(refs_by_putter: &[Vec<serde_json::Value>]) -> Vec<serde_json::Value> {
    let total = refs_by_putter.iter().map(Vec::len).sum();
    let max_len = refs_by_putter.iter().map(Vec::len).max().unwrap_or(0);
    let mut refs = Vec::with_capacity(total);
    for index in 0..max_len {
        for putter_refs in refs_by_putter {
            if let Some(reference) = putter_refs.get(index) {
                refs.push(reference.clone());
            }
        }
    }
    refs
}

#[derive(Debug, Serialize)]
struct SingleGetterReport {
    run_id: String,
    workload: String,
    instance_id: String,
    backend: String,
    operations_per_point: u64,
    warmup_operations_per_point: u64,
    duration_seconds: f64,
    concurrency: usize,
    object_size_bytes: u64,
    getter_instance_id: String,
    putter_count: usize,
    putter_instance_ids: Vec<String>,
    stop_reason: SweepStopReason,
    datapoints: Vec<SingleGetterDatapointReport>,
}

#[derive(Debug, Serialize)]
struct SingleGetterDatapointReport {
    run_id: String,
    workload: String,
    instance_id: String,
    backend: String,
    resource_id: String,
    datapoint_index: usize,
    getter_instance_id: String,
    putter_count: usize,
    putter_instance_ids: Vec<String>,
    operations: u64,
    warmup_operations: u64,
    duration_seconds: f64,
    object_size_bytes: u64,
    total_bytes: u64,
    working_set_size: u64,
    refs_distribution: String,
    getter_reuse_policy: String,
    cross_getter_reuse: bool,
    refs_chunk_size: u64,
    refs_chunk_count: u64,
    aggregate_target_ops_per_s: f64,
    preload_put_ms: f64,
    preload_put_concurrency: u64,
    preload_put_ops_per_s: f64,
    expected_working_set_bytes: u64,
    store: BTreeMap<String, String>,
    paced: PacedTaskRunReport,
    getter: GetterReport,
    per_putter: Vec<PutterReport>,
}

#[derive(Debug, Serialize)]
struct GetterReport {
    instance_id: String,
    count: usize,
    total_bytes: u64,
    elapsed_ms: f64,
    materialized_dir: String,
    paced: PacedTaskRunReport,
}

impl GetterReport {
    fn from_result(instance_id: &str, result: PacedBlobGetResult) -> Self {
        Self {
            instance_id: instance_id.to_string(),
            count: result.count,
            total_bytes: result.total_bytes,
            elapsed_ms: result.elapsed_ms,
            materialized_dir: result.materialized_dir,
            paced: result.paced,
        }
    }
}

#[derive(Debug, Serialize)]
struct PutterReport {
    instance_id: String,
    planned_count: usize,
    count: usize,
    total_bytes: u64,
    elapsed_ms: f64,
}

impl PutterReport {
    fn from_result(instance_id: &str, planned_count: usize, result: BlobPutResult) -> Self {
        Self {
            instance_id: instance_id.to_string(),
            planned_count,
            count: result.count,
            total_bytes: result.total_bytes,
            elapsed_ms: result.elapsed_ms,
        }
    }
}
