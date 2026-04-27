use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::join_all;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::blob_store_factory::unique_resource_id;
use crate::config::{ExperimentSpec, InstanceConfig, InstancesConfig};
use crate::driver::latency::LatencySummary;
use crate::driver::paced::PacedTaskRunReport;
use crate::driver::sweep::{SweepDecision, SweepStopReason, ThroughputSweepPolicy};
use crate::experiments::blob::control::{
    begin_on_target, init_blob_store_on_target_with_request, prepare_blob_get_append_on_target,
    prepare_blob_get_begin_on_target, prepare_blob_get_finish_on_target, put_blob_batch_on_target,
    reset_on_target, start_prepared_blob_get_on_target,
};
use crate::rpc::protocol::{
    InitBlobStoreRequest, PacedBlobGetResult, PrepareBlobGetAppendRequest,
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
        return Err("blob.multi_getter requires benchmark.backend = s3 or p2p".to_string());
    }
    if backend == "p2p" && experiment.p2p.tracker_backend.trim() != "dynamodb" {
        return Err("blob.multi_getter p2p requires p2p.tracker_backend = dynamodb".to_string());
    }
    let duration_seconds = experiment
        .benchmark
        .duration_seconds
        .ok_or_else(|| "blob.multi_getter requires benchmark.duration_seconds".to_string())?;
    let source_id = source_id(&experiment)?;
    let getter_ids = getter_ids(&experiment, &source_id)?;
    if getter_ids.is_empty() {
        return Err("blob.multi_getter requires at least one getter participant".to_string());
    }

    let policy = sweep_policy(&experiment)?;
    let target_rates = target_rates(&experiment, &policy)?;
    let mut datapoints = Vec::new();
    let mut stop_reason = SweepStopReason::MaxPoints;
    for (index, overall_rate) in target_rates.into_iter().enumerate() {
        let datapoint = run_datapoint(
            instance,
            runtime,
            instances,
            &experiment,
            &backend,
            &source_id,
            &getter_ids,
            duration_seconds,
            overall_rate,
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

    let report = MultiGetterReport {
        run_id: experiment.run.run_id.clone(),
        workload: experiment.run.workload.clone(),
        instance_id: instance.id.clone(),
        backend,
        operations_per_point: experiment.benchmark.operations,
        warmup_operations_per_point: experiment.benchmark.warmup_operations,
        duration_seconds,
        concurrency: experiment.benchmark.concurrency,
        object_size_bytes: experiment.benchmark.object_size_bytes,
        getter_count: getter_ids.len(),
        source_instance_id: source_id,
        getter_instance_ids: getter_ids,
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
    source_id: &str,
    getter_ids: &[String],
    duration_seconds: f64,
    overall_rate: f64,
    datapoint_index: usize,
) -> Result<MultiGetterDatapointReport, String> {
    let getter_count = getter_ids.len();
    let per_getter_rate = overall_rate / getter_count as f64;
    let operations_per_getter = operations_for_rate(per_getter_rate, duration_seconds)?;
    let working_set_size = operations_per_getter;
    let resource_id = unique_resource_id(&experiment.run.run_id, source_id);
    let run_id = experiment.run.run_id.clone();

    begin_on_target(
        instances,
        instance,
        runtime,
        source_id,
        &run_id,
        experiment.coordination.force_reset_on_start,
    )
    .await?;
    init_blob_store_on_target_with_request(
        instances,
        instance,
        runtime,
        source_id,
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

    let preload_put_concurrency = preload_put_concurrency(experiment)?;
    let put_started = std::time::Instant::now();
    let source_put = put_blob_batch_on_target(
        instances,
        instance,
        runtime,
        source_id,
        experiment.coordination.barrier_timeout_ms,
        PutBlobBatchRequest {
            run_id: run_id.clone(),
            count: working_set_size,
            object_size_bytes: experiment.benchmark.object_size_bytes,
            max_in_flight: preload_put_concurrency,
        },
    )
    .await?;
    let preload_put_ms = put_started.elapsed().as_secs_f64() * 1000.0;

    for getter_id in getter_ids {
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
                create_remote_resources: false,
            },
        )
        .await?;
    }

    let refs_chunk_count = source_put.refs.len().div_ceil(REF_CHUNK_SIZE);
    for getter_id in getter_ids {
        let plan_id = getter_plan_id(datapoint_index, getter_id);
        prepare_blob_get_begin_on_target(
            instances,
            instance,
            runtime,
            getter_id,
            PrepareBlobGetBeginRequest {
                run_id: run_id.clone(),
                plan_id: plan_id.clone(),
                expected_ref_count: source_put.refs.len(),
                target_ops_per_s: per_getter_rate,
                max_in_flight: experiment.benchmark.concurrency,
            },
        )
        .await?;
        for (chunk_index, refs) in source_put.refs.chunks(REF_CHUNK_SIZE).enumerate() {
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
                plan_id,
            },
        )
        .await?;
    }

    let start_after_unix_ns = future_start_unix_ns(Duration::from_secs(2));
    let getter_futures = getter_ids.iter().map(|getter_id| {
        start_prepared_blob_get_on_target(
            instances,
            instance,
            runtime,
            getter_id,
            experiment.coordination.barrier_timeout_ms,
            StartPreparedBlobGetRequest {
                run_id: run_id.clone(),
                plan_id: getter_plan_id(datapoint_index, getter_id),
                start_after_unix_ns: Some(start_after_unix_ns),
            },
        )
    });
    let getter_results = join_all(getter_futures).await;

    let mut per_getter = Vec::with_capacity(getter_ids.len());
    let mut getter_failures = Vec::new();
    for (getter_id, result) in getter_ids.iter().zip(getter_results) {
        match result {
            Ok(result) => per_getter.push(GetterReport::from_result(getter_id, result)),
            Err(err) => getter_failures.push(format!("{getter_id}: {err}")),
        }
    }

    let _ = reset_on_target(instances, instance, runtime, source_id, &run_id, true).await;
    for getter_id in getter_ids {
        let _ = reset_on_target(instances, instance, runtime, getter_id, &run_id, true).await;
    }

    let aggregate = aggregate_getters(overall_rate, &per_getter, &getter_failures);
    for getter in &mut per_getter {
        getter.paced.samples.clear();
    }
    Ok(MultiGetterDatapointReport {
        run_id,
        workload: experiment.run.workload.clone(),
        instance_id: instance.id.clone(),
        backend: backend.to_string(),
        resource_id,
        datapoint_index,
        source_instance_id: source_id.to_string(),
        getter_count,
        getter_instance_ids: getter_ids.to_vec(),
        operations: operations_per_getter.saturating_mul(getter_count) as u64,
        operations_per_getter: operations_per_getter as u64,
        warmup_operations: 0,
        duration_seconds,
        object_size_bytes: experiment.benchmark.object_size_bytes,
        total_bytes: aggregate.completed_tasks as u64 * experiment.benchmark.object_size_bytes,
        working_set_size: working_set_size as u64,
        refs_distribution: "shared-list".to_string(),
        getter_reuse_policy: "no-repeat".to_string(),
        cross_getter_reuse: true,
        refs_chunk_size: REF_CHUNK_SIZE as u64,
        refs_chunk_count: refs_chunk_count as u64,
        per_getter_target_ops_per_s: per_getter_rate,
        aggregate_target_ops_per_s: overall_rate,
        preload_put_ms,
        preload_put_concurrency: preload_put_concurrency as u64,
        preload_put_ops_per_s: if preload_put_ms > 0.0 {
            working_set_size as f64 / (preload_put_ms / 1000.0)
        } else {
            0.0
        },
        store: BTreeMap::new(),
        paced: aggregate,
        per_getter,
    })
}

fn getter_plan_id(datapoint_index: usize, getter_id: &str) -> String {
    format!("datapoint-{datapoint_index:06}-{getter_id}")
}

fn source_id(experiment: &ExperimentSpec) -> Result<String, String> {
    experiment
        .participants
        .iter()
        .find(|participant| participant.label.as_deref() == Some("source"))
        .or_else(|| experiment.participants.first())
        .map(|participant| participant.instance_id.clone())
        .ok_or_else(|| "blob.multi_getter requires participants".to_string())
}

fn getter_ids(experiment: &ExperimentSpec, source_id: &str) -> Result<Vec<String>, String> {
    let labeled: Vec<String> = experiment
        .participants
        .iter()
        .filter(|participant| participant.label.as_deref() == Some("getter"))
        .map(|participant| participant.instance_id.clone())
        .collect();
    if !labeled.is_empty() {
        return Ok(labeled);
    }
    Ok(experiment
        .participants
        .iter()
        .map(|participant| participant.instance_id.clone())
        .filter(|instance_id| instance_id != source_id)
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
        Err("blob.multi_getter throughput sweep generated no target rates".to_string())
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
                .get("LC_BENCH_BLOB_MULTI_GETTER_PRELOAD_CONCURRENCY")
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

fn future_start_unix_ns(delay: Duration) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.saturating_add(delay).as_nanos().min(u64::MAX as u128) as u64
}

fn aggregate_getters(
    target_ops_per_s: f64,
    getters: &[GetterReport],
    failures: &[String],
) -> AggregatePacedSummary {
    let mut offered = Vec::new();
    let mut service = Vec::new();
    let mut lag = Vec::new();
    let mut failure_messages = failures.to_vec();
    let mut total_tasks = 0;
    let mut completed_tasks = 0;
    let mut failed_tasks = 0;
    let mut wall_time_ms = 0.0_f64;
    let mut achieved_ops_per_s = 0.0_f64;
    let mut successful_ops_per_s = 0.0_f64;

    for getter in getters {
        let paced = &getter.paced;
        total_tasks += paced.total_tasks;
        completed_tasks += paced.completed_tasks;
        failed_tasks += paced.failed_tasks;
        wall_time_ms = wall_time_ms.max(paced.wall_time_ms);
        achieved_ops_per_s += paced.achieved_ops_per_s;
        successful_ops_per_s += paced.successful_ops_per_s;
        for sample in &paced.samples {
            offered.push(sample.offered_latency_ms);
            service.push(sample.service_latency_ms);
            lag.push(sample.schedule_lag_ms);
        }
        failure_messages.extend(paced.failures.iter().take(16).map(|failure| {
            format!(
                "{} index={}: {}",
                getter.instance_id, failure.index, failure.message
            )
        }));
    }

    AggregatePacedSummary {
        target_ops_per_s,
        achieved_ops_per_s,
        successful_ops_per_s,
        total_tasks,
        completed_tasks,
        failed_tasks: failed_tasks + failures.len(),
        wall_time_ms,
        offered_latency: LatencySummary::from_samples(&offered),
        service_latency: LatencySummary::from_samples(&service),
        schedule_lag: LatencySummary::from_samples(&lag),
        failure_messages,
    }
}

#[derive(Debug, Serialize)]
struct MultiGetterReport {
    run_id: String,
    workload: String,
    instance_id: String,
    backend: String,
    operations_per_point: u64,
    warmup_operations_per_point: u64,
    duration_seconds: f64,
    concurrency: usize,
    object_size_bytes: u64,
    getter_count: usize,
    source_instance_id: String,
    getter_instance_ids: Vec<String>,
    stop_reason: SweepStopReason,
    datapoints: Vec<MultiGetterDatapointReport>,
}

#[derive(Debug, Serialize)]
struct MultiGetterDatapointReport {
    run_id: String,
    workload: String,
    instance_id: String,
    backend: String,
    resource_id: String,
    datapoint_index: usize,
    source_instance_id: String,
    getter_count: usize,
    getter_instance_ids: Vec<String>,
    operations: u64,
    operations_per_getter: u64,
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
    per_getter_target_ops_per_s: f64,
    aggregate_target_ops_per_s: f64,
    preload_put_ms: f64,
    preload_put_concurrency: u64,
    preload_put_ops_per_s: f64,
    store: BTreeMap<String, String>,
    paced: AggregatePacedSummary,
    per_getter: Vec<GetterReport>,
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
struct AggregatePacedSummary {
    target_ops_per_s: f64,
    achieved_ops_per_s: f64,
    successful_ops_per_s: f64,
    total_tasks: usize,
    completed_tasks: usize,
    failed_tasks: usize,
    wall_time_ms: f64,
    offered_latency: LatencySummary,
    service_latency: LatencySummary,
    schedule_lag: LatencySummary,
    failure_messages: Vec<String>,
}
