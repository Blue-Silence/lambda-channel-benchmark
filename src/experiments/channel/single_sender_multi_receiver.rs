use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::blob_store_factory::unique_resource_id;
use crate::config::{ExperimentSpec, InstanceConfig, InstancesConfig};
use crate::driver::latency::LatencySummary;
use crate::driver::sweep::{SweepDecision, SweepStopReason, ThroughputSweepPolicy};
use crate::rpc::protocol::{
    ChannelReceiverResult, ChannelSendResult, InitReceiverRequest, InitSenderRequest,
    StartChannelReceiverRequest, StartPacedChannelSendRequest,
};
use crate::rpc::server::state::NodeRuntimeState;

use super::control::{
    begin_on_target, cancel_receiver_on_target, finish_receiver_on_target, init_receiver_on_target,
    init_sender_on_target, reset_on_target, start_receiver_on_target, start_send_on_target,
    wait_receiver_ready_on_target,
};

const POLL_TARGET_MULTIPLIER: f64 = 2.0;
const DEFAULT_MIN_ELEMENTS_PER_DATAPOINT: usize = 1;

pub(super) async fn run(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    instances: &InstancesConfig,
    experiment: ExperimentSpec,
) -> Result<String, String> {
    let backend = experiment.benchmark.backend.trim().to_ascii_lowercase();
    if !matches!(backend.as_str(), "s3" | "p2p") {
        return Err(
            "channel.single_sender_multi_receiver requires benchmark.backend = s3 or p2p"
                .to_string(),
        );
    }
    if experiment.lambda_channel.metadata_backend.trim() != "dynamodb" {
        return Err(
            "channel.single_sender_multi_receiver formal runs require lambda_channel.metadata_backend = dynamodb"
                .to_string(),
        );
    }
    if backend == "p2p" && experiment.p2p.tracker_backend.trim() != "dynamodb" {
        return Err(
            "channel.single_sender_multi_receiver p2p requires p2p.tracker_backend = dynamodb"
                .to_string(),
        );
    }
    let consume_mode = consume_mode(&experiment)?;
    let duration_seconds = experiment.benchmark.duration_seconds.ok_or_else(|| {
        "channel.single_sender_multi_receiver requires benchmark.duration_seconds".to_string()
    })?;
    let sender_id = sender_id(&experiment)?;
    let receiver_ids = receiver_ids(&experiment, &sender_id)?;
    if receiver_ids.is_empty() {
        return Err(
            "channel.single_sender_multi_receiver requires at least one receiver".to_string(),
        );
    }

    let policy = sweep_policy(&experiment)?;
    let target_rates = target_rates(&experiment, &policy)?;
    let datapoint_element_bounds = datapoint_element_bounds(&experiment)?;
    let mut datapoints = Vec::new();
    let mut stop_reason = SweepStopReason::MaxPoints;
    for (index, target_sender_ops_per_s) in target_rates.into_iter().enumerate() {
        let element_count = operations_for_rate(
            target_sender_ops_per_s,
            duration_seconds,
            datapoint_element_bounds,
        )?;
        let datapoint_duration_seconds = element_count as f64 / target_sender_ops_per_s;
        let datapoint = run_datapoint(
            instance,
            runtime,
            instances,
            &experiment,
            &backend,
            &consume_mode,
            &sender_id,
            &receiver_ids,
            datapoint_duration_seconds,
            target_sender_ops_per_s,
            element_count,
            index,
            &policy,
        )
        .await?;
        let failed_tasks = datapoint.paced.failed_tasks;
        let decision = policy.decide_after_metrics(
            datapoints.len() + 1,
            datapoint.expected_receiver_ops_per_s,
            datapoint.aggregate_delivered_ops_per_s,
            failed_tasks,
        );
        let sender_limited = datapoint.sender_limited;
        datapoints.push(datapoint);
        if sender_limited {
            stop_reason = SweepStopReason::Saturated;
            break;
        }
        if let SweepDecision::Stop { reason } = decision {
            stop_reason = reason;
            break;
        }
    }

    let report = ChannelReport {
        run_id: experiment.run.run_id.clone(),
        workload: experiment.run.workload.clone(),
        instance_id: sender_id.clone(),
        backend,
        operations_per_point: experiment.benchmark.operations,
        warmup_operations_per_point: experiment.benchmark.warmup_operations,
        duration_seconds,
        concurrency: experiment.benchmark.concurrency,
        object_size_bytes: experiment.benchmark.object_size_bytes,
        consume_mode,
        sender_instance_id: sender_id,
        receiver_count: receiver_ids.len(),
        receiver_instance_ids: receiver_ids,
        stop_reason,
        datapoints,
    };
    serde_json::to_string(&report)
        .map_err(|err| format!("failed to serialize channel report: {err}"))
}

#[allow(clippy::too_many_arguments)]
async fn run_datapoint(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    instances: &InstancesConfig,
    experiment: &ExperimentSpec,
    backend: &str,
    consume_mode: &str,
    sender_id: &str,
    receiver_ids: &[String],
    duration_seconds: f64,
    target_sender_ops_per_s: f64,
    element_count: usize,
    datapoint_index: usize,
    policy: &ThroughputSweepPolicy,
) -> Result<ChannelDatapointReport, String> {
    let resource_id = unique_resource_id(&experiment.run.run_id, sender_id);
    let channel_id = format!(
        "{}-{}",
        sanitize_id(&experiment.lambda_channel.channel_id_prefix),
        resource_id
    );
    let run_id = experiment.run.run_id.clone();

    begin_on_target(
        instances,
        instance,
        runtime,
        sender_id,
        &run_id,
        experiment.coordination.force_reset_on_start,
    )
    .await?;
    init_sender_on_target(
        instances,
        instance,
        runtime,
        sender_id,
        InitSenderRequest {
            run_id: run_id.clone(),
            channel_id: channel_id.clone(),
            backend: Some(backend.to_string()),
            root_dir: None,
            reopen: false,
            recover: false,
            force_reinit: true,
            metadata_backend: Some(experiment.lambda_channel.metadata_backend.clone()),
            experiment: Some(experiment.clone()),
            resource_id: Some(resource_id.clone()),
            create_remote_resources: true,
        },
    )
    .await?;

    for receiver_id in receiver_ids {
        begin_on_target(
            instances,
            instance,
            runtime,
            receiver_id,
            &run_id,
            experiment.coordination.force_reset_on_start,
        )
        .await?;
        init_receiver_on_target(
            instances,
            instance,
            runtime,
            receiver_id,
            InitReceiverRequest {
                run_id: run_id.clone(),
                channel_id: channel_id.clone(),
                consumer_id: format!("{receiver_id}-{resource_id}"),
                backend: Some(backend.to_string()),
                root_dir: None,
                start_seq: 0,
                consume_mode: consume_mode.to_string(),
                passive_mode: false,
                force_reinit: true,
                metadata_backend: Some(experiment.lambda_channel.metadata_backend.clone()),
                experiment: Some(experiment.clone()),
                resource_id: Some(resource_id.clone()),
                create_remote_resources: false,
            },
        )
        .await?;
    }

    let expected_receiver_ops_per_s =
        expected_receiver_ops_per_s(consume_mode, target_sender_ops_per_s, receiver_ids.len());
    let receiver_poll_rate =
        receiver_poll_target_ops_per_s(consume_mode, target_sender_ops_per_s, receiver_ids.len());
    let receiver_poll_concurrency =
        receiver_poll_concurrency(consume_mode, experiment.benchmark.concurrency);
    let accepted_receivers = join_all(receiver_ids.iter().map(|receiver_id| {
        start_receiver_on_target(
            instances,
            instance,
            runtime,
            receiver_id,
            StartChannelReceiverRequest {
                run_id: run_id.clone(),
                poll_target_ops_per_s: receiver_poll_rate,
                poll_concurrency: receiver_poll_concurrency,
                materialize_concurrency: experiment.benchmark.concurrency,
                output_subdir: receiver_id.clone(),
                max_runtime_ms: Some((duration_seconds * 5.0 * 1000.0).ceil() as u64),
            },
        )
    }))
    .await;
    let mut receiver_requests = Vec::with_capacity(receiver_ids.len());
    for (receiver_id, accepted) in receiver_ids.iter().zip(accepted_receivers) {
        receiver_requests.push((receiver_id.clone(), accepted?));
    }
    for (receiver_id, accepted) in &receiver_requests {
        wait_receiver_ready_on_target(
            instances,
            instance,
            runtime,
            receiver_id,
            &run_id,
            experiment.coordination.barrier_timeout_ms,
            accepted,
        )
        .await?;
    }

    let sender_result = start_send_on_target(
        instances,
        instance,
        runtime,
        sender_id,
        experiment.coordination.barrier_timeout_ms,
        StartPacedChannelSendRequest {
            run_id: run_id.clone(),
            count: element_count,
            object_size_bytes: experiment.benchmark.object_size_bytes,
            target_ops_per_s: target_sender_ops_per_s,
            max_in_flight: experiment.benchmark.concurrency,
            payload_strategy: "prestage-patch".to_string(),
        },
    )
    .await?;
    let sender_failed =
        sender_result.paced.failed_tasks > 0 || !sender_result.failure_messages.is_empty();
    if sender_failed {
        let cancel_results = join_all(receiver_requests.iter().map(|(receiver_id, accepted)| {
            let run_id = run_id.clone();
            async move {
                cancel_receiver_on_target(
                    instances,
                    instance,
                    runtime,
                    receiver_id,
                    &run_id,
                    accepted,
                )
                .await
            }
        }))
        .await;
        for (receiver_id, result) in receiver_ids.iter().zip(cancel_results) {
            if let Err(err) = result {
                eprintln!("failed to cancel receiver {receiver_id}: {err}");
            }
        }
    }

    let receiver_results = join_all(receiver_requests.into_iter().map(
        |(receiver_id, accepted)| {
            let run_id = run_id.clone();
            async move {
                finish_receiver_on_target(
                    instances,
                    instance,
                    runtime,
                    &receiver_id,
                    &run_id,
                    experiment.coordination.barrier_timeout_ms,
                    accepted,
                )
                .await
            }
        },
    ))
    .await;
    let mut per_receiver = Vec::with_capacity(receiver_ids.len());
    let mut receiver_failures = Vec::new();
    for (receiver_id, result) in receiver_ids.iter().zip(receiver_results) {
        match result {
            Ok(result) => per_receiver.push(ReceiverReport::from_result(receiver_id, result)),
            Err(err) => receiver_failures.push(format!("{receiver_id}: {err}")),
        }
    }
    if sender_failed {
        receiver_failures.extend(
            sender_result
                .failure_messages
                .iter()
                .take(16)
                .map(|message| format!("sender: {message}")),
        );
    }

    let sender_successful_push_ops_per_s = sender_result.paced.successful_ops_per_s;
    let sender_limited_ratio = if target_sender_ops_per_s > 0.0 {
        sender_successful_push_ops_per_s / target_sender_ops_per_s
    } else {
        0.0
    };
    let sender_limited = sender_limited_ratio < policy.saturation_achieved_ratio;

    let cleanup_timeout = Duration::from_millis(experiment.coordination.cleanup_timeout_ms.max(1));
    let cleanup_targets = std::iter::once(sender_id.to_string())
        .chain(receiver_ids.iter().cloned())
        .collect::<Vec<_>>();
    let cleanup_results = join_all(cleanup_targets.into_iter().map(|target_id| {
        let run_id = run_id.clone();
        async move {
            let result = tokio::time::timeout(
                cleanup_timeout,
                reset_on_target(instances, instance, runtime, &target_id, &run_id, true),
            )
            .await;
            (target_id, result)
        }
    }))
    .await;
    for (target_id, result) in cleanup_results {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => receiver_failures.push(format!("cleanup {target_id}: {err}")),
            Err(_) => receiver_failures.push(format!(
                "cleanup {target_id}: timed out after {} ms",
                experiment.coordination.cleanup_timeout_ms
            )),
        }
    }

    let aggregate = aggregate_receivers(
        expected_receiver_ops_per_s,
        &per_receiver,
        &receiver_failures,
    );
    let stats = receiver_stats(&per_receiver);
    Ok(ChannelDatapointReport {
        run_id: run_id.clone(),
        workload: experiment.run.workload.clone(),
        instance_id: sender_id.to_string(),
        backend: backend.to_string(),
        resource_id,
        channel_id,
        datapoint_index,
        operations: element_count as u64,
        warmup_operations: experiment.benchmark.warmup_operations,
        duration_seconds,
        object_size_bytes: experiment.benchmark.object_size_bytes,
        total_bytes: aggregate.delivered_bytes,
        consume_mode: consume_mode.to_string(),
        sender_instance_id: sender_id.to_string(),
        receiver_count: receiver_ids.len(),
        receiver_instance_ids: receiver_ids.to_vec(),
        target_sender_ops_per_s,
        expected_receiver_ops_per_s,
        sender_successful_push_ops_per_s,
        sender_limited,
        sender_limited_ratio,
        aggregate_delivered_ops_per_s: aggregate.aggregate_delivered_ops_per_s,
        aggregate_delivered_bytes_per_s: aggregate.aggregate_delivered_bytes_per_s,
        sum_receiver_ops_per_s: stats.sum_ops_per_s,
        mean_receiver_ops_per_s: stats.mean_ops_per_s,
        slowest_receiver_ops_per_s: stats.min_ops_per_s,
        fastest_receiver_ops_per_s: stats.max_ops_per_s,
        receiver_count_min: stats.min_count,
        receiver_count_max: stats.max_count,
        receiver_count_stddev: stats.count_stddev,
        eof_time_spread_ms: stats.eof_spread_ms,
        payload_strategy: sender_result.payload_strategy.clone(),
        prestage_payload_ms: sender_result.prestage_payload_ms,
        poll_strategy: "paced-try-pop-until-eof".to_string(),
        poll_target_multiplier: POLL_TARGET_MULTIPLIER,
        delivery_latency_p50_ms: aggregate.delivery_latency.p50_ms,
        delivery_latency_p90_ms: aggregate.delivery_latency.p90_ms,
        delivery_latency_p99_ms: aggregate.delivery_latency.p99_ms,
        materialize_latency_p50_ms: aggregate.materialize_latency.p50_ms,
        materialize_latency_p90_ms: aggregate.materialize_latency.p90_ms,
        materialize_latency_p99_ms: aggregate.materialize_latency.p99_ms,
        store: BTreeMap::new(),
        paced: aggregate.paced,
        sender: SenderReport::from_result(sender_result),
        per_receiver,
    })
}

fn sender_id(experiment: &ExperimentSpec) -> Result<String, String> {
    if experiment.participants.is_empty() {
        return Err("channel experiment requires participants".to_string());
    }
    Ok(experiment
        .participants
        .iter()
        .find(|participant| {
            participant.label.as_deref().is_some_and(|label| {
                matches!(
                    label.trim().to_ascii_lowercase().as_str(),
                    "sender" | "orchestrator"
                )
            })
        })
        .unwrap_or(&experiment.participants[0])
        .instance_id
        .clone())
}

fn receiver_ids(experiment: &ExperimentSpec, sender_id: &str) -> Result<Vec<String>, String> {
    let labeled = experiment
        .participants
        .iter()
        .filter(|participant| {
            participant
                .label
                .as_deref()
                .is_some_and(|label| label.trim().eq_ignore_ascii_case("receiver"))
        })
        .map(|participant| participant.instance_id.clone())
        .collect::<Vec<_>>();
    if !labeled.is_empty() {
        return Ok(labeled);
    }
    Ok(experiment
        .participants
        .iter()
        .map(|participant| participant.instance_id.clone())
        .filter(|id| id != sender_id)
        .collect())
}

fn consume_mode(experiment: &ExperimentSpec) -> Result<String, String> {
    let mode = experiment
        .lambda_channel
        .consume_mode
        .trim()
        .to_ascii_lowercase();
    if matches!(mode.as_str(), "fanout" | "competitive") {
        Ok(mode)
    } else {
        Err(format!("unsupported channel consume_mode: {mode}"))
    }
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
        Err("channel throughput sweep generated no target rates".to_string())
    } else {
        Ok(rates)
    }
}

fn operations_for_rate(
    rate: f64,
    duration_seconds: f64,
    bounds: DatapointElementBounds,
) -> Result<usize, String> {
    if !rate.is_finite() || rate <= 0.0 {
        return Err("target sender rate must be a finite positive number".to_string());
    }
    let operations = (rate * duration_seconds)
        .ceil()
        .max(bounds.min as f64)
        .min(bounds.max as f64);
    if operations > usize::MAX as f64 {
        return Err("rate * duration is too large for this platform".to_string());
    }
    Ok(operations as usize)
}

#[derive(Clone, Copy)]
struct DatapointElementBounds {
    min: usize,
    max: usize,
}

fn datapoint_element_bounds(experiment: &ExperimentSpec) -> Result<DatapointElementBounds, String> {
    let min = experiment
        .lambda_channel
        .min_elements_per_datapoint
        .unwrap_or(DEFAULT_MIN_ELEMENTS_PER_DATAPOINT);
    let max = experiment
        .lambda_channel
        .max_elements_per_datapoint
        .unwrap_or(usize::MAX);
    if min == 0 || max == 0 {
        return Err("channel datapoint element bounds must be greater than zero".to_string());
    }
    if min > max {
        return Err("channel datapoint min elements must be <= max elements".to_string());
    }
    Ok(DatapointElementBounds { min, max })
}

fn expected_receiver_ops_per_s(mode: &str, sender_rate: f64, receiver_count: usize) -> f64 {
    if mode == "fanout" {
        sender_rate * receiver_count as f64
    } else {
        sender_rate
    }
}

fn receiver_poll_target_ops_per_s(mode: &str, sender_rate: f64, receiver_count: usize) -> f64 {
    if mode == "fanout" {
        sender_rate * POLL_TARGET_MULTIPLIER
    } else {
        sender_rate / receiver_count as f64 * POLL_TARGET_MULTIPLIER
    }
}

fn receiver_poll_concurrency(mode: &str, benchmark_concurrency: usize) -> usize {
    if mode == "competitive" {
        1
    } else {
        benchmark_concurrency.max(1)
    }
}

#[derive(Debug, Serialize)]
struct ChannelReport {
    run_id: String,
    workload: String,
    instance_id: String,
    backend: String,
    operations_per_point: u64,
    warmup_operations_per_point: u64,
    duration_seconds: f64,
    concurrency: usize,
    object_size_bytes: u64,
    consume_mode: String,
    sender_instance_id: String,
    receiver_count: usize,
    receiver_instance_ids: Vec<String>,
    stop_reason: SweepStopReason,
    datapoints: Vec<ChannelDatapointReport>,
}

#[derive(Debug, Serialize)]
struct ChannelDatapointReport {
    run_id: String,
    workload: String,
    instance_id: String,
    backend: String,
    resource_id: String,
    channel_id: String,
    datapoint_index: usize,
    operations: u64,
    warmup_operations: u64,
    duration_seconds: f64,
    object_size_bytes: u64,
    total_bytes: u64,
    consume_mode: String,
    sender_instance_id: String,
    receiver_count: usize,
    receiver_instance_ids: Vec<String>,
    target_sender_ops_per_s: f64,
    expected_receiver_ops_per_s: f64,
    sender_successful_push_ops_per_s: f64,
    sender_limited: bool,
    sender_limited_ratio: f64,
    aggregate_delivered_ops_per_s: f64,
    aggregate_delivered_bytes_per_s: f64,
    sum_receiver_ops_per_s: f64,
    mean_receiver_ops_per_s: f64,
    slowest_receiver_ops_per_s: f64,
    fastest_receiver_ops_per_s: f64,
    receiver_count_min: u64,
    receiver_count_max: u64,
    receiver_count_stddev: f64,
    eof_time_spread_ms: f64,
    payload_strategy: String,
    prestage_payload_ms: f64,
    poll_strategy: String,
    poll_target_multiplier: f64,
    delivery_latency_p50_ms: f64,
    delivery_latency_p90_ms: f64,
    delivery_latency_p99_ms: f64,
    materialize_latency_p50_ms: f64,
    materialize_latency_p90_ms: f64,
    materialize_latency_p99_ms: f64,
    store: BTreeMap<String, String>,
    paced: AggregatePacedSummary,
    sender: SenderReport,
    per_receiver: Vec<ReceiverReport>,
}

#[derive(Debug, Serialize)]
struct SenderReport {
    sent_count: usize,
    total_bytes: u64,
    close_elapsed_ms: f64,
    prestage_payload_ms: f64,
    payload_strategy: String,
    payload_dir: String,
    paced: crate::driver::paced::PacedTaskRunReport,
    failure_messages: Vec<String>,
}

impl SenderReport {
    fn from_result(result: ChannelSendResult) -> Self {
        Self {
            sent_count: result.sent_count,
            total_bytes: result.total_bytes,
            close_elapsed_ms: result.close_elapsed_ms,
            prestage_payload_ms: result.prestage_payload_ms,
            payload_strategy: result.payload_strategy,
            payload_dir: result.payload_dir,
            paced: result.paced,
            failure_messages: result.failure_messages,
        }
    }
}

#[derive(Debug, Serialize)]
struct ReceiverReport {
    instance_id: String,
    delivered_count: usize,
    delivered_bytes: u64,
    empty_polls: u64,
    transient_poll_errors: u64,
    timed_out: bool,
    cancelled: bool,
    eof_seq: Option<i64>,
    eof_elapsed_ms: Option<f64>,
    elapsed_ms: f64,
    started_unix_ns: u64,
    finished_unix_ns: u64,
    materialized_dir: String,
    poll_target_ops_per_s: f64,
    poll_concurrency: usize,
    materialize_concurrency: usize,
    delivered_ops_per_s: f64,
    delivered_bytes_per_s: f64,
    delivery_latency: LatencySummary,
    materialize_latency: LatencySummary,
    #[serde(skip_serializing)]
    delivery_latency_samples_ms: Vec<f64>,
    #[serde(skip_serializing)]
    materialize_latency_samples_ms: Vec<f64>,
    failure_messages: Vec<String>,
}

impl ReceiverReport {
    fn from_result(instance_id: &str, result: ChannelReceiverResult) -> Self {
        let elapsed_s = (result.elapsed_ms / 1000.0).max(f64::EPSILON);
        Self {
            instance_id: instance_id.to_string(),
            delivered_count: result.delivered_count,
            delivered_bytes: result.delivered_bytes,
            empty_polls: result.empty_polls,
            transient_poll_errors: result.transient_poll_errors,
            timed_out: result.timed_out,
            cancelled: result.cancelled,
            eof_seq: result.eof_seq,
            eof_elapsed_ms: result.eof_elapsed_ms,
            elapsed_ms: result.elapsed_ms,
            started_unix_ns: result.started_unix_ns,
            finished_unix_ns: result.finished_unix_ns,
            materialized_dir: result.materialized_dir,
            poll_target_ops_per_s: result.poll_target_ops_per_s,
            poll_concurrency: result.poll_concurrency,
            materialize_concurrency: result.materialize_concurrency,
            delivered_ops_per_s: result.delivered_count as f64 / elapsed_s,
            delivered_bytes_per_s: result.delivered_bytes as f64 / elapsed_s,
            delivery_latency: result.delivery_latency,
            materialize_latency: result.materialize_latency,
            delivery_latency_samples_ms: result.delivery_latency_samples_ms,
            materialize_latency_samples_ms: result.materialize_latency_samples_ms,
            failure_messages: result.failure_messages,
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

struct ReceiverAggregate {
    delivered_bytes: u64,
    aggregate_delivered_ops_per_s: f64,
    aggregate_delivered_bytes_per_s: f64,
    delivery_latency: LatencySummary,
    materialize_latency: LatencySummary,
    paced: AggregatePacedSummary,
}

fn aggregate_receivers(
    expected_receiver_ops_per_s: f64,
    receivers: &[ReceiverReport],
    failures: &[String],
) -> ReceiverAggregate {
    let mut delivery = Vec::new();
    let mut materialize = Vec::new();
    let mut failure_messages = failures.to_vec();
    let mut delivered_count = 0usize;
    let mut delivered_bytes = 0u64;
    let mut started_unix_ns = u64::MAX;
    let mut finished_unix_ns = 0_u64;
    for receiver in receivers {
        delivered_count += receiver.delivered_count;
        delivered_bytes = delivered_bytes.saturating_add(receiver.delivered_bytes);
        started_unix_ns = started_unix_ns.min(receiver.started_unix_ns);
        finished_unix_ns = finished_unix_ns.max(receiver.finished_unix_ns);
        delivery.extend(receiver.delivery_latency_samples_ms.iter().copied());
        materialize.extend(receiver.materialize_latency_samples_ms.iter().copied());
        failure_messages.extend(
            receiver
                .failure_messages
                .iter()
                .take(16)
                .map(|message| format!("{}: {message}", receiver.instance_id)),
        );
    }
    let wall_time_ms = if receivers.is_empty()
        || started_unix_ns == u64::MAX
        || finished_unix_ns <= started_unix_ns
    {
        0.0
    } else {
        finished_unix_ns.saturating_sub(started_unix_ns) as f64 / 1_000_000.0
    };
    let wall_s = (wall_time_ms / 1000.0).max(f64::EPSILON);
    let aggregate_delivered_ops_per_s = delivered_count as f64 / wall_s;
    let aggregate_delivered_bytes_per_s = delivered_bytes as f64 / wall_s;
    let delivery_latency = LatencySummary::from_samples(&delivery);
    let materialize_latency = LatencySummary::from_samples(&materialize);
    ReceiverAggregate {
        delivered_bytes,
        aggregate_delivered_ops_per_s,
        aggregate_delivered_bytes_per_s,
        delivery_latency: delivery_latency.clone(),
        materialize_latency: materialize_latency.clone(),
        paced: AggregatePacedSummary {
            target_ops_per_s: expected_receiver_ops_per_s,
            achieved_ops_per_s: aggregate_delivered_ops_per_s,
            successful_ops_per_s: aggregate_delivered_ops_per_s,
            total_tasks: delivered_count,
            completed_tasks: delivered_count,
            failed_tasks: failure_messages.len(),
            wall_time_ms,
            offered_latency: delivery_latency.clone(),
            service_latency: delivery_latency,
            schedule_lag: LatencySummary::default(),
            failure_messages,
        },
    }
}

struct ReceiverStats {
    sum_ops_per_s: f64,
    mean_ops_per_s: f64,
    min_ops_per_s: f64,
    max_ops_per_s: f64,
    min_count: u64,
    max_count: u64,
    count_stddev: f64,
    eof_spread_ms: f64,
}

fn receiver_stats(receivers: &[ReceiverReport]) -> ReceiverStats {
    if receivers.is_empty() {
        return ReceiverStats {
            sum_ops_per_s: 0.0,
            mean_ops_per_s: 0.0,
            min_ops_per_s: 0.0,
            max_ops_per_s: 0.0,
            min_count: 0,
            max_count: 0,
            count_stddev: 0.0,
            eof_spread_ms: 0.0,
        };
    }
    let counts = receivers
        .iter()
        .map(|receiver| receiver.delivered_count as f64)
        .collect::<Vec<_>>();
    let rates = receivers
        .iter()
        .map(|receiver| receiver.delivered_ops_per_s)
        .collect::<Vec<_>>();
    let sum_ops_per_s = rates.iter().sum::<f64>();
    let mean_ops_per_s = sum_ops_per_s / rates.len() as f64;
    let min_ops_per_s = rates.iter().copied().fold(f64::INFINITY, f64::min);
    let max_ops_per_s = rates.iter().copied().fold(0.0, f64::max);
    let min_count = counts.iter().copied().fold(f64::INFINITY, f64::min) as u64;
    let max_count = counts.iter().copied().fold(0.0, f64::max) as u64;
    let mean_count = counts.iter().sum::<f64>() / counts.len() as f64;
    let count_stddev = (counts
        .iter()
        .map(|count| (count - mean_count).powi(2))
        .sum::<f64>()
        / counts.len() as f64)
        .sqrt();
    let eof_times = receivers
        .iter()
        .filter_map(|receiver| receiver.eof_elapsed_ms)
        .collect::<Vec<_>>();
    let eof_spread_ms = if eof_times.is_empty() {
        0.0
    } else {
        eof_times.iter().copied().fold(0.0, f64::max)
            - eof_times.iter().copied().fold(f64::INFINITY, f64::min)
    };
    ReceiverStats {
        sum_ops_per_s,
        mean_ops_per_s,
        min_ops_per_s,
        max_ops_per_s,
        min_count,
        max_count,
        count_stddev,
        eof_spread_ms,
    }
}

fn sanitize_id(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect()
}
