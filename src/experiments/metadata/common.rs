use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::stream::{FuturesUnordered, StreamExt};
use lambda_channel::common::{NativeMap, NativeValue};
use lambda_channel::metadata_store::{utc_now_iso_string, ChannelMetaRecord, ElemMetaRecord};
use lambda_channel::metadata_store_impl::dynamodb::AsyncDynamoDbMetadataStore;
use lambda_channel::metadata_store_impl::MetadataStoreHandle;
use serde::Serialize;

use crate::config::{ExperimentSpec, InstanceConfig};
use crate::driver::latency::LatencySummary;
use crate::driver::paced::{PacedTaskRunConfig, PacedTaskRunReport};
use crate::driver::sweep::{SweepStopReason, ThroughputSweepPolicy};

use super::MetadataWorkload;

pub(super) struct MetadataStoreResource {
    pub(super) handle: MetadataStoreHandle,
    pub(super) details: BTreeMap<String, String>,
    pub(super) cleanup: Vec<MetadataCleanupResource>,
}

#[derive(Clone, Debug)]
pub(super) enum MetadataCleanupResource {
    DynamoDbTable {
        table_name: String,
    },
}

#[derive(Clone, Debug, Default)]
pub(super) struct AwsClientConfig {
    profile_name: Option<String>,
    region_name: Option<String>,
    endpoint_url: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
}

impl AwsClientConfig {
    fn for_dynamodb(experiment: &ExperimentSpec) -> Result<Self, String> {
        Ok(Self {
            profile_name: env_any(experiment, &["LC_BENCH_AWS_PROFILE", "AWS_PROFILE"]),
            region_name: env_any(
                experiment,
                &["LC_BENCH_AWS_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"],
            ),
            endpoint_url: env_any(
                experiment,
                &[
                    "LC_BENCH_DYNAMODB_ENDPOINT_URL",
                    "AWS_DYNAMODB_ENDPOINT_URL",
                    "DYNAMODB_ENDPOINT_URL",
                ],
            ),
            access_key_id: env_any(
                experiment,
                &["LC_BENCH_AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"],
            ),
            secret_access_key: env_any(
                experiment,
                &["LC_BENCH_AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"],
            ),
            session_token: env_any(
                experiment,
                &["LC_BENCH_AWS_SESSION_TOKEN", "AWS_SESSION_TOKEN"],
            ),
        }
        .validated()?)
    }

    fn apply_process_env(&self) {
        set_process_env("AWS_ACCESS_KEY_ID", self.access_key_id.as_deref());
        set_process_env("AWS_SECRET_ACCESS_KEY", self.secret_access_key.as_deref());
        set_process_env("AWS_SESSION_TOKEN", self.session_token.as_deref());
        set_process_env("AWS_PROFILE", self.profile_name.as_deref());
    }

    fn validated(self) -> Result<Self, String> {
        match (&self.access_key_id, &self.secret_access_key) {
            (None, None) | (Some(_), Some(_)) => Ok(self),
            _ => Err(
                "AWS config requires both access key id and secret access key when either is set"
                    .to_string(),
            ),
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct MetadataSweepReport {
    run_id: String,
    workload: String,
    instance_id: String,
    backend: String,
    operations_per_point: u64,
    warmup_operations_per_point: u64,
    duration_seconds: Option<f64>,
    concurrency: usize,
    object_size_bytes: u64,
    policy: ThroughputSweepPolicy,
    stop_reason: SweepStopReason,
    datapoints: Vec<MetadataDatapointReport>,
}

#[derive(Debug, Serialize)]
pub(super) struct MetadataDatapointReport {
    run_id: String,
    workload: String,
    instance_id: String,
    backend: String,
    resource_id: String,
    channel_id: String,
    operation: String,
    operations: u64,
    warmup_operations: u64,
    object_size_bytes: u64,
    scan_limit: Option<usize>,
    claim_target_count: Option<usize>,
    store: BTreeMap<String, String>,
    counters: BTreeMap<String, u64>,
    paced: PacedRunSummary,
}

pub(super) struct MetadataDatapointOutcome {
    pub(super) report: MetadataDatapointReport,
    pub(super) paced: PacedTaskRunReport,
}

#[derive(Debug, Serialize)]
struct PacedRunSummary {
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

impl From<PacedTaskRunReport> for PacedRunSummary {
    fn from(report: PacedTaskRunReport) -> Self {
        Self {
            target_ops_per_s: report.target_ops_per_s,
            achieved_ops_per_s: report.achieved_ops_per_s,
            successful_ops_per_s: report.successful_ops_per_s,
            total_tasks: report.total_tasks,
            completed_tasks: report.completed_tasks,
            failed_tasks: report.failed_tasks,
            wall_time_ms: report.wall_time_ms,
            offered_latency: report.offered_latency,
            service_latency: report.service_latency,
            schedule_lag: report.schedule_lag,
            failure_messages: report
                .failures
                .into_iter()
                .take(16)
                .map(|failure| format!("index={}: {}", failure.index, failure.message))
                .collect(),
        }
    }
}

impl MetadataSweepReport {
    pub(super) fn new(
        instance: &InstanceConfig,
        experiment: &ExperimentSpec,
        backend: &str,
        policy: ThroughputSweepPolicy,
        stop_reason: SweepStopReason,
        datapoints: Vec<MetadataDatapointReport>,
    ) -> Self {
        Self {
            run_id: experiment.run.run_id.clone(),
            workload: experiment.run.workload.clone(),
            instance_id: instance.id.clone(),
            backend: backend.to_string(),
            operations_per_point: experiment.benchmark.operations,
            warmup_operations_per_point: experiment.benchmark.warmup_operations,
            duration_seconds: experiment.benchmark.duration_seconds,
            concurrency: experiment.benchmark.concurrency,
            object_size_bytes: experiment.benchmark.object_size_bytes,
            policy,
            stop_reason,
            datapoints,
        }
    }
}

impl MetadataDatapointReport {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        instance: &InstanceConfig,
        experiment: &ExperimentSpec,
        workload: MetadataWorkload,
        resource_id: String,
        channel_id: String,
        operation: &str,
        operations: u64,
        store: BTreeMap<String, String>,
        paced: PacedTaskRunReport,
        counters: BTreeMap<String, u64>,
        scan_limit: Option<usize>,
        claim_target_count: Option<usize>,
    ) -> Self {
        Self {
            run_id: experiment.run.run_id.clone(),
            workload: experiment.run.workload.clone(),
            instance_id: instance.id.clone(),
            backend: workload.backend_name().to_string(),
            resource_id,
            channel_id,
            operation: operation.to_string(),
            operations,
            warmup_operations: experiment.benchmark.warmup_operations,
            object_size_bytes: experiment.benchmark.object_size_bytes,
            scan_limit,
            claim_target_count,
            store,
            counters,
            paced: PacedRunSummary::from(paced),
        }
    }
}

pub(super) fn ensure_dynamodb_backend(experiment: &ExperimentSpec) -> Result<(), String> {
    let backend = experiment
        .lambda_channel
        .metadata_backend
        .trim()
        .to_ascii_lowercase();
    if backend == "dynamodb" {
        Ok(())
    } else {
        Err(format!(
            "metadata experiments currently support only lambda_channel.metadata_backend = \"dynamodb\"; got {:?}",
            experiment.lambda_channel.metadata_backend
        ))
    }
}

pub(super) fn sweep_policy(experiment: &ExperimentSpec) -> Result<ThroughputSweepPolicy, String> {
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

pub(super) async fn create_metadata_store(
    instance: &InstanceConfig,
    experiment: &ExperimentSpec,
    resource_id: &str,
) -> Result<MetadataStoreResource, String> {
    let aws_config = AwsClientConfig::for_dynamodb(experiment)?;
    aws_config.apply_process_env();
    let table_prefix = env_any(
        experiment,
        &[
            "LC_BENCH_METADATA_TABLE_PREFIX",
            "LC_BENCH_METADATA_TABLE",
            "P2P_DDB_TABLE",
        ],
    )
    .unwrap_or_else(|| "lcbench-metadata".to_string());
    let table_name = dynamodb_table_name(&table_prefix, resource_id);
    let create_table = env_bool_any(
        experiment,
        &[
            "LC_BENCH_METADATA_CREATE_TABLE",
            "METADATA_CREATE_TABLE",
            "DDB_CREATE_TABLE",
        ],
    )?
    .unwrap_or(true);
    let cleanup_table = env_bool_any(
        experiment,
        &["LC_BENCH_METADATA_CLEANUP_TABLE", "METADATA_CLEANUP_TABLE"],
    )?
    .unwrap_or(true);
    let init_wait_timeout_s = env_u64_any(
        experiment,
        &[
            "LC_BENCH_METADATA_INIT_WAIT_TIMEOUT_S",
            "METADATA_INIT_WAIT_TIMEOUT_S",
        ],
    )?
    .unwrap_or(20);

    let store = AsyncDynamoDbMetadataStore::init_store(
        table_name.clone(),
        aws_config.region_name.clone(),
        aws_config.endpoint_url.clone(),
        create_table,
        init_wait_timeout_s,
    )
    .await
    .map_err(|err| format!("failed to create DynamoDB metadata store: {err}"))?;

    let mut details = BTreeMap::new();
    details.insert("table_name".to_string(), table_name.clone());
    details.insert("instance_id".to_string(), instance.id.clone());
    if let Some(region) = aws_config.region_name.as_ref() {
        details.insert("region".to_string(), region.clone());
    }
    if let Some(endpoint) = aws_config.endpoint_url.as_ref() {
        details.insert("endpoint_url".to_string(), endpoint.clone());
    }

    let cleanup = if cleanup_table {
        vec![MetadataCleanupResource::DynamoDbTable { table_name }]
    } else {
        Vec::new()
    };

    Ok(MetadataStoreResource {
        handle: Arc::new(store) as MetadataStoreHandle,
        details,
        cleanup,
    })
}

pub(super) async fn cleanup_resources(
    resources: Vec<MetadataCleanupResource>,
) -> Result<(), String> {
    let mut errors = Vec::new();
    for resource in resources.into_iter().rev() {
        if let Err(err) = cleanup_resource(resource).await {
            errors.push(err);
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!("cleanup failed: {}", errors.join("; ")))
    }
}

async fn cleanup_resource(resource: MetadataCleanupResource) -> Result<(), String> {
    match resource {
        MetadataCleanupResource::DynamoDbTable { table_name, .. } => {
            eprintln!(
                "deferred AWS cleanup for DynamoDB table {table_name}; run cloudlab/scripts/entrypoints/gc_aws_resources.py by prefix"
            );
            Ok(())
        }
    }
}

pub(super) fn new_channel_meta(channel_id: &str) -> ChannelMetaRecord {
    let now = utc_now_iso_string();
    ChannelMetaRecord {
        channel_id: channel_id.to_string(),
        remaining_count: None,
        next_seq_lower_bound: None,
        receiver_ref_count: 0,
        created_at: now.clone(),
        updated_at: now,
        attrs: NativeMap::new(),
    }
}

pub(super) fn new_elem(
    channel_id: &str,
    seq: i64,
    seed: u64,
    payload_size: usize,
) -> ElemMetaRecord {
    let mut ptr = NativeMap::new();
    ptr.insert(
        "kind".to_string(),
        NativeValue::String("metadata-bench".to_string()),
    );
    ptr.insert("seq".to_string(), NativeValue::Int(seq));
    ptr.insert("seed".to_string(), NativeValue::Int(seed as i64));
    if payload_size > 0 {
        ptr.insert(
            "payload".to_string(),
            NativeValue::String(make_payload(seed, seq, payload_size)),
        );
    }
    ElemMetaRecord::new_normal(
        channel_id.to_string(),
        seq,
        ptr,
        "ready".to_string(),
        utc_now_iso_string(),
        None,
        None,
        None,
        NativeMap::new(),
    )
}

pub(super) async fn create_channel(
    store: &MetadataStoreHandle,
    channel_id: &str,
) -> Result<(), String> {
    store
        .create_channel(new_channel_meta(channel_id))
        .await
        .map_err(|err| format!("failed to create metadata channel {channel_id}: {err}"))?;
    Ok(())
}

pub(super) async fn put_elem_range(
    store: &MetadataStoreHandle,
    channel_id: &str,
    start_seq: usize,
    count: usize,
    payload_size: usize,
    seed: u64,
) -> Result<(), String> {
    for index in 0..count {
        let seq = start_seq
            .checked_add(index)
            .ok_or_else(|| "metadata seq range overflowed usize".to_string())?;
        store
            .put_elem(new_elem(
                channel_id,
                i64_from_usize(seq)?,
                seed,
                payload_size,
            ))
            .await
            .map_err(|err| format!("failed to put warmup/preload elem seq={seq}: {err}"))?;
    }
    Ok(())
}

pub(super) async fn put_elem_range_concurrent(
    store: &MetadataStoreHandle,
    channel_id: &str,
    start_seq: usize,
    count: usize,
    payload_size: usize,
    seed: u64,
    max_in_flight: usize,
) -> Result<(), String> {
    let max_in_flight = max_in_flight.max(1);
    let mut in_flight = FuturesUnordered::new();

    for index in 0..count {
        let seq = start_seq
            .checked_add(index)
            .ok_or_else(|| "metadata seq range overflowed usize".to_string())?;
        let seq_i64 = i64_from_usize(seq)?;
        let store = store.clone();
        let channel_id = channel_id.to_string();

        in_flight.push(async move {
            store
                .put_elem(new_elem(&channel_id, seq_i64, seed, payload_size))
                .await
                .map_err(|err| format!("failed to put preload elem seq={seq}: {err}"))
        });

        if in_flight.len() >= max_in_flight {
            if let Some(result) = in_flight.next().await {
                result?;
            }
        }
    }

    while let Some(result) = in_flight.next().await {
        result?;
    }

    Ok(())
}

pub(super) fn run_config(
    experiment: &ExperimentSpec,
    target_ops_per_s: f64,
) -> Result<PacedTaskRunConfig, String> {
    Ok(PacedTaskRunConfig {
        target_ops_per_s,
        max_in_flight: experiment.benchmark.concurrency,
        pacer_core_id: pacer_core_id_from_experiment(experiment)?,
    })
}

pub(super) fn measured_operations(
    experiment: &ExperimentSpec,
    target_ops_per_s: f64,
) -> Result<u64, String> {
    experiment.benchmark.operations_for_target(target_ops_per_s)
}

pub(super) fn measured_count(
    experiment: &ExperimentSpec,
    target_ops_per_s: f64,
) -> Result<usize, String> {
    usize_from_u64(
        measured_operations(experiment, target_ops_per_s)?,
        "duration-based benchmark operations",
    )
}

pub(super) fn warmup_count(experiment: &ExperimentSpec) -> Result<usize, String> {
    usize_from_u64(
        experiment.benchmark.warmup_operations,
        "benchmark.warmup_operations",
    )
}

pub(super) fn payload_size(experiment: &ExperimentSpec) -> Result<usize, String> {
    usize_from_u64(
        experiment.benchmark.object_size_bytes,
        "benchmark.object_size_bytes",
    )
}

pub(super) fn i64_from_usize(value: usize) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("value {value} is too large for i64"))
}

pub(super) fn unique_resource_id(experiment: &ExperimentSpec, instance: &InstanceConfig) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "{}-{}-{now}",
        sanitize_identifier(&experiment.run.run_id),
        sanitize_identifier(&instance.id)
    )
}

pub(super) fn channel_id(experiment: &ExperimentSpec, resource_id: &str) -> String {
    let prefix = sanitize_identifier(&experiment.lambda_channel.channel_id_prefix);
    format!("{prefix}-{resource_id}")
}

pub(super) fn counter_map(
    counters: impl IntoIterator<Item = (&'static str, u64)>,
) -> BTreeMap<String, u64> {
    counters
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

pub(super) fn with_followup_errors(primary: String, cleanup: Result<(), String>) -> String {
    match cleanup {
        Ok(()) => primary,
        Err(cleanup_err) => format!("{primary}; cleanup also failed: {cleanup_err}"),
    }
}

pub(super) fn env_usize_any(
    experiment: &ExperimentSpec,
    keys: &[&str],
) -> Result<Option<usize>, String> {
    let Some(value) = env_any(experiment, keys) else {
        return Ok(None);
    };
    value
        .parse::<usize>()
        .map(Some)
        .map_err(|err| format!("failed to parse metadata env value {value:?} as usize: {err}"))
}

fn pacer_core_id_from_experiment(experiment: &ExperimentSpec) -> Result<Option<usize>, String> {
    let Some(value) = experiment.env.get("LC_BENCH_PACER_CORE") else {
        return Ok(None);
    };
    value
        .parse::<usize>()
        .map(Some)
        .map_err(|err| format!("failed to parse env.LC_BENCH_PACER_CORE={value:?} as usize: {err}"))
}

fn env_any(experiment: &ExperimentSpec, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| {
            experiment
                .env
                .get(*key)
                .filter(|value| !value.trim().is_empty())
        })
        .cloned()
        .or_else(|| {
            keys.iter().find_map(|key| {
                std::env::var(key)
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            })
        })
}

fn env_bool_any(experiment: &ExperimentSpec, keys: &[&str]) -> Result<Option<bool>, String> {
    let Some(value) = env_any(experiment, keys) else {
        return Ok(None);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(format!("failed to parse boolean env value {value:?}")),
    }
}

fn env_u64_any(experiment: &ExperimentSpec, keys: &[&str]) -> Result<Option<u64>, String> {
    let Some(value) = env_any(experiment, keys) else {
        return Ok(None);
    };
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|err| format!("failed to parse metadata env value {value:?} as u64: {err}"))
}

fn set_process_env(key: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        std::env::set_var(key, value);
    }
}

fn usize_from_u64(value: u64, name: &str) -> Result<usize, String> {
    usize::try_from(value).map_err(|_| format!("{name} is too large for this platform"))
}

fn make_payload(seed: u64, seq: i64, size: usize) -> String {
    let template = format!("{seed}:{seq}:lambda-channel-metadata-benchmark:");
    let mut output = String::with_capacity(size);
    while output.len() < size {
        output.push_str(&template);
    }
    output.truncate(size);
    output
}

fn dynamodb_table_name(prefix: &str, resource_id: &str) -> String {
    let mut name = format!(
        "{}-{}",
        sanitize_dynamodb_part(prefix),
        sanitize_dynamodb_part(resource_id)
    );
    if name.len() > 255 {
        name.truncate(255);
    }
    name
}

fn sanitize_identifier(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            sanitized.push(ch);
        } else {
            sanitized.push('-');
        }
    }
    if sanitized.is_empty() {
        "metadata".to_string()
    } else {
        sanitized
    }
}

fn sanitize_dynamodb_part(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            sanitized.push(ch);
        } else {
            sanitized.push('-');
        }
    }
    sanitized
        .trim_matches(|ch| ch == '-' || ch == '.')
        .chars()
        .take(128)
        .collect::<String>()
        .if_empty("lcbench")
}

trait IfEmpty {
    fn if_empty(self, fallback: &str) -> String;
}

impl IfEmpty for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}
