use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_sdk_s3::Client as S3Client;
use lambda_channel::blob_store_impl::BlobStoreHandle;
use lambda_channel::common::{NativeMap, NativeValue};
use serde::Serialize;

use crate::config::{ExperimentSpec, InstanceConfig};
use crate::driver::latency::LatencySummary;
use crate::driver::paced::{
    boxed_task, run_paced_tasks, PacedTask, PacedTaskRunConfig, PacedTaskRunReport,
};
use crate::driver::sweep::{SweepStopReason, ThroughputSweepPolicy};

pub(super) struct PutStore {
    pub(super) handle: BlobStoreHandle,
    pub(super) details: BTreeMap<String, String>,
    pub(super) cleanup: Vec<PutCleanupResource>,
}

#[derive(Clone, Debug)]
pub(super) enum PutCleanupResource {
    LocalDir(PathBuf),
    S3Bucket {
        bucket: String,
        config: AwsClientConfig,
    },
    DynamoDbTable {
        table_name: String,
        config: AwsClientConfig,
    },
}

#[derive(Clone, Debug, Default)]
pub(super) struct AwsClientConfig {
    pub(super) profile_name: Option<String>,
    pub(super) region_name: Option<String>,
    pub(super) endpoint_url: Option<String>,
    pub(super) access_key_id: Option<String>,
    pub(super) secret_access_key: Option<String>,
    pub(super) session_token: Option<String>,
    pub(super) force_path_style: bool,
}

impl AwsClientConfig {
    pub(super) fn for_s3(experiment: &ExperimentSpec) -> Result<Self, String> {
        Ok(Self {
            profile_name: env_any(experiment, &["LC_BENCH_AWS_PROFILE", "AWS_PROFILE"]),
            region_name: env_any(
                experiment,
                &["LC_BENCH_AWS_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"],
            ),
            endpoint_url: env_any(
                experiment,
                &[
                    "LC_BENCH_S3_ENDPOINT_URL",
                    "AWS_S3_ENDPOINT_URL",
                    "AWS_ENDPOINT_URL",
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
            force_path_style: env_bool_any(
                experiment,
                &["LC_BENCH_S3_FORCE_PATH_STYLE", "AWS_S3_FORCE_PATH_STYLE"],
            )?
            .unwrap_or(false),
        }
        .validated()?)
    }

    pub(super) fn for_dynamodb(experiment: &ExperimentSpec) -> Result<Self, String> {
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
            force_path_style: false,
        }
        .validated()?)
    }

    pub(super) fn to_native_map(&self) -> NativeMap {
        let mut map = NativeMap::new();
        insert_native_string(&mut map, "profile_name", self.profile_name.clone());
        insert_native_string(&mut map, "region_name", self.region_name.clone());
        insert_native_string(&mut map, "endpoint_url", self.endpoint_url.clone());
        insert_native_string(&mut map, "aws_access_key_id", self.access_key_id.clone());
        insert_native_string(
            &mut map,
            "aws_secret_access_key",
            self.secret_access_key.clone(),
        );
        insert_native_string(&mut map, "aws_session_token", self.session_token.clone());
        insert_native_bool(&mut map, "force_path_style", Some(self.force_path_style));
        map
    }

    pub(super) fn apply_process_env(&self) {
        set_process_env("AWS_ACCESS_KEY_ID", self.access_key_id.as_deref());
        set_process_env("AWS_SECRET_ACCESS_KEY", self.secret_access_key.as_deref());
        set_process_env("AWS_SESSION_TOKEN", self.session_token.as_deref());
        set_process_env("AWS_PROFILE", self.profile_name.as_deref());
    }

    pub(super) async fn build_s3_client(&self) -> Result<S3Client, String> {
        let shared_config = self.load_shared_config().await?;
        let config = aws_sdk_s3::config::Builder::from(&shared_config)
            .force_path_style(self.force_path_style)
            .build();
        Ok(S3Client::from_conf(config))
    }

    async fn build_dynamodb_client(&self) -> Result<DynamoDbClient, String> {
        let shared_config = self.load_shared_config().await?;
        Ok(DynamoDbClient::new(&shared_config))
    }

    async fn load_shared_config(&self) -> Result<aws_config::SdkConfig, String> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(profile_name) = self.profile_name.as_deref() {
            loader = loader.profile_name(profile_name);
        }
        if let Some(region_name) = self.region_name.as_deref() {
            loader = loader.region(Region::new(region_name.to_string()));
        }
        if let Some(endpoint_url) = self.endpoint_url.as_deref() {
            loader = loader.endpoint_url(endpoint_url);
        }
        match (
            self.access_key_id.as_deref(),
            self.secret_access_key.as_deref(),
        ) {
            (None, None) => {}
            (Some(access_key_id), Some(secret_access_key)) => {
                loader = loader.credentials_provider(Credentials::new(
                    access_key_id,
                    secret_access_key,
                    self.session_token.clone(),
                    None,
                    "lc-bench",
                ));
            }
            _ => return Err(
                "AWS config requires both access key id and secret access key when either is set"
                    .to_string(),
            ),
        }
        Ok(loader.load().await)
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
pub(super) struct PutSweepReport {
    run_id: String,
    workload: String,
    instance_id: String,
    backend: String,
    operations_per_point: u64,
    warmup_operations_per_point: u64,
    object_size_bytes: u64,
    policy: ThroughputSweepPolicy,
    stop_reason: SweepStopReason,
    datapoints: Vec<PutDatapointReport>,
}

#[derive(Debug, Serialize)]
pub(super) struct PutDatapointReport {
    run_id: String,
    workload: String,
    instance_id: String,
    backend: String,
    resource_id: String,
    operations: u64,
    warmup_operations: u64,
    object_size_bytes: u64,
    total_bytes: u64,
    input_dir: String,
    store: BTreeMap<String, String>,
    paced: PacedRunSummary,
}

pub(super) struct PutDatapointOutcome {
    pub(super) report: PutDatapointReport,
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

impl From<crate::driver::paced::PacedTaskRunReport> for PacedRunSummary {
    fn from(report: crate::driver::paced::PacedTaskRunReport) -> Self {
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

impl PutSweepReport {
    pub(super) fn new(
        instance: &InstanceConfig,
        experiment: &ExperimentSpec,
        backend: &str,
        policy: ThroughputSweepPolicy,
        stop_reason: SweepStopReason,
        datapoints: Vec<PutDatapointReport>,
    ) -> Self {
        Self {
            run_id: experiment.run.run_id.clone(),
            workload: experiment.run.workload.clone(),
            instance_id: instance.id.clone(),
            backend: backend.to_string(),
            operations_per_point: experiment.benchmark.operations,
            warmup_operations_per_point: experiment.benchmark.warmup_operations,
            object_size_bytes: experiment.benchmark.object_size_bytes,
            policy,
            stop_reason,
            datapoints,
        }
    }
}

pub(super) async fn run_put_datapoint(
    instance: &InstanceConfig,
    experiment: &ExperimentSpec,
    backend: &str,
    resource_id: &str,
    resource_dir: &Path,
    store: BlobStoreHandle,
    store_details: BTreeMap<String, String>,
    target_ops_per_s: f64,
) -> Result<PutDatapointOutcome, String> {
    let object_size = object_size_usize(experiment.benchmark.object_size_bytes)?;
    let measured_count = usize::try_from(experiment.benchmark.operations)
        .map_err(|_| "benchmark.operations is too large for this platform".to_string())?;
    let warmup_count = usize::try_from(experiment.benchmark.warmup_operations)
        .map_err(|_| "benchmark.warmup_operations is too large for this platform".to_string())?;
    let total_input_count = warmup_count
        .checked_add(measured_count)
        .ok_or_else(|| "warmup_operations + operations is too large".to_string())?;

    let (input_dir, paced) = execute_put(
        &store,
        resource_dir,
        experiment,
        total_input_count,
        warmup_count,
        object_size,
        target_ops_per_s,
    )
    .await?;

    let report = PutDatapointReport {
        run_id: experiment.run.run_id.clone(),
        workload: experiment.run.workload.clone(),
        instance_id: instance.id.clone(),
        backend: backend.to_string(),
        resource_id: resource_id.to_string(),
        operations: experiment.benchmark.operations,
        warmup_operations: experiment.benchmark.warmup_operations,
        object_size_bytes: experiment.benchmark.object_size_bytes,
        total_bytes: experiment
            .benchmark
            .operations
            .saturating_mul(experiment.benchmark.object_size_bytes),
        input_dir: path_to_string(&input_dir),
        store: store_details,
        paced: PacedRunSummary::from(paced.clone()),
    };

    Ok(PutDatapointOutcome { report, paced })
}

async fn execute_put(
    store: &BlobStoreHandle,
    resource_dir: &Path,
    experiment: &ExperimentSpec,
    total_input_count: usize,
    warmup_count: usize,
    object_size: usize,
    target_ops_per_s: f64,
) -> Result<(PathBuf, crate::driver::paced::PacedTaskRunReport), String> {
    let input_dir = resource_dir.join("put-inputs");
    let input_paths = prepare_payload_files(
        &input_dir,
        &experiment.run.run_id,
        experiment.run.seed,
        total_input_count,
        object_size,
    )
    .await?;
    let (warmup_paths, measured_paths) = input_paths.split_at(warmup_count);

    for path in warmup_paths {
        let path = path_to_string(path);
        store
            .put_file(&path)
            .await
            .map_err(|err| format!("warmup put failed for {path}: {err}"))?;
    }

    let tasks = build_put_tasks(store.clone(), measured_paths);
    let paced = run_paced_tasks(
        tasks,
        PacedTaskRunConfig {
            target_ops_per_s,
            max_in_flight: experiment.benchmark.concurrency,
        },
    )
    .await?;
    Ok((input_dir, paced))
}

fn build_put_tasks(store: BlobStoreHandle, paths: &[PathBuf]) -> Vec<PacedTask> {
    paths
        .iter()
        .map(|path| {
            let store = store.clone();
            let path = path_to_string(path);
            boxed_task(async move {
                store
                    .put_file(&path)
                    .await
                    .map(|_| ())
                    .map_err(|err| format!("put failed for {path}: {err}"))
            })
        })
        .collect()
}

pub(super) async fn cleanup_resources(resources: Vec<PutCleanupResource>) -> Result<(), String> {
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

async fn cleanup_resource(resource: PutCleanupResource) -> Result<(), String> {
    match resource {
        PutCleanupResource::LocalDir(path) => cleanup_local_dir(&path).await,
        PutCleanupResource::S3Bucket { bucket, config } => {
            cleanup_s3_bucket(&bucket, &config).await
        }
        PutCleanupResource::DynamoDbTable { table_name, config } => {
            cleanup_dynamodb_table(&table_name, &config).await
        }
    }
}

async fn cleanup_local_dir(path: &Path) -> Result<(), String> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!(
            "failed to remove local resource dir {}: {err}",
            path.display()
        )),
    }
}

pub(super) async fn create_s3_bucket(
    client: &S3Client,
    bucket: &str,
    region_name: Option<&str>,
) -> Result<(), String> {
    let mut request = client.create_bucket().bucket(bucket);
    if let Some(region_name) = region_name.filter(|region| *region != "us-east-1") {
        request = request.create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(region_name))
                .build(),
        );
    }
    request
        .send()
        .await
        .map(|_| ())
        .map_err(|err| format!("failed to create S3 bucket {bucket}: {err}"))
}

pub(super) async fn cleanup_s3_bucket(
    bucket: &str,
    config: &AwsClientConfig,
) -> Result<(), String> {
    let client = config.build_s3_client().await?;
    let mut continuation_token = None;
    loop {
        let mut request = client.list_objects_v2().bucket(bucket);
        if let Some(token) = continuation_token.as_deref() {
            request = request.continuation_token(token);
        }
        let listed = match request.send().await {
            Ok(listed) => listed,
            Err(err) if aws_error_is_missing(&err) => return Ok(()),
            Err(err) => return Err(format!("failed to list S3 bucket {bucket}: {err}")),
        };
        for object in listed.contents() {
            if let Some(key) = object.key() {
                client
                    .delete_object()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(|err| {
                        format!("failed to delete S3 object s3://{bucket}/{key}: {err}")
                    })?;
            }
        }
        if listed.is_truncated().unwrap_or(false) {
            continuation_token = listed.next_continuation_token().map(str::to_string);
            if continuation_token.is_none() {
                return Err(format!(
                    "S3 bucket {bucket} listing was truncated without a continuation token"
                ));
            }
        } else {
            break;
        }
    }

    match client.delete_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(err) if aws_error_is_missing(&err) => Ok(()),
        Err(err) => Err(format!("failed to delete S3 bucket {bucket}: {err}")),
    }
}

async fn cleanup_dynamodb_table(table_name: &str, config: &AwsClientConfig) -> Result<(), String> {
    let client = config.build_dynamodb_client().await?;
    match client.delete_table().table_name(table_name).send().await {
        Ok(_) => Ok(()),
        Err(err) if aws_error_is_missing(&err) => Ok(()),
        Err(err) => Err(format!(
            "failed to delete DynamoDB table {table_name}: {err}"
        )),
    }
}

async fn prepare_payload_files(
    dir: &Path,
    run_id: &str,
    seed: u64,
    count: usize,
    object_size: usize,
) -> Result<Vec<PathBuf>, String> {
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|err| format!("failed to create input dir {}: {err}", dir.display()))?;

    let mut paths = Vec::with_capacity(count);
    for index in 0..count {
        let path = dir.join(format!("object_{index:08}.bin"));
        let payload = make_small_unique_payload(run_id, seed, index as u64, object_size);
        tokio::fs::write(&path, payload)
            .await
            .map_err(|err| format!("failed to write payload {}: {err}", path.display()))?;
        paths.push(path);
    }
    Ok(paths)
}

fn make_small_unique_payload(run_id: &str, seed: u64, index: u64, size: usize) -> Vec<u8> {
    let mut payload = vec![0_u8; size];
    payload[..8].copy_from_slice(&index.to_le_bytes());

    let mut state = hash_run_id(run_id) ^ seed ^ index.rotate_left(17);
    for chunk in payload[8..].chunks_mut(8) {
        state = splitmix64(state);
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    payload
}

fn hash_run_id(run_id: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in run_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
    mixed ^ (mixed >> 31)
}

pub(super) fn unique_resource_id(experiment: &ExperimentSpec, instance: &InstanceConfig) -> String {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let entropy = format!("{:x}{:x}", std::process::id(), now_nanos);
    let entropy = tail_ascii(&entropy, 16);
    format!(
        "{}-{}-{}",
        truncate_ascii(&sanitize_dns_part(&experiment.run.run_id), 16),
        truncate_ascii(&sanitize_dns_part(&instance.id), 8),
        entropy
    )
}

pub(super) fn s3_bucket_name(prefix: &str, resource_id: &str) -> String {
    let suffix = sanitize_dns_part(resource_id);
    let prefix = sanitize_dns_part(prefix);
    let prefix = if prefix.is_empty() {
        "lcbench".to_string()
    } else {
        prefix
    };
    let max_prefix_len = 63_usize.saturating_sub(suffix.len()).saturating_sub(1);
    let prefix = truncate_ascii(&prefix, max_prefix_len.max(1));
    let mut bucket = format!("{prefix}-{suffix}");
    while bucket.len() < 3 {
        bucket.push('0');
    }
    bucket.trim_matches('-').to_string()
}

pub(super) fn dynamodb_table_name(prefix: &str, resource_id: &str) -> String {
    let suffix = sanitize_dynamodb_part(resource_id);
    let prefix = sanitize_dynamodb_part(prefix);
    let prefix = if prefix.is_empty() {
        "lcbench".to_string()
    } else {
        prefix
    };
    let max_prefix_len = 255_usize.saturating_sub(suffix.len()).saturating_sub(1);
    let prefix = truncate_ascii(&prefix, max_prefix_len.max(1));
    let mut table = format!("{prefix}-{suffix}");
    while table.len() < 3 {
        table.push('0');
    }
    table
}

fn sanitize_dns_part(value: &str) -> String {
    collapse_separators(
        value.trim().to_ascii_lowercase().chars().map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch
            } else {
                '-'
            }
        }),
        '-',
    )
}

fn sanitize_dynamodb_part(value: &str) -> String {
    collapse_separators(
        value.trim().chars().map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
                ch
            } else {
                '-'
            }
        }),
        '-',
    )
}

fn collapse_separators(chars: impl Iterator<Item = char>, separator: char) -> String {
    let mut out = String::new();
    let mut last_was_separator = true;
    for ch in chars {
        if ch == separator {
            if !last_was_separator {
                out.push(separator);
                last_was_separator = true;
            }
        } else {
            out.push(ch);
            last_was_separator = false;
        }
    }
    while out.ends_with(separator) {
        out.pop();
    }
    out
}

fn truncate_ascii(value: &str, max_len: usize) -> String {
    value.chars().take(max_len).collect()
}

fn tail_ascii(value: &str, max_len: usize) -> String {
    value
        .chars()
        .rev()
        .take(max_len)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

pub(super) fn with_followup_errors(
    primary: String,
    close_result: Result<(), String>,
    cleanup_result: Result<(), String>,
) -> String {
    let mut message = primary;
    if let Err(err) = close_result {
        message.push_str("; also ");
        message.push_str(&err);
    }
    if let Err(err) = cleanup_result {
        message.push_str("; also ");
        message.push_str(&err);
    }
    message
}

fn aws_error_is_missing(err: &(impl std::fmt::Debug + std::fmt::Display)) -> bool {
    let detail = format!("{err} {err:?}");
    detail.contains("NoSuchBucket")
        || detail.contains("NoSuchKey")
        || detail.contains("NotFound")
        || detail.contains("NotFoundException")
        || detail.contains("ResourceNotFoundException")
}

fn object_size_usize(object_size_bytes: u64) -> Result<usize, String> {
    if object_size_bytes < 8 {
        return Err(
            "blob put object_size_bytes must be at least 8 so each tiny object can carry a unique id"
                .to_string(),
        );
    }
    usize::try_from(object_size_bytes)
        .map_err(|_| "object_size_bytes is too large for this platform".to_string())
}

pub(super) fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

pub(super) fn sanitize_path_part(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub(super) fn env_any(experiment: &ExperimentSpec, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| {
            experiment
                .env
                .get(*key)
                .cloned()
                .or_else(|| std::env::var(key).ok())
        })
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn required_env_any(
    experiment: &ExperimentSpec,
    keys: &[&str],
    description: &str,
) -> Result<String, String> {
    env_any(experiment, keys).ok_or_else(|| {
        format!(
            "{description} is required; set one of {} in [env] or the process environment",
            keys.join(", ")
        )
    })
}

pub(super) fn env_bool_any(
    experiment: &ExperimentSpec,
    keys: &[&str],
) -> Result<Option<bool>, String> {
    let Some(value) = env_any(experiment, keys) else {
        return Ok(None);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "n" | "off" => Ok(Some(false)),
        _ => Err(format!(
            "invalid boolean value {value:?} for one of {}",
            keys.join(", ")
        )),
    }
}

pub(super) fn env_u64_any(
    experiment: &ExperimentSpec,
    keys: &[&str],
) -> Result<Option<u64>, String> {
    let Some(value) = env_any(experiment, keys) else {
        return Ok(None);
    };
    value.parse::<u64>().map(Some).map_err(|err| {
        format!(
            "invalid u64 value {value:?} for one of {}: {err}",
            keys.join(", ")
        )
    })
}

pub(super) fn insert_native_string(map: &mut NativeMap, key: &str, value: Option<String>) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        map.insert(key.to_string(), NativeValue::String(value));
    }
}

pub(super) fn insert_native_bool(map: &mut NativeMap, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        map.insert(key.to_string(), NativeValue::Bool(value));
    }
}

fn set_process_env(key: &str, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    std::env::set_var(key, value);
}
