use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_sdk_s3::Client as S3Client;
use lambda_channel::async_promise::AsyncPromiseOutcome;
use lambda_channel::blob_store_impl::local_file_blob_store::AsyncLocalFileBlobStore;
use lambda_channel::blob_store_impl::p2p_blob_store::{AsyncP2PBlobStore, TrackerTableNamesRecord};
use lambda_channel::blob_store_impl::s3_blob_store::AsyncS3BlobStore;
use lambda_channel::blob_store_impl::BlobStoreHandle;
use lambda_channel::common::{NativeMap, NativeValue};

use crate::config::{ExperimentSpec, InstanceConfig};

pub(crate) struct BlobStoreCreateOptions<'a> {
    pub(crate) instance: &'a InstanceConfig,
    pub(crate) experiment: Option<&'a ExperimentSpec>,
    pub(crate) run_dir: &'a Path,
    pub(crate) backend: &'a str,
    pub(crate) resource_id: &'a str,
    pub(crate) root_dir: Option<PathBuf>,
    pub(crate) create_remote_resources: bool,
}

pub(crate) struct CreatedBlobStore {
    pub(crate) backend: String,
    pub(crate) root_dir: Option<String>,
    pub(crate) handle: BlobStoreHandle,
    pub(crate) details: BTreeMap<String, String>,
}

pub(crate) async fn create_blob_store(
    options: BlobStoreCreateOptions<'_>,
) -> Result<CreatedBlobStore, String> {
    let backend = normalize_backend(options.backend);
    match backend.as_str() {
        "local-file" => create_local_file_store(options, backend).await,
        "s3" => create_s3_store(options, backend).await,
        "p2p" => create_p2p_store(options, backend).await,
        other => Err(format!("unsupported blob store backend: {other}")),
    }
}

async fn create_local_file_store(
    options: BlobStoreCreateOptions<'_>,
    backend: String,
) -> Result<CreatedBlobStore, String> {
    let root_dir = options
        .root_dir
        .unwrap_or_else(|| options.run_dir.join("blob-store"));
    tokio::fs::create_dir_all(&root_dir).await.map_err(|err| {
        format!(
            "failed to create local blob store root {}: {err}",
            root_dir.display()
        )
    })?;
    let store = AsyncLocalFileBlobStore::new(path_to_string(&root_dir))
        .await
        .map_err(|err| format!("failed to create local file blob store: {err}"))?;

    let mut details = BTreeMap::new();
    details.insert("root_dir".to_string(), path_to_string(&root_dir));

    Ok(CreatedBlobStore {
        backend,
        root_dir: Some(path_to_string(&root_dir)),
        handle: Arc::new(store) as BlobStoreHandle,
        details,
    })
}

async fn create_s3_store(
    options: BlobStoreCreateOptions<'_>,
    backend: String,
) -> Result<CreatedBlobStore, String> {
    let experiment = options
        .experiment
        .ok_or_else(|| "s3 blob store init requires experiment config".to_string())?;
    let bucket_prefix = required_env_any(
        experiment,
        &[
            "LC_BENCH_S3_BUCKET_PREFIX",
            "LC_BENCH_S3_BUCKET",
            "S3_BUCKET_PREFIX",
            "S3_BUCKET",
        ],
        "S3 bucket prefix",
    )?;
    let bucket = s3_bucket_name(&bucket_prefix, options.resource_id);
    let key_prefix = env_any(experiment, &["LC_BENCH_S3_KEY_PREFIX", "S3_KEY_PREFIX"])
        .unwrap_or_else(|| format!("{}/{}", experiment.run.run_id, options.resource_id));
    let config = AwsClientConfig::for_s3(experiment)?;
    if options.create_remote_resources {
        let client = config.build_s3_client().await?;
        create_s3_bucket(&client, &bucket, config.region_name.as_deref()).await?;
    }
    let store = AsyncS3BlobStore::new(bucket.clone(), key_prefix.clone(), config.to_native_map())
        .await
        .map_err(|err| format!("failed to create S3 blob store: {err}"))?;

    let mut details = BTreeMap::new();
    details.insert("bucket".to_string(), bucket);
    details.insert("bucket_prefix".to_string(), bucket_prefix);
    details.insert("key_prefix".to_string(), key_prefix);
    if let Some(region) = config.region_name {
        details.insert("region".to_string(), region);
    }
    if let Some(endpoint) = config.endpoint_url {
        details.insert("endpoint_url".to_string(), endpoint);
    }

    Ok(CreatedBlobStore {
        backend,
        root_dir: None,
        handle: Arc::new(store) as BlobStoreHandle,
        details,
    })
}

async fn create_p2p_store(
    options: BlobStoreCreateOptions<'_>,
    backend: String,
) -> Result<CreatedBlobStore, String> {
    let experiment = options
        .experiment
        .ok_or_else(|| "p2p blob store init requires experiment config".to_string())?;
    let aws_config = AwsClientConfig::for_dynamodb(experiment)?;
    aws_config.apply_process_env();

    let blob_meta_prefix = required_env_any(
        experiment,
        &[
            "LC_BENCH_P2P_TRACKER_TABLE_META_PREFIX",
            "LC_BENCH_P2P_TRACKER_TABLE_META",
            "P2P_TRACKER_TABLE_META_PREFIX",
            "P2P_TRACKER_TABLE_META",
        ],
        "P2P tracker blob-meta table prefix",
    )?;
    let chunk_holders_prefix = required_env_any(
        experiment,
        &[
            "LC_BENCH_P2P_TRACKER_TABLE_HOLDERS_PREFIX",
            "LC_BENCH_P2P_TRACKER_TABLE_HOLDERS",
            "P2P_TRACKER_TABLE_HOLDERS_PREFIX",
            "P2P_TRACKER_TABLE_HOLDERS",
        ],
        "P2P tracker chunk-holders table prefix",
    )?;
    let tracker_tables = TrackerTableNamesRecord {
        blob_meta: dynamodb_table_name(&blob_meta_prefix, options.resource_id),
        chunk_holders: dynamodb_table_name(&chunk_holders_prefix, options.resource_id),
    };
    let tracker_init_wait_timeout_s = env_u64_any(
        experiment,
        &[
            "LC_BENCH_P2P_TRACKER_INIT_WAIT_TIMEOUT_S",
            "P2P_TRACKER_INIT_WAIT_TIMEOUT_S",
        ],
    )?
    .unwrap_or(20);
    let enable_transfer_debug_log = env_bool_any(
        experiment,
        &["LC_BENCH_P2P_TRANSFER_DEBUG_LOG", "P2P_TRANSFER_DEBUG_LOG"],
    )?
    .unwrap_or(false);

    let cache_dir = options
        .root_dir
        .unwrap_or_else(|| options.run_dir.join("blob-store-p2p-cache"));
    tokio::fs::create_dir_all(&cache_dir).await.map_err(|err| {
        format!(
            "failed to create p2p cache dir {}: {err}",
            cache_dir.display()
        )
    })?;

    let bind_port = endpoint_port(&options.instance.p2p_advertise_endpoint)
        .unwrap_or(experiment.p2p.chunk_server_port_base);
    let promise = AsyncP2PBlobStore::new_promise(
        tracker_tables.clone(),
        aws_config.region_name.clone(),
        aws_config.endpoint_url.clone(),
        options.create_remote_resources,
        tracker_init_wait_timeout_s,
        options.instance.id.clone(),
        path_to_string(&cache_dir),
        true,
        experiment.p2p.chunk_server_bind_host.clone(),
        bind_port,
        experiment.p2p.chunk_server_runtime_worker_threads,
        options.instance.p2p_advertise_endpoint.clone(),
        experiment.p2p.accel_probability.unwrap_or(0.9),
        experiment.p2p.enable_accel,
        enable_transfer_debug_log,
        None,
        false,
        Some(5.0),
        0.05,
        false,
        0.0,
        0.1,
        false,
        experiment.p2p.non_abortable_task_workers,
    )
    .await;

    let handle: BlobStoreHandle = match promise.wait().await {
        AsyncPromiseOutcome::Result(store) => store,
        AsyncPromiseOutcome::Error(err) => {
            return Err(format!("failed to create P2P blob store: {}", err.as_ref()));
        }
        AsyncPromiseOutcome::Canceled => {
            return Err("failed to create P2P blob store: startup promise canceled".to_string());
        }
    };

    let mut details = BTreeMap::new();
    details.insert("cache_dir".to_string(), path_to_string(&cache_dir));
    details.insert("holder_id".to_string(), options.instance.id.clone());
    details.insert(
        "advertise_endpoint".to_string(),
        options.instance.p2p_advertise_endpoint.clone(),
    );
    details.insert(
        "tracker_blob_meta_table".to_string(),
        tracker_tables.blob_meta,
    );
    details.insert(
        "tracker_chunk_holders_table".to_string(),
        tracker_tables.chunk_holders,
    );
    if let Some(region) = aws_config.region_name {
        details.insert("region".to_string(), region);
    }
    if let Some(endpoint) = aws_config.endpoint_url {
        details.insert("endpoint_url".to_string(), endpoint);
    }

    Ok(CreatedBlobStore {
        backend,
        root_dir: Some(path_to_string(&cache_dir)),
        handle,
        details,
    })
}

pub(crate) fn unique_resource_id(run_id: &str, instance_id: &str) -> String {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let entropy = format!("{:x}{:x}", std::process::id(), now_nanos);
    let entropy = tail_ascii(&entropy, 16);
    format!(
        "{}-{}-{}",
        truncate_ascii(&sanitize_dns_part(run_id), 16),
        truncate_ascii(&sanitize_dns_part(instance_id), 8),
        entropy
    )
}

fn normalize_backend(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "localfs" => "local-file".to_string(),
        other => other.to_string(),
    }
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn endpoint_port(endpoint: &str) -> Option<u16> {
    let without_scheme = endpoint
        .trim()
        .rsplit_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(endpoint.trim());
    let authority = without_scheme.split('/').next().unwrap_or(without_scheme);
    authority.rsplit_once(':')?.1.parse::<u16>().ok()
}

fn s3_bucket_name(prefix: &str, resource_id: &str) -> String {
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

fn dynamodb_table_name(prefix: &str, resource_id: &str) -> String {
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

fn env_any(experiment: &ExperimentSpec, keys: &[&str]) -> Option<String> {
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

fn required_env_any(
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

fn env_bool_any(experiment: &ExperimentSpec, keys: &[&str]) -> Result<Option<bool>, String> {
    env_any(experiment, keys)
        .map(|value| {
            parse_bool(&value).ok_or_else(|| format!("failed to parse boolean env value {value:?}"))
        })
        .transpose()
}

fn env_u64_any(experiment: &ExperimentSpec, keys: &[&str]) -> Result<Option<u64>, String> {
    env_any(experiment, keys)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|err| format!("failed to parse integer env value {value:?}: {err}"))
        })
        .transpose()
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

#[derive(Clone, Debug, Default)]
struct AwsClientConfig {
    profile_name: Option<String>,
    region_name: Option<String>,
    endpoint_url: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    force_path_style: bool,
}

impl AwsClientConfig {
    fn for_s3(experiment: &ExperimentSpec) -> Result<Self, String> {
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
            force_path_style: false,
        }
        .validated()?)
    }

    fn to_native_map(&self) -> NativeMap {
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

    fn apply_process_env(&self) {
        set_process_env("AWS_ACCESS_KEY_ID", self.access_key_id.as_deref());
        set_process_env("AWS_SECRET_ACCESS_KEY", self.secret_access_key.as_deref());
        set_process_env("AWS_SESSION_TOKEN", self.session_token.as_deref());
        set_process_env("AWS_PROFILE", self.profile_name.as_deref());
    }

    async fn build_s3_client(&self) -> Result<S3Client, String> {
        let shared_config = self.load_shared_config().await?;
        let config = aws_sdk_s3::config::Builder::from(&shared_config)
            .force_path_style(self.force_path_style)
            .build();
        Ok(S3Client::from_conf(config))
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
            _ => {
                return Err(
                    "AWS config requires both access key id and secret access key when either is set"
                        .to_string(),
                );
            }
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

fn insert_native_string(map: &mut NativeMap, key: &str, value: Option<String>) {
    map.insert(
        key.to_string(),
        value.map(NativeValue::String).unwrap_or(NativeValue::Null),
    );
}

fn insert_native_bool(map: &mut NativeMap, key: &str, value: Option<bool>) {
    map.insert(
        key.to_string(),
        value.map(NativeValue::Bool).unwrap_or(NativeValue::Null),
    );
}

fn set_process_env(name: &str, value: Option<&str>) {
    if let Some(value) = value {
        std::env::set_var(name, value);
    }
}

async fn create_s3_bucket(
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
