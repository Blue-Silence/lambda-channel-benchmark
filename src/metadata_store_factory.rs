use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use lambda_channel::metadata_store_impl::dynamodb::AsyncDynamoDbMetadataStore;
use lambda_channel::metadata_store_impl::in_memory::AsyncInMemoryMetadataStore;
use lambda_channel::metadata_store_impl::MetadataStoreHandle;

use crate::config::{ExperimentSpec, InstanceConfig};

const INIT_MAX_ATTEMPTS: usize = 5;
const INIT_RETRY_BASE_DELAY_MS: u64 = 100;

pub(crate) struct CreatedMetadataStore {
    pub(crate) backend: String,
    pub(crate) handle: MetadataStoreHandle,
    pub(crate) details: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
struct AwsClientConfig {
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

pub(crate) async fn create_metadata_store(
    instance: &InstanceConfig,
    experiment: Option<&ExperimentSpec>,
    backend: &str,
    resource_id: &str,
    create_remote_resources: bool,
) -> Result<CreatedMetadataStore, String> {
    let backend = backend.trim().to_ascii_lowercase();
    match backend.as_str() {
        "inmemory" | "in-memory" => {
            let mut details = BTreeMap::new();
            details.insert("instance_id".to_string(), instance.id.clone());
            Ok(CreatedMetadataStore {
                backend: "inmemory".to_string(),
                handle: Arc::new(AsyncInMemoryMetadataStore::default()) as MetadataStoreHandle,
                details,
            })
        }
        "dynamodb" => {
            let experiment = experiment
                .ok_or_else(|| "dynamodb metadata store requires experiment config".to_string())?;
            create_dynamodb_metadata_store(
                instance,
                experiment,
                resource_id,
                create_remote_resources,
            )
            .await
        }
        other => Err(format!("unsupported metadata store backend: {other}")),
    }
}

async fn create_dynamodb_metadata_store(
    instance: &InstanceConfig,
    experiment: &ExperimentSpec,
    resource_id: &str,
    create_remote_resources: bool,
) -> Result<CreatedMetadataStore, String> {
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
    let init_wait_timeout_s = env_u64_any(
        experiment,
        &[
            "LC_BENCH_METADATA_INIT_WAIT_TIMEOUT_S",
            "METADATA_INIT_WAIT_TIMEOUT_S",
        ],
    )?
    .unwrap_or(20);

    let mut last_error = String::new();
    for attempt in 1..=INIT_MAX_ATTEMPTS {
        match AsyncDynamoDbMetadataStore::init_store(
            table_name.clone(),
            aws_config.region_name.clone(),
            aws_config.endpoint_url.clone(),
            create_remote_resources,
            init_wait_timeout_s,
        )
        .await
        {
            Ok(store) => {
                let mut details = BTreeMap::new();
                details.insert("table_name".to_string(), table_name);
                details.insert("instance_id".to_string(), instance.id.clone());
                if let Some(region) = aws_config.region_name {
                    details.insert("region".to_string(), region);
                }
                if let Some(endpoint) = aws_config.endpoint_url {
                    details.insert("endpoint_url".to_string(), endpoint);
                }
                return Ok(CreatedMetadataStore {
                    backend: "dynamodb".to_string(),
                    handle: Arc::new(store) as MetadataStoreHandle,
                    details,
                });
            }
            Err(err) => {
                last_error = format!("{err}");
                if attempt < INIT_MAX_ATTEMPTS {
                    let delay_ms = INIT_RETRY_BASE_DELAY_MS.saturating_mul(1_u64 << (attempt - 1));
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
    Err(format!(
        "failed to init DynamoDB metadata store {table_name} after {INIT_MAX_ATTEMPTS} attempts: {last_error}"
    ))
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

fn env_u64_any(experiment: &ExperimentSpec, keys: &[&str]) -> Result<Option<u64>, String> {
    let Some(value) = env_any(experiment, keys) else {
        return Ok(None);
    };
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|err| format!("failed to parse env value {value:?} as u64: {err}"))
}

fn set_process_env(key: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        std::env::set_var(key, value);
    }
}

fn dynamodb_table_name(prefix: &str, resource_id: &str) -> String {
    let mut table = format!(
        "{}-{}",
        sanitize_dynamodb_part(prefix),
        sanitize_dynamodb_part(resource_id)
    );
    table.truncate(255);
    if table.len() < 3 {
        table.push_str("000");
        table.truncate(3);
    }
    table
}

fn sanitize_dynamodb_part(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('-');
        }
    }
    out.trim_matches('-').to_string()
}
