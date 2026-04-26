use std::collections::BTreeMap;
use std::path::Path;

use lambda_channel::async_promise::AsyncPromiseOutcome;
use lambda_channel::blob_store_impl::p2p_blob_store::{AsyncP2PBlobStore, TrackerTableNamesRecord};
use lambda_channel::blob_store_impl::BlobStoreHandle;

use crate::config::{ExperimentSpec, InstanceConfig};

use super::common::{
    cleanup_resources, dynamodb_table_name, env_bool_any, env_u64_any, path_to_string,
    required_env_any, AwsClientConfig, PutCleanupResource, PutStore,
};

pub(super) async fn create_store(
    instance: &InstanceConfig,
    experiment: &ExperimentSpec,
    resource_dir: &Path,
    resource_id: &str,
) -> Result<PutStore, String> {
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
        blob_meta: dynamodb_table_name(&blob_meta_prefix, resource_id),
        chunk_holders: dynamodb_table_name(&chunk_holders_prefix, resource_id),
    };
    let create_tables = env_bool_any(
        experiment,
        &[
            "LC_BENCH_P2P_CREATE_TRACKER_TABLES",
            "P2P_CREATE_TRACKER_TABLES",
        ],
    )?
    .unwrap_or(true);
    let tracker_init_wait_timeout_s = env_u64_any(
        experiment,
        &[
            "LC_BENCH_P2P_TRACKER_INIT_WAIT_TIMEOUT_S",
            "P2P_TRACKER_INIT_WAIT_TIMEOUT_S",
        ],
    )?
    .unwrap_or(20);

    let cache_dir = resource_dir.join("blob-store-p2p-cache");
    tokio::fs::create_dir_all(&cache_dir).await.map_err(|err| {
        format!(
            "failed to create p2p cache dir {}: {err}",
            cache_dir.display()
        )
    })?;

    let bind_port = endpoint_port(&instance.p2p_advertise_endpoint)
        .unwrap_or(experiment.p2p.chunk_server_port_base);
    let enable_transfer_debug_log = env_bool_any(
        experiment,
        &["LC_BENCH_P2P_TRANSFER_DEBUG_LOG", "P2P_TRANSFER_DEBUG_LOG"],
    )?
    .unwrap_or(false);

    let cleanup = vec![
        PutCleanupResource::LocalDir(resource_dir.to_path_buf()),
        PutCleanupResource::DynamoDbTable {
            table_name: tracker_tables.blob_meta.clone(),
            config: aws_config.clone(),
        },
        PutCleanupResource::DynamoDbTable {
            table_name: tracker_tables.chunk_holders.clone(),
            config: aws_config.clone(),
        },
    ];
    let promise = AsyncP2PBlobStore::new_promise(
        tracker_tables.clone(),
        aws_config.region_name.clone(),
        aws_config.endpoint_url.clone(),
        create_tables,
        tracker_init_wait_timeout_s,
        instance.id.clone(),
        path_to_string(&cache_dir),
        true,
        experiment.p2p.chunk_server_bind_host.clone(),
        bind_port,
        experiment.p2p.chunk_server_runtime_worker_threads,
        instance.p2p_advertise_endpoint.clone(),
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
            let _ = cleanup_resources(cleanup).await;
            return Err(format!("failed to create P2P blob store: {}", err.as_ref()));
        }
        AsyncPromiseOutcome::Canceled => {
            let _ = cleanup_resources(cleanup).await;
            return Err("failed to create P2P blob store: startup promise canceled".to_string());
        }
    };

    let mut details = BTreeMap::new();
    details.insert("resource_dir".to_string(), path_to_string(resource_dir));
    details.insert("cache_dir".to_string(), path_to_string(&cache_dir));
    details.insert("holder_id".to_string(), instance.id.clone());
    details.insert(
        "advertise_endpoint".to_string(),
        instance.p2p_advertise_endpoint.clone(),
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

    Ok(PutStore {
        handle,
        details,
        cleanup,
    })
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
