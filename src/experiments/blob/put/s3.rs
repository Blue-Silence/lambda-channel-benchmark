use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use lambda_channel::blob_store_impl::s3_blob_store::AsyncS3BlobStore;
use lambda_channel::blob_store_impl::BlobStoreHandle;

use crate::config::{ExperimentSpec, InstanceConfig};

use super::common::{
    cleanup_resources, create_s3_bucket, env_any, path_to_string, required_env_any, s3_bucket_name,
    AwsClientConfig, PutCleanupResource, PutStore,
};

pub(super) async fn create_store(
    instance: &InstanceConfig,
    experiment: &ExperimentSpec,
    resource_dir: &Path,
    resource_id: &str,
) -> Result<PutStore, String> {
    let bucket_prefix = required_env_any(
        experiment,
        &[
            "LC_BENCH_S3_BUCKET_PREFIX",
            "LC_BENCH_S3_BUCKET",
            "S3_BUCKET_PREFIX",
            "S3_BUCKET",
        ],
        "S3 blob put bucket prefix",
    )?;
    let bucket = s3_bucket_name(&bucket_prefix, resource_id);
    let key_prefix = env_any(experiment, &["LC_BENCH_S3_KEY_PREFIX", "S3_KEY_PREFIX"])
        .unwrap_or_else(|| format!("{}/{}", experiment.run.run_id, instance.id));
    let config = AwsClientConfig::for_s3(experiment)?;
    let client = config.build_s3_client().await?;
    create_s3_bucket(&client, &bucket, config.region_name.as_deref()).await?;

    let cleanup = vec![
        PutCleanupResource::LocalDir(resource_dir.to_path_buf()),
        PutCleanupResource::S3Bucket {
            bucket: bucket.clone(),
        },
    ];
    let store =
        match AsyncS3BlobStore::new(bucket.clone(), key_prefix.clone(), config.to_native_map())
            .await
        {
            Ok(store) => store,
            Err(err) => {
                let _ = cleanup_resources(cleanup).await;
                return Err(format!("failed to create S3 blob store: {err}"));
            }
        };

    let mut details = BTreeMap::new();
    details.insert("resource_dir".to_string(), path_to_string(resource_dir));
    details.insert("bucket".to_string(), bucket);
    details.insert("bucket_prefix".to_string(), bucket_prefix);
    details.insert("key_prefix".to_string(), key_prefix);
    if let Some(region) = config.region_name.clone() {
        details.insert("region".to_string(), region);
    }
    if let Some(endpoint) = config.endpoint_url.clone() {
        details.insert("endpoint_url".to_string(), endpoint);
    }

    Ok(PutStore {
        handle: Arc::new(store) as BlobStoreHandle,
        details,
        cleanup,
    })
}
