use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use lambda_channel::blob_store_impl::local_file_blob_store::AsyncLocalFileBlobStore;
use lambda_channel::blob_store_impl::BlobStoreHandle;

use crate::config::{ExperimentSpec, InstanceConfig};

use super::common::{path_to_string, PutCleanupResource, PutStore};

pub(super) async fn create_store(
    _instance: &InstanceConfig,
    _experiment: &ExperimentSpec,
    resource_dir: &Path,
    _resource_id: &str,
) -> Result<PutStore, String> {
    let root_dir = resource_dir.join("blob-store-local-file");
    tokio::fs::create_dir_all(&root_dir).await.map_err(|err| {
        format!(
            "failed to create local blob store dir {}: {err}",
            root_dir.display()
        )
    })?;
    let store = AsyncLocalFileBlobStore::new(path_to_string(&root_dir))
        .await
        .map_err(|err| format!("failed to create local file blob store: {err}"))?;

    let mut details = BTreeMap::new();
    details.insert("resource_dir".to_string(), path_to_string(resource_dir));
    details.insert("root_dir".to_string(), path_to_string(&root_dir));

    Ok(PutStore {
        handle: Arc::new(store) as BlobStoreHandle,
        details,
        cleanup: vec![PutCleanupResource::LocalDir(resource_dir.to_path_buf())],
    })
}
