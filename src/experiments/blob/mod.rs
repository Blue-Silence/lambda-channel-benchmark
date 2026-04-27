mod control;
mod get_materialize;
mod multi_getter;
mod p2p_peer_fetch;
mod put;

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::{ExperimentSpec, InstanceConfig, InstancesConfig};
use crate::rpc::server::state::NodeRuntimeState;

pub(crate) use control::run_blob_get_on_node;

pub(crate) async fn run(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    instances: &InstancesConfig,
    experiment: ExperimentSpec,
) -> Result<String, String> {
    match experiment.run.workload.as_str() {
        "blob.put.local_file" | "blob.put.s3" | "blob.put.p2p" => {
            put::run(instance, runtime, instances, experiment).await
        }
        "blob.get_materialize" | "blob.p2p_local_hit" => {
            get_materialize::run(instance, runtime, instances, experiment).await
        }
        "blob.p2p_peer_fetch" => {
            p2p_peer_fetch::run(instance, runtime, instances, experiment).await
        }
        "blob.multi_getter" => multi_getter::run(instance, runtime, instances, experiment).await,
        "blob.persist_upload" | "blob.fallback_fetch" => Err(format!(
            "{} direct orchestration is not implemented yet; add persist/cache reset primitives first",
            experiment.run.workload
        )),
        other => Err(format!(
            "no direct experiment runner is registered for workload {other}"
        )),
    }
}
