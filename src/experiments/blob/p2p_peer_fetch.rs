use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::{ExperimentSpec, InstanceConfig, InstancesConfig};
use crate::experiments::blob::control::{participant_by_label, run_blob_get_on_target};
use crate::rpc::protocol::RunBlobGetRequest;
use crate::rpc::server::state::NodeRuntimeState;

pub(super) async fn run(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    instances: &InstancesConfig,
    experiment: ExperimentSpec,
) -> Result<String, String> {
    let run_id = experiment.run.run_id.clone();
    let count = experiment.benchmark.operations as usize;
    let object_size_bytes = experiment.benchmark.object_size_bytes;
    let backend = experiment.benchmark.backend.clone();
    let force_reset = experiment.coordination.force_reset_on_start;
    let barrier_timeout_ms = experiment.coordination.barrier_timeout_ms;
    let holder_id = participant_by_label(&experiment, "holder")
        .ok_or_else(|| "blob.p2p_peer_fetch requires a holder participant".to_string())?;
    let receiver_id = participant_by_label(&experiment, "receiver")
        .ok_or_else(|| "blob.p2p_peer_fetch requires a receiver participant".to_string())?;

    let get = run_blob_get_on_target(
        instances,
        instance,
        runtime,
        &receiver_id,
        RunBlobGetRequest {
            run_id: run_id.clone(),
            peer_instance_id: holder_id,
            count,
            object_size_bytes,
            blob_store_backend: backend,
            barrier_timeout_ms,
            force_reset,
            cleanup: true,
        },
    )
    .await?;

    Ok(format!(
        "blob.p2p_peer_fetch receiver={} holder={} prepared={} materialized={} total_bytes={} get_ms={:.3}",
        get.coordinator_id,
        get.peer_instance_id,
        get.prepared_count,
        get.materialized_count,
        get.total_bytes,
        get.local_get_elapsed_ms,
    ))
}
