mod control;
mod single_sender_multi_receiver;

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::{ExperimentSpec, InstanceConfig, InstancesConfig};
use crate::rpc::server::state::NodeRuntimeState;

pub(crate) async fn run(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    instances: &InstancesConfig,
    experiment: ExperimentSpec,
) -> Result<String, String> {
    match experiment.run.workload.as_str() {
        "channel.single_sender_multi_receiver" => {
            single_sender_multi_receiver::run(instance, runtime, instances, experiment).await
        }
        other => Err(format!(
            "no direct channel experiment runner is registered for workload {other}"
        )),
    }
}
