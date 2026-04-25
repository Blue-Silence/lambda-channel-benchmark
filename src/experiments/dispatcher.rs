use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::{ExperimentSpec, InstanceConfig, InstancesConfig};
use crate::rpc::server::state::NodeRuntimeState;

pub(crate) async fn run_experiment_on_node(
    instance: &InstanceConfig,
    runtime: &Arc<Mutex<NodeRuntimeState>>,
    instances: &InstancesConfig,
    experiment: ExperimentSpec,
) -> Result<String, String> {
    experiment.validate_with_instances(instances)?;

    if experiment.run.workload.starts_with("blob.") {
        return crate::experiments::blob::run(instance, runtime, instances, experiment).await;
    }

    Err(format!(
        "no direct experiment runner is registered for workload {}",
        experiment.run.workload
    ))
}
