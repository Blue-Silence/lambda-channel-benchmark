use tarpc::context;

use crate::config::{experiment_summary, instances_summary, ExperimentSpec, InstancesConfig};
use crate::rpc::{
    connect_node, connect_node_addr, serve_node, RunBlobGetRequest, RunExperimentRequest,
};

#[derive(Clone, Debug, Default)]
pub struct NodeOptions {
    pub instance_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct TriggerOptions {
    pub coordinator_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ProxyOptions {
    pub rpc_addr: String,
}

#[derive(Clone, Debug)]
pub struct BlobGetOptions {
    pub coordinator_id: Option<String>,
    pub peer_instance_id: String,
    pub run_id: String,
    pub count: usize,
    pub object_size_bytes: u64,
    pub blob_store_backend: String,
    pub cleanup: bool,
}

pub async fn run_node(instances: InstancesConfig, options: NodeOptions) -> Result<(), String> {
    instances.validate()?;
    let instance_id = resolve_node_instance_id(&instances, options.instance_id)?;
    let instance = instances
        .find_instance(&instance_id)
        .ok_or_else(|| format!("unknown instance id: {instance_id}"))?;
    let instance = instance.clone();

    println!(
        "node instance list loaded: {}",
        instances_summary(&instances)
    );
    println!(
        "node instance={} rpc_addr={} p2p_advertise_endpoint={} work_dir={}",
        instance.id,
        instance.rpc_addr,
        instance.p2p_advertise_endpoint,
        instance.work_dir.display(),
    );
    serve_node(instances, instance).await
}

pub async fn run_trigger(
    instances: InstancesConfig,
    experiment: ExperimentSpec,
    options: TriggerOptions,
) -> Result<(), String> {
    experiment.validate_with_instances(&instances)?;
    let coordinator_id = resolve_coordinator_id(&instances, &experiment, options.coordinator_id)?;
    let coordinator = instances
        .find_instance(&coordinator_id)
        .ok_or_else(|| format!("unknown coordinator instance id: {coordinator_id}"))?;

    println!(
        "triggering experiment {} on coordinator={} rpc_addr={}",
        experiment_summary(&experiment),
        coordinator.id,
        coordinator.rpc_addr
    );

    let client = connect_node(coordinator).await?;
    let response = client
        .run_experiment(context::current(), RunExperimentRequest { experiment })
        .await
        .map_err(|err| format!("run_experiment RPC failed for {}: {err}", coordinator.id))?;

    println!(
        "run_experiment ok={} coordinator={} run_id={} message={}",
        response.ok, response.coordinator_id, response.run_id, response.message
    );
    if response.ok {
        Ok(())
    } else {
        Err(response.message)
    }
}

pub async fn run_proxy(experiment: ExperimentSpec, options: ProxyOptions) -> Result<(), String> {
    experiment.validate()?;
    println!(
        "proxying experiment {} to node rpc_addr={}",
        experiment_summary(&experiment),
        options.rpc_addr
    );

    let client = connect_node_addr(&options.rpc_addr).await?;
    let response = client
        .run_experiment(context::current(), RunExperimentRequest { experiment })
        .await
        .map_err(|err| format!("run_experiment RPC failed for {}: {err}", options.rpc_addr))?;

    println!(
        "run_experiment ok={} coordinator={} run_id={} message={}",
        response.ok, response.coordinator_id, response.run_id, response.message
    );
    if response.ok {
        Ok(())
    } else {
        Err(response.message)
    }
}

pub async fn run_blob_get(
    instances: InstancesConfig,
    options: BlobGetOptions,
) -> Result<(), String> {
    instances.validate()?;
    let coordinator_id =
        resolve_coordinator_id_without_experiment(&instances, options.coordinator_id)?;
    let coordinator = instances
        .find_instance(&coordinator_id)
        .ok_or_else(|| format!("unknown coordinator instance id: {coordinator_id}"))?;
    if instances.find_instance(&options.peer_instance_id).is_none() {
        return Err(format!(
            "unknown peer instance id: {}",
            options.peer_instance_id
        ));
    }

    println!(
        "running direct blob get run_id={} coordinator={} peer={} backend={} count={} object_size_bytes={}",
        options.run_id,
        coordinator.id,
        options.peer_instance_id,
        options.blob_store_backend,
        options.count,
        options.object_size_bytes,
    );

    let client = connect_node(coordinator).await?;
    let response = client
        .run_blob_get(
            context::current(),
            RunBlobGetRequest {
                run_id: options.run_id,
                peer_instance_id: options.peer_instance_id,
                count: options.count,
                object_size_bytes: options.object_size_bytes,
                blob_store_backend: options.blob_store_backend,
                barrier_timeout_ms: 30_000,
                force_reset: true,
                cleanup: options.cleanup,
            },
        )
        .await
        .map_err(|err| format!("run_blob_get RPC failed for {}: {err}", coordinator.id))?;

    println!(
        "run_blob_get ok={} coordinator={} peer={} run_id={} prepared={} materialized={} total_bytes={} peer_put_ms={:.3} local_get_ms={:.3} message={}",
        response.ok,
        response.coordinator_id,
        response.peer_instance_id,
        response.run_id,
        response.prepared_count,
        response.materialized_count,
        response.total_bytes,
        response.peer_put_elapsed_ms,
        response.local_get_elapsed_ms,
        response.message,
    );
    if response.ok {
        Ok(())
    } else {
        Err(response.message)
    }
}

fn resolve_node_instance_id(
    instances: &InstancesConfig,
    explicit: Option<String>,
) -> Result<String, String> {
    if let Some(instance_id) = explicit.filter(|value| !value.trim().is_empty()) {
        return Ok(instance_id);
    }
    if let Ok(instance_id) = std::env::var("LC_BENCH_INSTANCE_ID") {
        if !instance_id.trim().is_empty() {
            return Ok(instance_id);
        }
    }
    if instances.instances.len() == 1 {
        return Ok(instances.instances[0].id.clone());
    }
    Err(
        "node requires --instance-id or LC_BENCH_INSTANCE_ID when the instance list has more than one entry"
            .to_string(),
    )
}

fn resolve_coordinator_id(
    instances: &InstancesConfig,
    experiment: &ExperimentSpec,
    explicit: Option<String>,
) -> Result<String, String> {
    if let Some(coordinator_id) = explicit.filter(|value| !value.trim().is_empty()) {
        return Ok(coordinator_id);
    }
    if let Ok(coordinator_id) = std::env::var("LC_BENCH_COORDINATOR_ID") {
        if !coordinator_id.trim().is_empty() {
            return Ok(coordinator_id);
        }
    }
    if let Some(participant) = experiment.participants.first() {
        return Ok(participant.instance_id.clone());
    }
    instances
        .instances
        .first()
        .map(|instance| instance.id.clone())
        .ok_or_else(|| "instances list is empty".to_string())
}

fn resolve_coordinator_id_without_experiment(
    instances: &InstancesConfig,
    explicit: Option<String>,
) -> Result<String, String> {
    if let Some(coordinator_id) = explicit.filter(|value| !value.trim().is_empty()) {
        return Ok(coordinator_id);
    }
    if let Ok(coordinator_id) = std::env::var("LC_BENCH_COORDINATOR_ID") {
        if !coordinator_id.trim().is_empty() {
            return Ok(coordinator_id);
        }
    }
    instances
        .instances
        .first()
        .map(|instance| instance.id.clone())
        .ok_or_else(|| "instances list is empty".to_string())
}
