use std::path::PathBuf;
use std::time::{Duration, Instant};

use tarpc::context;
use tokio::time::sleep;

use crate::config::{experiment_summary, instances_summary, ExperimentSpec, InstancesConfig};
use crate::output;
use crate::rpc::protocol::{
    AcceptedResponse, ExperimentRunResult, HealthRequest, NodeRpcClient, PollRequestRequest,
    RequestResult, RequestStatus,
};
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
    pub csv_output: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct HealthOptions {
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
    submit_experiment_and_wait(&client, experiment, &coordinator.id, true).await?;
    Ok(())
}

pub async fn run_proxy(experiment: ExperimentSpec, options: ProxyOptions) -> Result<(), String> {
    experiment.validate()?;
    println!(
        "proxying experiment {} to node rpc_addr={}",
        experiment_summary(&experiment),
        options.rpc_addr
    );

    let client = connect_node_addr(&options.rpc_addr).await?;
    let print_result_message = options.csv_output.is_none();
    let result =
        submit_experiment_and_wait(&client, experiment, &options.rpc_addr, print_result_message)
            .await?;
    if let Some(csv_output) = options.csv_output.as_ref() {
        print_experiment_failure_summary(&result.message)?;
        let rows = output::append_experiment_csv(csv_output, &result.message)?;
        println!(
            "appended {} datapoint rows to {}",
            rows,
            csv_output.display()
        );
    }
    Ok(())
}

pub async fn run_health(options: HealthOptions) -> Result<(), String> {
    let client = connect_node_addr(&options.rpc_addr).await?;
    let response = client
        .health(rpc_context(Duration::from_secs(5)), HealthRequest {})
        .await
        .map_err(|err| format!("health RPC failed for {}: {err}", options.rpc_addr))?;

    println!(
        "health ok={} instance_id={} status={} current_run_id={} generation={}",
        response.ok,
        response.instance_id,
        response.node_status,
        response.current_run_id.as_deref().unwrap_or("-"),
        response.generation,
    );

    if response.ok {
        Ok(())
    } else {
        Err(format!("node {} reported unhealthy", options.rpc_addr))
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

async fn submit_experiment_and_wait(
    client: &NodeRpcClient,
    experiment: ExperimentSpec,
    target_label: &str,
    print_result_message: bool,
) -> Result<ExperimentRunResult, String> {
    let rpc_timeout = Duration::from_millis(experiment.coordination.rpc_timeout_ms);
    let accepted = client
        .submit_experiment(
            rpc_context(rpc_timeout),
            RunExperimentRequest { experiment },
        )
        .await
        .map_err(|err| format!("submit_experiment RPC failed for {target_label}: {err}"))?;
    wait_for_submitted_experiment(
        client,
        target_label,
        accepted,
        print_result_message,
        rpc_timeout,
    )
    .await
}

async fn wait_for_submitted_experiment(
    client: &NodeRpcClient,
    target_label: &str,
    accepted: AcceptedResponse,
    print_result_message: bool,
    rpc_timeout: Duration,
) -> Result<ExperimentRunResult, String> {
    if !accepted.ok {
        return Err(format!(
            "target {target_label} rejected submit_experiment: {}",
            accepted.message
        ));
    }
    let req_id = accepted.req_id.ok_or_else(|| {
        format!("target {target_label} accepted submit_experiment without req_id")
    })?;
    println!(
        "submit_experiment accepted target={} run_id={} req_id={} message={}",
        target_label, accepted.run_id, req_id, accepted.message
    );

    let mut last_progress = Instant::now();
    loop {
        let response = client
            .poll_request(
                rpc_context(rpc_timeout),
                PollRequestRequest {
                    run_id: accepted.run_id.clone(),
                    req_id: req_id.clone(),
                    include_result: true,
                },
            )
            .await
            .map_err(|err| {
                format!("poll_request RPC failed for {target_label} req_id={req_id}: {err}")
            })?;
        if !response.ok {
            return Err(format!(
                "target {target_label} rejected poll_request req_id={req_id}: {}",
                response.message
            ));
        }

        match response.status {
            RequestStatus::Running => {
                if last_progress.elapsed() >= Duration::from_secs(5) {
                    println!(
                        "submit_experiment running target={} run_id={} req_id={}",
                        target_label, accepted.run_id, req_id
                    );
                    last_progress = Instant::now();
                }
            }
            RequestStatus::Finished => {
                let Some(result) = response.result else {
                    return Err(format!(
                        "target {target_label} finished req_id={req_id} without result"
                    ));
                };
                let RequestResult::Experiment(result) = result else {
                    return Err(format!(
                        "target {target_label} returned non-experiment result for req_id={req_id}"
                    ));
                };
                if print_result_message {
                    println!(
                        "submit_experiment finished coordinator={} run_id={} req_id={} message={}",
                        result.coordinator_id, result.run_id, req_id, result.message
                    );
                } else {
                    println!(
                        "submit_experiment finished coordinator={} run_id={} req_id={}",
                        result.coordinator_id, result.run_id, req_id
                    );
                }
                return Ok(result);
            }
            RequestStatus::Failed => {
                return Err(format!(
                    "target {target_label} failed submit_experiment req_id={req_id}: {}",
                    response.message
                ));
            }
            RequestStatus::Missing => {
                return Err(format!(
                    "target {target_label} does not know submit_experiment req_id={req_id}"
                ));
            }
        }

        sleep(Duration::from_millis(500)).await;
    }
}

fn rpc_context(timeout: Duration) -> context::Context {
    let mut ctx = context::current();
    ctx.deadline = Instant::now() + timeout;
    ctx
}

fn print_experiment_failure_summary(report_json: &str) -> Result<(), String> {
    const MAX_FAILURE_SAMPLES: usize = 16;

    let report: serde_json::Value = serde_json::from_str(report_json)
        .map_err(|err| format!("experiment result is not valid JSON: {err}"))?;
    let datapoints = report
        .get("datapoints")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "experiment result JSON missing datapoints[]".to_string())?;

    let run_id = string_value(report.get("run_id")).unwrap_or("-");
    let workload = string_value(report.get("workload")).unwrap_or("-");
    let backend = string_value(report.get("backend")).unwrap_or("-");
    let mut printed_header = false;

    for (index, datapoint) in datapoints.iter().enumerate() {
        let paced = datapoint.get("paced").unwrap_or(&serde_json::Value::Null);
        let failed_tasks = paced
            .get("failed_tasks")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let failures = paced
            .get("failures")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let failure_messages = paced
            .get("failure_messages")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        if failed_tasks == 0 && failures.is_empty() && failure_messages.is_empty() {
            continue;
        }

        if !printed_header {
            println!(
                "experiment failure summary run_id={} workload={} backend={}",
                run_id, workload, backend
            );
            printed_header = true;
        }

        let datapoint_index = datapoint
            .get("datapoint_index")
            .and_then(serde_json::Value::as_u64)
            .map(|value| value.to_string())
            .unwrap_or_else(|| index.to_string());
        let resource_id = string_value(datapoint.get("resource_id")).unwrap_or("-");
        let target_ops = paced
            .get("target_ops_per_s")
            .and_then(serde_json::Value::as_f64)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "  datapoint={} resource_id={} target_ops_per_s={} failed_tasks={} failure_samples={}",
            datapoint_index,
            resource_id,
            target_ops,
            failed_tasks,
            failures.len().max(failure_messages.len())
        );

        for failure in failures.iter().take(MAX_FAILURE_SAMPLES) {
            let index = failure
                .get("index")
                .and_then(serde_json::Value::as_u64)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());
            let message = string_value(failure.get("message")).unwrap_or("-");
            println!("    failure index={}: {}", index, one_line(message, 800));
        }
        for message in failure_messages.iter().take(MAX_FAILURE_SAMPLES) {
            if let Some(message) = string_value(Some(message)) {
                println!("    failure: {}", one_line(message, 800));
            }
        }

        let omitted = failures
            .len()
            .saturating_sub(MAX_FAILURE_SAMPLES)
            .max(failure_messages.len().saturating_sub(MAX_FAILURE_SAMPLES));
        if omitted > 0 {
            println!("    ... omitted {} additional failure sample(s)", omitted);
        }
    }

    Ok(())
}

fn string_value(value: Option<&serde_json::Value>) -> Option<&str> {
    value.and_then(serde_json::Value::as_str)
}

fn one_line(message: &str, max_chars: usize) -> String {
    let mut output = String::new();
    for ch in message.chars() {
        let ch = match ch {
            '\n' | '\r' | '\t' => ' ',
            ch => ch,
        };
        if output.chars().count() >= max_chars {
            output.push_str("...");
            break;
        }
        output.push(ch);
    }
    output
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
    if let Some(instance_id) = resolve_node_instance_id_from_local_hostname(instances)? {
        return Ok(instance_id);
    }
    if instances.instances.len() == 1 {
        return Ok(instances.instances[0].id.clone());
    }
    Err(
        "node could not infer instance id from hostname; make the hostname match an instance id, pass --instance-id, or set LC_BENCH_INSTANCE_ID"
            .to_string(),
    )
}

fn resolve_node_instance_id_from_local_hostname(
    instances: &InstancesConfig,
) -> Result<Option<String>, String> {
    let hostname = current_hostname()?;
    resolve_node_instance_id_from_hostname(instances, &hostname)
}

fn resolve_node_instance_id_from_hostname(
    instances: &InstancesConfig,
    hostname: &str,
) -> Result<Option<String>, String> {
    for candidate in hostname_candidates(hostname) {
        let matches = instances
            .instances
            .iter()
            .filter(|instance| instance.id == candidate)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [] => {}
            [instance] => return Ok(Some(instance.id.clone())),
            _ => {
                return Err(format!(
                    "hostname {hostname} ambiguously matches multiple instances named {candidate}"
                ))
            }
        }
    }
    Ok(None)
}

fn hostname_candidates(hostname: &str) -> Vec<String> {
    let hostname = hostname.trim().trim_end_matches('.');
    let mut candidates = Vec::new();
    if !hostname.is_empty() {
        candidates.push(hostname.to_string());
        if let Some((short, _)) = hostname.split_once('.') {
            if !short.is_empty() && short != hostname {
                candidates.push(short.to_string());
            }
        }
    }
    candidates
}

#[cfg(unix)]
fn current_hostname() -> Result<String, String> {
    use std::ffi::CStr;

    let mut buffer = [0_i8; 256];
    let rc = unsafe { libc::gethostname(buffer.as_mut_ptr(), buffer.len()) };
    if rc != 0 {
        return env_hostname().ok_or_else(|| {
            format!(
                "failed to read hostname: {}",
                std::io::Error::last_os_error()
            )
        });
    }
    buffer[buffer.len() - 1] = 0;
    let hostname = unsafe { CStr::from_ptr(buffer.as_ptr()) }
        .to_string_lossy()
        .trim()
        .to_string();
    if hostname.is_empty() {
        return env_hostname().ok_or_else(|| "hostname is empty".to_string());
    }
    Ok(hostname)
}

#[cfg(not(unix))]
fn current_hostname() -> Result<String, String> {
    env_hostname().ok_or_else(|| "failed to read hostname".to_string())
}

fn env_hostname() -> Option<String> {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crate::config::{InstanceConfig, InstancesConfig, RegistryConfig};

    use super::{hostname_candidates, resolve_node_instance_id_from_hostname};

    fn instances(ids: &[&str]) -> InstancesConfig {
        InstancesConfig {
            registry: RegistryConfig {
                name: "test".to_string(),
                description: String::new(),
            },
            instances: ids
                .iter()
                .map(|id| InstanceConfig {
                    id: (*id).to_string(),
                    rpc_addr: format!("{id}:19000"),
                    rpc_listen_addr: None,
                    p2p_advertise_endpoint: format!("http://{id}:18080"),
                    work_dir: PathBuf::from(format!(".bench/{id}")),
                    capabilities: Vec::new(),
                    labels: BTreeMap::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn hostname_candidates_try_full_then_short_name() {
        assert_eq!(
            hostname_candidates("node0.example.cloudlab."),
            vec!["node0.example.cloudlab".to_string(), "node0".to_string()]
        );
        assert_eq!(hostname_candidates("node0"), vec!["node0".to_string()]);
    }

    #[test]
    fn resolves_instance_from_full_hostname() {
        let instances = instances(&["node0.example.cloudlab", "node0"]);
        let resolved =
            resolve_node_instance_id_from_hostname(&instances, "node0.example.cloudlab").unwrap();

        assert_eq!(resolved.as_deref(), Some("node0.example.cloudlab"));
    }

    #[test]
    fn resolves_instance_from_short_hostname_when_full_is_absent() {
        let instances = instances(&["node0", "node1"]);
        let resolved =
            resolve_node_instance_id_from_hostname(&instances, "node0.example.cloudlab").unwrap();

        assert_eq!(resolved.as_deref(), Some("node0"));
    }
}
