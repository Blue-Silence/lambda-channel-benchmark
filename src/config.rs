use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct ConfigPaths {
    pub experiment: PathBuf,
    pub instances: PathBuf,
}

impl Default for ConfigPaths {
    fn default() -> Self {
        Self {
            experiment: PathBuf::from("config/local-experiment.toml"),
            instances: PathBuf::from("config/instances/local-two.toml"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExperimentSpec {
    pub run: RunConfig,
    pub coordination: CoordinationConfig,
    pub benchmark: BenchmarkConfig,
    #[serde(default)]
    pub throughput_sweep: ThroughputSweepConfig,
    pub lambda_channel: LambdaChannelConfig,
    pub p2p: P2pConfig,
    #[serde(default)]
    pub participants: Vec<ParticipantConfig>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunConfig {
    pub run_id: String,
    pub workload: String,
    #[serde(default)]
    pub description: String,
    pub output_dir: PathBuf,
    #[serde(default)]
    pub seed: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CoordinationConfig {
    pub rpc_timeout_ms: u64,
    pub barrier_timeout_ms: u64,
    pub cleanup_timeout_ms: u64,
    #[serde(default)]
    pub force_reset_on_start: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BenchmarkConfig {
    #[serde(default)]
    pub backend: String,
    pub operations: u64,
    pub concurrency: usize,
    pub object_size_bytes: u64,
    pub warmup_operations: u64,
    #[serde(default)]
    pub duration_seconds: Option<f64>,
    #[serde(default)]
    pub offered_rate_per_s: Option<f64>,
    #[serde(default = "default_repetitions")]
    pub repetitions: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ThroughputSweepConfig {
    #[serde(default)]
    pub start_ops_per_s: Option<f64>,
    #[serde(default)]
    pub step_multiplier: Option<f64>,
    #[serde(default)]
    pub max_ops_per_s: Option<f64>,
    #[serde(default)]
    pub max_points: Option<usize>,
    #[serde(default)]
    pub saturation_achieved_ratio: Option<f64>,
    #[serde(default)]
    pub stop_on_failure: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LambdaChannelConfig {
    pub metadata_backend: String,
    pub channel_id_prefix: String,
    pub consume_mode: String,
    #[serde(default)]
    pub native_worker_threads: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct P2pConfig {
    pub tracker_backend: String,
    pub cache_root: PathBuf,
    pub chunk_server_bind_host: String,
    pub chunk_server_port_base: u16,
    #[serde(default = "default_p2p_chunk_server_runtime_worker_threads")]
    pub chunk_server_runtime_worker_threads: usize,
    #[serde(default = "default_p2p_enable_accel")]
    pub enable_accel: bool,
    #[serde(default)]
    pub accel_probability: Option<f64>,
    #[serde(default)]
    pub persist_backend: Option<String>,
    #[serde(default = "default_p2p_non_abortable_task_workers")]
    pub non_abortable_task_workers: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InstancesConfig {
    pub registry: RegistryConfig,
    pub instances: Vec<InstanceConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ParticipantConfig {
    pub instance_id: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegistryConfig {
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InstanceConfig {
    pub id: String,
    pub rpc_addr: String,
    pub p2p_advertise_endpoint: String,
    pub work_dir: PathBuf,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl InstancesConfig {
    pub fn find_instance(&self, instance_id: &str) -> Option<&InstanceConfig> {
        self.instances
            .iter()
            .find(|instance| instance.id == instance_id)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.instances.is_empty() {
            return Err("instances list must contain at least one instance".to_string());
        }
        let mut seen = std::collections::BTreeSet::new();
        for instance in &self.instances {
            if instance.id.trim().is_empty() {
                return Err("instance id must not be empty".to_string());
            }
            if !seen.insert(instance.id.as_str()) {
                return Err(format!("duplicate instance id: {}", instance.id));
            }
            if instance.rpc_addr.trim().is_empty() {
                return Err(format!("instance {} has empty rpc_addr", instance.id));
            }
            if instance.p2p_advertise_endpoint.trim().is_empty() {
                return Err(format!(
                    "instance {} has empty p2p_advertise_endpoint",
                    instance.id
                ));
            }
        }
        Ok(())
    }
}

impl ExperimentSpec {
    pub fn validate(&self) -> Result<(), String> {
        if self.run.run_id.trim().is_empty() {
            return Err("experiment run.run_id must not be empty".to_string());
        }
        if self.run.workload.trim().is_empty() {
            return Err("experiment run.workload must not be empty".to_string());
        }
        if self.benchmark.operations == 0 {
            return Err("benchmark.operations must be greater than zero".to_string());
        }
        if self.benchmark.concurrency == 0 {
            return Err("benchmark.concurrency must be greater than zero".to_string());
        }
        if self.benchmark.repetitions == 0 {
            return Err("benchmark.repetitions must be greater than zero".to_string());
        }
        if self
            .benchmark
            .duration_seconds
            .is_some_and(|duration| !duration.is_finite() || duration <= 0.0)
        {
            return Err("benchmark.duration_seconds must be a finite positive number".to_string());
        }
        if self
            .benchmark
            .offered_rate_per_s
            .is_some_and(|rate| rate <= 0.0)
        {
            return Err("benchmark.offered_rate_per_s must be greater than zero".to_string());
        }
        self.throughput_sweep.validate()?;
        if self.coordination.rpc_timeout_ms == 0 {
            return Err("coordination.rpc_timeout_ms must be greater than zero".to_string());
        }
        if self.coordination.barrier_timeout_ms == 0 {
            return Err("coordination.barrier_timeout_ms must be greater than zero".to_string());
        }
        if self.coordination.cleanup_timeout_ms == 0 {
            return Err("coordination.cleanup_timeout_ms must be greater than zero".to_string());
        }
        if self.lambda_channel.metadata_backend.trim().is_empty() {
            return Err("lambda_channel.metadata_backend must not be empty".to_string());
        }
        if self.lambda_channel.channel_id_prefix.trim().is_empty() {
            return Err("lambda_channel.channel_id_prefix must not be empty".to_string());
        }
        if self.lambda_channel.consume_mode.trim().is_empty() {
            return Err("lambda_channel.consume_mode must not be empty".to_string());
        }
        if self.p2p.tracker_backend.trim().is_empty() {
            return Err("p2p.tracker_backend must not be empty".to_string());
        }
        if self.p2p.chunk_server_bind_host.trim().is_empty() {
            return Err("p2p.chunk_server_bind_host must not be empty".to_string());
        }
        if self.p2p.chunk_server_runtime_worker_threads == 0 {
            return Err(
                "p2p.chunk_server_runtime_worker_threads must be greater than zero".to_string(),
            );
        }
        if self.p2p.non_abortable_task_workers == 0 {
            return Err("p2p.non_abortable_task_workers must be greater than zero".to_string());
        }
        if self
            .p2p
            .accel_probability
            .is_some_and(|probability| !(0.0..=1.0).contains(&probability))
        {
            return Err("p2p.accel_probability must be between 0 and 1".to_string());
        }
        for participant in &self.participants {
            if participant.instance_id.trim().is_empty() {
                return Err("participant instance_id must not be empty".to_string());
            }
        }
        Ok(())
    }

    pub fn validate_with_instances(&self, instances: &InstancesConfig) -> Result<(), String> {
        self.validate()?;
        instances.validate()?;
        for participant in &self.participants {
            if instances.find_instance(&participant.instance_id).is_none() {
                return Err(format!(
                    "participant references unknown instance id: {}",
                    participant.instance_id
                ));
            }
        }
        Ok(())
    }
}

impl ThroughputSweepConfig {
    fn validate(&self) -> Result<(), String> {
        if self
            .start_ops_per_s
            .is_some_and(|value| !value.is_finite() || value <= 0.0)
        {
            return Err(
                "throughput_sweep.start_ops_per_s must be a finite positive number".to_string(),
            );
        }
        if self
            .step_multiplier
            .is_some_and(|value| !value.is_finite() || value <= 1.0)
        {
            return Err(
                "throughput_sweep.step_multiplier must be a finite number greater than 1"
                    .to_string(),
            );
        }
        if self
            .max_ops_per_s
            .is_some_and(|value| !value.is_finite() || value <= 0.0)
        {
            return Err(
                "throughput_sweep.max_ops_per_s must be a finite positive number".to_string(),
            );
        }
        if self.max_points.is_some_and(|value| value == 0) {
            return Err("throughput_sweep.max_points must be greater than zero".to_string());
        }
        if self.saturation_achieved_ratio.is_some_and(|value| {
            !value.is_finite() || !(0.0..=1.0).contains(&value) || value == 0.0
        }) {
            return Err(
                "throughput_sweep.saturation_achieved_ratio must be in the range (0, 1]"
                    .to_string(),
            );
        }
        Ok(())
    }
}

impl BenchmarkConfig {
    pub fn operations_for_target(&self, target_ops_per_s: f64) -> Result<u64, String> {
        let Some(duration_seconds) = self.duration_seconds else {
            return Ok(self.operations);
        };
        if !target_ops_per_s.is_finite() || target_ops_per_s <= 0.0 {
            return Err("target_ops_per_s must be a finite positive number".to_string());
        }
        let operations = (target_ops_per_s * duration_seconds).ceil();
        if !operations.is_finite() || operations <= 0.0 {
            return Err("duration-based operations must be a finite positive number".to_string());
        }
        if operations > u64::MAX as f64 {
            return Err(format!(
                "duration_seconds={} at target_ops_per_s={} produces too many operations",
                duration_seconds, target_ops_per_s
            ));
        }
        Ok(operations as u64)
    }
}

pub fn load_experiment(path: &Path) -> Result<ExperimentSpec, String> {
    load_toml(path)
}

pub fn load_instances(path: &Path) -> Result<InstancesConfig, String> {
    load_toml(path)
}

pub fn experiment_summary(experiment: &ExperimentSpec) -> String {
    let offered_rate = experiment
        .benchmark
        .offered_rate_per_s
        .map(|rate| rate.to_string())
        .unwrap_or_else(|| "none".to_string());
    let native_threads = experiment
        .lambda_channel
        .native_worker_threads
        .map(|threads| threads.to_string())
        .unwrap_or_else(|| "default".to_string());
    let accel_probability = experiment
        .p2p
        .accel_probability
        .map(|probability| probability.to_string())
        .unwrap_or_else(|| "none".to_string());
    let persist_backend = experiment.p2p.persist_backend.as_deref().unwrap_or("none");
    let benchmark_backend = if experiment.benchmark.backend.trim().is_empty() {
        "none"
    } else {
        experiment.benchmark.backend.as_str()
    };
    let duration_seconds = experiment
        .benchmark
        .duration_seconds
        .map(|duration| duration.to_string())
        .unwrap_or_else(|| "none".to_string());
    format!(
        concat!(
            "run_id={}, workload={}, description={}, output_dir={}, seed={}, ",
            "backend={}, operations={}, duration_seconds={}, concurrency={}, object_size_bytes={}, warmup_operations={}, ",
            "offered_rate_per_s={}, repetitions={}, ",
            "force_reset_on_start={}, participants={}, ",
            "metadata_backend={}, channel_id_prefix={}, consume_mode={}, native_worker_threads={}, ",
            "p2p_tracker_backend={}, p2p_cache_root={}, p2p_bind_host={}, p2p_port_base={}, p2p_chunk_server_runtime_worker_threads={}, ",
            "p2p_enable_accel={}, p2p_accel_probability={}, p2p_persist_backend={}, p2p_non_abortable_task_workers={}, env_vars={}"
        ),
        experiment.run.run_id,
        experiment.run.workload,
        experiment.run.description,
        experiment.run.output_dir.display(),
        experiment.run.seed,
        benchmark_backend,
        experiment.benchmark.operations,
        duration_seconds,
        experiment.benchmark.concurrency,
        experiment.benchmark.object_size_bytes,
        experiment.benchmark.warmup_operations,
        offered_rate,
        experiment.benchmark.repetitions,
        experiment.coordination.force_reset_on_start,
        experiment.participants.len(),
        experiment.lambda_channel.metadata_backend,
        experiment.lambda_channel.channel_id_prefix,
        experiment.lambda_channel.consume_mode,
        native_threads,
        experiment.p2p.tracker_backend,
        experiment.p2p.cache_root.display(),
        experiment.p2p.chunk_server_bind_host,
        experiment.p2p.chunk_server_port_base,
        experiment.p2p.chunk_server_runtime_worker_threads,
        experiment.p2p.enable_accel,
        accel_probability,
        persist_backend,
        experiment.p2p.non_abortable_task_workers,
        experiment.env.len(),
    )
}

pub fn instances_summary(instances: &InstancesConfig) -> String {
    format!(
        "registry={}, registry_description={}, instances={}",
        instances.registry.name,
        instances.registry.description,
        instances.instances.len(),
    )
}

fn load_toml<T>(path: &Path) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
    let content = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    toml::from_str(&content).map_err(|err| format!("failed to parse {}: {err}", path.display()))
}

fn default_repetitions() -> u64 {
    1
}

fn default_p2p_chunk_server_runtime_worker_threads() -> usize {
    4
}

fn default_p2p_enable_accel() -> bool {
    true
}

fn default_p2p_non_abortable_task_workers() -> usize {
    16
}
