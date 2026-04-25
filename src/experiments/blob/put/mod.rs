mod common;
mod local_file;
mod p2p;
mod s3;

use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::{ExperimentSpec, InstanceConfig, InstancesConfig};
use crate::driver::sweep::{SweepDecision, ThroughputSweepPolicy};
use crate::rpc::server::state::NodeRuntimeState;

use common::{PutCleanupResource, PutDatapointOutcome, PutStore};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutWorkload {
    LocalFile,
    S3,
    P2P,
}

impl PutWorkload {
    fn from_workload(workload: &str) -> Result<Self, String> {
        match workload {
            "blob.put.local_file" => Ok(Self::LocalFile),
            "blob.put.s3" => Ok(Self::S3),
            "blob.put.p2p" => Ok(Self::P2P),
            other => Err(format!(
                "unsupported blob put workload {other}; use blob.put.local_file, blob.put.s3, or blob.put.p2p"
            )),
        }
    }

    fn backend_name(self) -> &'static str {
        match self {
            Self::LocalFile => "local-file",
            Self::S3 => "s3",
            Self::P2P => "p2p",
        }
    }
}

pub(super) async fn run(
    instance: &InstanceConfig,
    _runtime: &Arc<Mutex<NodeRuntimeState>>,
    _instances: &InstancesConfig,
    experiment: ExperimentSpec,
) -> Result<String, String> {
    let workload = PutWorkload::from_workload(&experiment.run.workload)?;
    let policy = sweep_policy(&experiment)?;
    let mut target_ops_per_s = policy.start_ops_per_s;
    let mut datapoints = Vec::new();
    let stop_reason;

    loop {
        let outcome = run_datapoint(instance, &experiment, workload, target_ops_per_s).await?;
        let decision = policy.decide_after(datapoints.len() + 1, &outcome.paced);
        datapoints.push(outcome.report);
        match decision {
            SweepDecision::Continue { next_ops_per_s } => {
                target_ops_per_s = next_ops_per_s;
            }
            SweepDecision::Stop { reason } => {
                stop_reason = reason;
                break;
            }
        }
    }

    let report = common::PutSweepReport::new(
        instance,
        &experiment,
        workload.backend_name(),
        policy,
        stop_reason,
        datapoints,
    );
    serde_json::to_string(&report).map_err(|err| format!("failed to serialize put report: {err}"))
}

async fn run_datapoint(
    instance: &InstanceConfig,
    experiment: &ExperimentSpec,
    workload: PutWorkload,
    target_ops_per_s: f64,
) -> Result<PutDatapointOutcome, String> {
    let resource_id = common::unique_resource_id(&experiment, instance);
    let resource_dir = instance
        .work_dir
        .join("runs")
        .join(common::sanitize_path_part(&experiment.run.run_id))
        .join(&resource_id);
    tokio::fs::create_dir_all(&resource_dir)
        .await
        .map_err(|err| {
            format!(
                "failed to create blob put resource dir {}: {err}",
                resource_dir.display()
            )
        })?;

    let store =
        match create_store(instance, &experiment, workload, &resource_dir, &resource_id).await {
            Ok(store) => store,
            Err(err) => {
                let _ = common::cleanup_resources(vec![PutCleanupResource::LocalDir(resource_dir)])
                    .await;
                return Err(err);
            }
        };
    let store_details = store.details.clone();
    let store_handle = store.handle.clone();
    let result = common::run_put_datapoint(
        instance,
        experiment,
        workload.backend_name(),
        &resource_id,
        &resource_dir,
        store_handle,
        store_details,
        target_ops_per_s,
    )
    .await;

    let close_result = store
        .handle
        .close()
        .await
        .map_err(|err| format!("failed to close blob store: {err}"));
    let cleanup_result = common::cleanup_resources(store.cleanup).await;

    match result {
        Ok(report) => {
            if let Err(err) = close_result {
                return Err(common::with_followup_errors(err, Ok(()), cleanup_result));
            }
            cleanup_result?;
            Ok(report)
        }
        Err(err) => Err(common::with_followup_errors(
            format!("blob put execution failed: {err}"),
            close_result,
            cleanup_result,
        )),
    }
}

fn sweep_policy(experiment: &ExperimentSpec) -> Result<ThroughputSweepPolicy, String> {
    let mut policy = ThroughputSweepPolicy::paper_default(experiment.benchmark.offered_rate_per_s);
    let sweep = &experiment.throughput_sweep;
    if let Some(value) = sweep.start_ops_per_s {
        policy.start_ops_per_s = value;
    }
    if let Some(value) = sweep.step_multiplier {
        policy.step_multiplier = value;
    }
    if let Some(value) = sweep.max_ops_per_s {
        policy.max_ops_per_s = value;
    }
    if let Some(value) = sweep.max_points {
        policy.max_points = value;
    }
    if let Some(value) = sweep.saturation_achieved_ratio {
        policy.saturation_achieved_ratio = value;
    }
    if let Some(value) = sweep.stop_on_failure {
        policy.stop_on_failure = value;
    }
    policy.validate()?;
    Ok(policy)
}

async fn create_store(
    instance: &InstanceConfig,
    experiment: &ExperimentSpec,
    workload: PutWorkload,
    resource_dir: &Path,
    resource_id: &str,
) -> Result<PutStore, String> {
    match workload {
        PutWorkload::LocalFile => {
            local_file::create_store(instance, experiment, resource_dir, resource_id).await
        }
        PutWorkload::S3 => s3::create_store(instance, experiment, resource_dir, resource_id).await,
        PutWorkload::P2P => {
            p2p::create_store(instance, experiment, resource_dir, resource_id).await
        }
    }
}
