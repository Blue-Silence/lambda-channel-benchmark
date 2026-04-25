mod append;
mod common;
mod competitive_claim;
mod prefix_scan;

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::{ExperimentSpec, InstanceConfig, InstancesConfig};
use crate::rpc::server::state::NodeRuntimeState;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MetadataWorkload {
    Append,
    PrefixScan,
    CompetitiveClaimLocal,
}

impl MetadataWorkload {
    pub(super) fn from_workload(workload: &str) -> Result<Self, String> {
        match workload {
            "metadata.append" => Ok(Self::Append),
            "metadata.prefix_scan" => Ok(Self::PrefixScan),
            "metadata.competitive_claim.local" => Ok(Self::CompetitiveClaimLocal),
            other => Err(format!(
                "unsupported metadata workload {other}; use metadata.append, metadata.prefix_scan, or metadata.competitive_claim.local"
            )),
        }
    }

    pub(super) fn backend_name(self) -> &'static str {
        "dynamodb"
    }
}

pub(crate) async fn run(
    instance: &InstanceConfig,
    _runtime: &Arc<Mutex<NodeRuntimeState>>,
    _instances: &InstancesConfig,
    experiment: ExperimentSpec,
) -> Result<String, String> {
    let workload = MetadataWorkload::from_workload(&experiment.run.workload)?;
    common::ensure_dynamodb_backend(&experiment)?;
    let policy = common::sweep_policy(&experiment)?;
    let mut target_ops_per_s = policy.start_ops_per_s;
    let mut datapoints = Vec::new();
    let stop_reason;

    loop {
        let outcome = match workload {
            MetadataWorkload::Append => {
                append::run_datapoint(instance, &experiment, workload, target_ops_per_s).await?
            }
            MetadataWorkload::PrefixScan => {
                prefix_scan::run_datapoint(instance, &experiment, workload, target_ops_per_s)
                    .await?
            }
            MetadataWorkload::CompetitiveClaimLocal => {
                competitive_claim::run_datapoint(instance, &experiment, workload, target_ops_per_s)
                    .await?
            }
        };
        let decision = policy.decide_after(datapoints.len() + 1, &outcome.paced);
        datapoints.push(outcome.report);
        match decision {
            crate::driver::sweep::SweepDecision::Continue { next_ops_per_s } => {
                target_ops_per_s = next_ops_per_s;
            }
            crate::driver::sweep::SweepDecision::Stop { reason } => {
                stop_reason = reason;
                break;
            }
        }
    }

    let report = common::MetadataSweepReport::new(
        instance,
        &experiment,
        workload.backend_name(),
        policy,
        stop_reason,
        datapoints,
    );
    serde_json::to_string(&report)
        .map_err(|err| format!("failed to serialize metadata report: {err}"))
}
