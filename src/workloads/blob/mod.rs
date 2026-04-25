mod scenarios;

use crate::cli::BenchConfig;
use crate::workloads::{stub_run, WorkloadRun};

pub fn run(_config: &BenchConfig) -> WorkloadRun {
    let scenario_names = scenarios::SCENARIOS
        .iter()
        .map(|scenario| {
            format!(
                "{} [{}]: {}",
                scenario.name, scenario.config_path, scenario.purpose
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    stub_run(
        "blob",
        format!("blob workload group is registered; scenarios: {scenario_names}"),
    )
}
