use crate::cli::BenchConfig;
use crate::workloads::{stub_run, WorkloadRun};

pub fn run(_config: &BenchConfig) -> WorkloadRun {
    stub_run(
        "receiver",
        "receiver microbenchmark harness is initialized; workload implementation is next",
    )
}
