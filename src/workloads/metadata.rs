use crate::cli::BenchConfig;
use crate::workloads::{stub_run, WorkloadRun};

pub fn run(_config: &BenchConfig) -> WorkloadRun {
    stub_run(
        "metadata",
        "metadata microbenchmark harness is initialized; workload implementation is next",
    )
}
