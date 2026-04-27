mod blob_store_factory;
mod cli;
mod config;
mod driver;
mod experiments;
mod output;
mod payload_file;
mod roles;
mod rpc;
mod workloads;

use std::fs;
use std::process::ExitCode;

use cli::{Invocation, InvocationCommand};
use config::{load_experiment, load_instances};
use output::BenchmarkReport;

#[tokio::main(worker_threads = 8)]
async fn main() -> ExitCode {
    let invocation = match Invocation::parse(std::env::args().skip(1)) {
        Ok(invocation) => invocation,
        Err(message) => {
            eprintln!("{message}");
            eprintln!();
            eprintln!("{}", cli::usage());
            return ExitCode::from(2);
        }
    };

    match invocation.command {
        InvocationCommand::Help => {
            println!("{}", cli::usage());
            ExitCode::SUCCESS
        }
        InvocationCommand::Workload(command) => {
            let report = workloads::run(command, &invocation.bench_config);
            let json = BenchmarkReport::from_run(report, &invocation.bench_config).to_json_pretty();

            if let Some(path) = invocation.bench_config.output.as_ref() {
                if let Err(err) = fs::write(path, json.as_bytes()) {
                    eprintln!("failed to write output {}: {err}", path.display());
                    return ExitCode::from(1);
                }
                println!("wrote benchmark report to {}", path.display());
            } else {
                println!("{json}");
            }

            ExitCode::SUCCESS
        }
        InvocationCommand::Node(options) => {
            let instances = match load_instances(&invocation.config_paths.instances) {
                Ok(instances) => instances,
                Err(message) => {
                    eprintln!("{message}");
                    return ExitCode::from(1);
                }
            };
            match roles::run_node(instances, options).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {
                    eprintln!("{message}");
                    ExitCode::from(1)
                }
            }
        }
        InvocationCommand::Trigger(options) => {
            let instances = match load_instances(&invocation.config_paths.instances) {
                Ok(instances) => instances,
                Err(message) => {
                    eprintln!("{message}");
                    return ExitCode::from(1);
                }
            };
            let experiment = match load_experiment(&invocation.config_paths.experiment) {
                Ok(experiment) => experiment,
                Err(message) => {
                    eprintln!("{message}");
                    return ExitCode::from(1);
                }
            };
            match roles::run_trigger(instances, experiment, options).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {
                    eprintln!("{message}");
                    ExitCode::from(1)
                }
            }
        }
        InvocationCommand::Proxy(options) => {
            let experiment = match load_experiment(&invocation.config_paths.experiment) {
                Ok(experiment) => experiment,
                Err(message) => {
                    eprintln!("{message}");
                    return ExitCode::from(1);
                }
            };
            match roles::run_proxy(experiment, options).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {
                    eprintln!("{message}");
                    ExitCode::from(1)
                }
            }
        }
        InvocationCommand::Health(options) => match roles::run_health(options).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(message) => {
                eprintln!("{message}");
                ExitCode::from(1)
            }
        },
        InvocationCommand::BlobGet(options) => {
            let instances = match load_instances(&invocation.config_paths.instances) {
                Ok(instances) => instances,
                Err(message) => {
                    eprintln!("{message}");
                    return ExitCode::from(1);
                }
            };
            match roles::run_blob_get(instances, options).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {
                    eprintln!("{message}");
                    ExitCode::from(1)
                }
            }
        }
    }
}
