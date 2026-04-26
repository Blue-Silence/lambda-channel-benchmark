use std::path::PathBuf;

use crate::config::ConfigPaths;
use crate::roles::{BlobGetOptions, HealthOptions, NodeOptions, ProxyOptions, TriggerOptions};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchCommand {
    Metadata,
    Blob,
    Sender,
    Receiver,
}

#[derive(Clone, Debug)]
pub enum InvocationCommand {
    Workload(BenchCommand),
    Node(NodeOptions),
    Trigger(TriggerOptions),
    Proxy(ProxyOptions),
    Health(HealthOptions),
    BlobGet(BlobGetOptions),
    Help,
}

#[derive(Clone, Debug)]
pub struct BenchConfig {
    pub backend: String,
    pub operations: u64,
    pub concurrency: usize,
    pub object_size_bytes: u64,
    pub warmup_operations: u64,
    pub offered_rate_per_s: Option<f64>,
    pub output: Option<PathBuf>,
    pub seed: u64,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            backend: "inmemory".to_string(),
            operations: 10_000,
            concurrency: 1,
            object_size_bytes: 1 << 20,
            warmup_operations: 1_000,
            offered_rate_per_s: None,
            output: None,
            seed: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Invocation {
    pub command: InvocationCommand,
    pub bench_config: BenchConfig,
    pub config_paths: ConfigPaths,
}

impl Invocation {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut args = args.into_iter();
        let Some(command_arg) = args.next() else {
            return Ok(Self {
                command: InvocationCommand::Help,
                bench_config: BenchConfig::default(),
                config_paths: ConfigPaths::default(),
            });
        };

        let mut config = BenchConfig::default();
        let mut config_paths = ConfigPaths::default();

        if matches!(command_arg.as_str(), "-h" | "--help" | "help") {
            return Ok(Self {
                command: InvocationCommand::Help,
                bench_config: config,
                config_paths,
            });
        }

        if command_arg == "node" || command_arg == "agent" {
            let mut instance_id = None;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--instance-id" => {
                        instance_id = Some(required_value(&mut args, "--instance-id")?)
                    }
                    "--experiment" => {
                        return Err(
                            "node does not read --experiment; submit experiments through proxy or trigger"
                                .to_string(),
                        )
                    }
                    "--instances" => {
                        config_paths.instances =
                            PathBuf::from(required_value(&mut args, "--instances")?)
                    }
                    "-h" | "--help" => {
                        return Ok(Self {
                            command: InvocationCommand::Help,
                            bench_config: config,
                            config_paths,
                        });
                    }
                    other => return Err(format!("unknown node option: {other}")),
                }
            }
            return Ok(Self {
                command: InvocationCommand::Node(NodeOptions { instance_id }),
                bench_config: config,
                config_paths,
            });
        }

        if command_arg == "trigger" || command_arg == "client" {
            let mut coordinator_id = None;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--coordinator" | "--coordinator-id" => {
                        coordinator_id = Some(required_value(&mut args, "--coordinator")?)
                    }
                    "--experiment" => {
                        config_paths.experiment =
                            PathBuf::from(required_value(&mut args, "--experiment")?)
                    }
                    "--instances" => {
                        config_paths.instances =
                            PathBuf::from(required_value(&mut args, "--instances")?)
                    }
                    "-h" | "--help" => {
                        return Ok(Self {
                            command: InvocationCommand::Help,
                            bench_config: config,
                            config_paths,
                        });
                    }
                    other => return Err(format!("unknown trigger option: {other}")),
                }
            }
            return Ok(Self {
                command: InvocationCommand::Trigger(TriggerOptions { coordinator_id }),
                bench_config: config,
                config_paths,
            });
        }

        if command_arg == "proxy" {
            let mut rpc_addr = None;
            let mut csv_output = None;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--url" | "--rpc-addr" => {
                        rpc_addr = Some(required_value(&mut args, "--url")?)
                    }
                    "--experiment" => {
                        config_paths.experiment =
                            PathBuf::from(required_value(&mut args, "--experiment")?)
                    }
                    "--instances" => {
                        return Err(
                            "proxy does not read --instances; the target node uses its startup instance list"
                                .to_string(),
                        )
                    }
                    "--csv" | "--csv-output" => {
                        csv_output = Some(PathBuf::from(required_value(&mut args, "--csv")?))
                    }
                    "-h" | "--help" => {
                        return Ok(Self {
                            command: InvocationCommand::Help,
                            bench_config: config,
                            config_paths,
                        });
                    }
                    other => return Err(format!("unknown proxy option: {other}")),
                }
            }
            let rpc_addr = rpc_addr.ok_or_else(|| "proxy requires --url <rpc-addr>".to_string())?;
            return Ok(Self {
                command: InvocationCommand::Proxy(ProxyOptions {
                    rpc_addr,
                    csv_output,
                }),
                bench_config: config,
                config_paths,
            });
        }

        if command_arg == "health" {
            let mut rpc_addr = None;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--url" | "--rpc-addr" => rpc_addr = Some(required_value(&mut args, "--url")?),
                    "-h" | "--help" => {
                        return Ok(Self {
                            command: InvocationCommand::Help,
                            bench_config: config,
                            config_paths,
                        });
                    }
                    other => return Err(format!("unknown health option: {other}")),
                }
            }
            let rpc_addr =
                rpc_addr.ok_or_else(|| "health requires --url <rpc-addr>".to_string())?;
            return Ok(Self {
                command: InvocationCommand::Health(HealthOptions { rpc_addr }),
                bench_config: config,
                config_paths,
            });
        }

        if command_arg == "blob-get" {
            let mut coordinator_id = None;
            let mut peer_instance_id = None;
            let mut run_id = "blob-get-direct".to_string();
            let mut count = 10_usize;
            let mut object_size_bytes = 64_u64 * 1024;
            let mut blob_store_backend = "local-file".to_string();
            let mut cleanup = true;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--coordinator" | "--coordinator-id" => {
                        coordinator_id = Some(required_value(&mut args, "--coordinator")?)
                    }
                    "--peer" | "--peer-instance-id" => {
                        peer_instance_id = Some(required_value(&mut args, "--peer")?)
                    }
                    "--run-id" => run_id = required_value(&mut args, "--run-id")?,
                    "--count" => {
                        count = parse_usize(&required_value(&mut args, "--count")?, "--count")?
                    }
                    "--object-size" => {
                        object_size_bytes = parse_size(
                            &required_value(&mut args, "--object-size")?,
                            "--object-size",
                        )?
                    }
                    "--backend" => blob_store_backend = required_value(&mut args, "--backend")?,
                    "--no-cleanup" => cleanup = false,
                    "--experiment" => {
                        return Err(
                            "blob-get does not read --experiment; use --run-id/--count/--backend directly"
                                .to_string(),
                        )
                    }
                    "--instances" => {
                        config_paths.instances =
                            PathBuf::from(required_value(&mut args, "--instances")?)
                    }
                    "-h" | "--help" => {
                        return Ok(Self {
                            command: InvocationCommand::Help,
                            bench_config: config,
                            config_paths,
                        });
                    }
                    other => return Err(format!("unknown blob-get option: {other}")),
                }
            }
            let peer_instance_id =
                peer_instance_id.ok_or_else(|| "blob-get requires --peer <id>".to_string())?;
            if count == 0 {
                return Err("--count must be greater than zero".to_string());
            }
            if object_size_bytes == 0 {
                return Err("--object-size must be greater than zero".to_string());
            }
            return Ok(Self {
                command: InvocationCommand::BlobGet(BlobGetOptions {
                    coordinator_id,
                    peer_instance_id,
                    run_id,
                    count,
                    object_size_bytes,
                    blob_store_backend,
                    cleanup,
                }),
                bench_config: config,
                config_paths,
            });
        }

        let command = match command_arg.as_str() {
            "metadata" => BenchCommand::Metadata,
            "blob" => BenchCommand::Blob,
            "sender" => BenchCommand::Sender,
            "receiver" => BenchCommand::Receiver,
            other => return Err(format!("unknown benchmark command: {other}")),
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--backend" => config.backend = required_value(&mut args, "--backend")?,
                "--operations" => {
                    config.operations =
                        parse_u64(&required_value(&mut args, "--operations")?, "--operations")?
                }
                "--concurrency" => {
                    config.concurrency = parse_usize(
                        &required_value(&mut args, "--concurrency")?,
                        "--concurrency",
                    )?
                }
                "--object-size" => {
                    config.object_size_bytes = parse_size(
                        &required_value(&mut args, "--object-size")?,
                        "--object-size",
                    )?
                }
                "--warmup" => {
                    config.warmup_operations =
                        parse_u64(&required_value(&mut args, "--warmup")?, "--warmup")?
                }
                "--rate" => {
                    let value = required_value(&mut args, "--rate")?;
                    config.offered_rate_per_s =
                        Some(value.parse::<f64>().map_err(|_| {
                            format!("--rate must be a positive number, got {value}")
                        })?);
                }
                "--output" => {
                    config.output = Some(PathBuf::from(required_value(&mut args, "--output")?))
                }
                "--seed" => {
                    config.seed = parse_u64(&required_value(&mut args, "--seed")?, "--seed")?
                }
                "-h" | "--help" => {
                    return Ok(Self {
                        command: InvocationCommand::Help,
                        bench_config: config,
                        config_paths,
                    });
                }
                other => return Err(format!("unknown option: {other}")),
            }
        }

        if config.concurrency == 0 {
            return Err("--concurrency must be greater than zero".to_string());
        }
        if config.operations == 0 {
            return Err("--operations must be greater than zero".to_string());
        }
        if config.offered_rate_per_s.is_some_and(|rate| rate <= 0.0) {
            return Err("--rate must be greater than zero".to_string());
        }

        Ok(Self {
            command: InvocationCommand::Workload(command),
            bench_config: config,
            config_paths,
        })
    }
}

fn required_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("expected value after {flag}"))
}

fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be an unsigned integer, got {value}"))
}

fn parse_usize(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be an unsigned integer, got {value}"))
}

fn parse_size(value: &str, flag: &str) -> Result<u64, String> {
    let trimmed = value.trim();
    let lower = trimmed.to_ascii_lowercase();
    let (digits, multiplier) = if let Some(prefix) = lower.strip_suffix("kib") {
        (prefix, 1_u64 << 10)
    } else if let Some(prefix) = lower.strip_suffix("mib") {
        (prefix, 1_u64 << 20)
    } else if let Some(prefix) = lower.strip_suffix("gib") {
        (prefix, 1_u64 << 30)
    } else if let Some(prefix) = lower.strip_suffix("kb") {
        (prefix, 1_000)
    } else if let Some(prefix) = lower.strip_suffix("mb") {
        (prefix, 1_000_000)
    } else if let Some(prefix) = lower.strip_suffix("gb") {
        (prefix, 1_000_000_000)
    } else {
        (trimmed, 1)
    };

    let base = digits
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a byte count or size like 64MiB, got {value}"))?;
    base.checked_mul(multiplier)
        .ok_or_else(|| format!("{flag} is too large: {value}"))
}

pub fn usage() -> &'static str {
    "Usage:
  lc-bench <metadata|blob|sender|receiver> [options]
  lc-bench node [--instance-id <id>] [--instances <path>]
  lc-bench trigger [--coordinator <id>] [--experiment <path>] [--instances <path>]
  lc-bench proxy --url <rpc-addr> [--experiment <path>] [--csv <path>]
  lc-bench health --url <rpc-addr>
  lc-bench blob-get --coordinator <id> --peer <id> [--backend <name>] [--count <n>] [--object-size <size>]

Options:
  --backend <name>        Backend under test, default: inmemory
  --operations <n>        Measured operation count, default: 10000
  --concurrency <n>       Concurrent workers or in-flight operations, default: 1
  --object-size <size>    Payload size for blob/channel workloads, default: 1MiB
  --warmup <n>            Warmup operation count, default: 1000
  --rate <ops/s>          Optional offered-load target
  --output <path>         Write JSON report to a file instead of stdout
  --csv <path>            Append proxy experiment datapoints to a CSV file
  --seed <n>              Deterministic seed for generated workloads
  -h, --help              Show this help

Coordination config defaults:
  --experiment config/local-experiment.toml  (proxy/trigger)
  --instances   config/instances/local-two.toml  (node/trigger/blob-get)

Coordination runtime:
  node starts the symmetric tarpc control-plane daemon and reads only the instance list.
  trigger is the legacy id-based submitter for one experiment.
  proxy connects directly to one node URL and asks it to orchestrate one experiment.
  node infers its instance id from hostname when --instance-id is omitted.
  --instance-id and LC_BENCH_INSTANCE_ID are explicit node identity overrides.
  trigger may read LC_BENCH_COORDINATOR_ID when --coordinator is omitted.
  proxy reads only the experiment config; peer inventory comes from the target node.
  blob-get asks the selected node to initialize expr state on itself and
  its peer, make the peer put blobs, then materialize returned refs locally.

Examples:
  cargo run -- metadata --backend inmemory --operations 100000 --concurrency 8
  cargo run -- blob --backend local-file --object-size 64MiB --operations 1000
  cargo run -- node
  LC_BENCH_INSTANCE_ID=local-a cargo run -- node
  cargo run -- proxy --url 127.0.0.1:19000 --experiment config/experiments/blob/get-materialize.toml
  cargo run -- trigger --coordinator local-a
  cargo run -- blob-get --coordinator local-a --peer local-b --backend local-file --count 1000 --object-size 64KiB
"
}
