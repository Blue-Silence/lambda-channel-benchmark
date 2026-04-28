#!/usr/bin/env python3
"""
Run the CloudLab blob single-getter workflow.

The experiment TOMLs decide the topology through [[participants]]. Formal
9-node experiments use node-0 as the getter/orchestrator and node-1..node-8 as
putters, while smoke TOMLs may name fewer putters. The Rust runner distributes
each datapoint working set evenly across the putters and runs paced gets on the
getter.
"""

from __future__ import annotations

import argparse
import configparser
import os
import shlex
import subprocess
import sys
import time
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ENTRYPOINT_DIR = ROOT / "cloudlab" / "scripts" / "entrypoints"
LIB_DIR = ROOT / "cloudlab" / "scripts" / "lib"
sys.path.insert(0, str(ENTRYPOINT_DIR))
sys.path.insert(0, str(LIB_DIR))

import allocate_profile
import check_experiment_ready
import deploy
import gc_aws_resources
import kill_expr_servers
import package as package_entrypoint
import refresh_nodes
import run_proxy_experiment
import start_expr_servers
import workflow_paths
from nodes import read_nodes
from ssh import connect


EXPERIMENTS_32B = [
    "config/experiments/blob/single-getter/9node/s3-32b.toml",
    "config/experiments/blob/single-getter/9node/p2p-32b.toml",
]
EXPERIMENTS_16M = [
    "config/experiments/blob/single-getter/9node/s3-16m.toml",
    "config/experiments/blob/single-getter/9node/p2p-16m.toml",
]
EXPERIMENTS_128M = [
    "config/experiments/blob/single-getter/9node/s3-128m.toml",
    "config/experiments/blob/single-getter/9node/p2p-128m.toml",
]
EXPERIMENTS_1G = [
    "config/experiments/blob/single-getter/9node/s3-1g.toml",
    "config/experiments/blob/single-getter/9node/p2p-1g.toml",
]
EXPERIMENTS_SMOKE_3NODE_32B = [
    "config/experiments/blob/single-getter/smoke/3node-s3-32b.toml",
    "config/experiments/blob/single-getter/smoke/3node-p2p-32b.toml",
]
EXPERIMENTS_TEMP_3NODE_ALL = [
    "config/experiments/blob/single-getter/3node/s3-32b.toml",
    "config/experiments/blob/single-getter/3node/p2p-32b.toml",
    "config/experiments/blob/single-getter/3node/s3-16m.toml",
    "config/experiments/blob/single-getter/3node/p2p-16m.toml",
    "config/experiments/blob/single-getter/3node/s3-128m.toml",
    "config/experiments/blob/single-getter/3node/p2p-128m.toml",
    "config/experiments/blob/single-getter/3node/s3-1g.toml",
    "config/experiments/blob/single-getter/3node/p2p-1g.toml",
]
EXPERIMENT_SETS = {
    "32b": EXPERIMENTS_32B,
    "16m": EXPERIMENTS_16M,
    "128m": EXPERIMENTS_128M,
    "1g": EXPERIMENTS_1G,
    "all": EXPERIMENTS_32B + EXPERIMENTS_16M + EXPERIMENTS_128M + EXPERIMENTS_1G,
    "smoke-3node-32b": EXPERIMENTS_SMOKE_3NODE_32B,
    "temp-3node-all": EXPERIMENTS_TEMP_3NODE_ALL,
}

S3_BUCKET_PREFIXES = ["lcbench-blob-singleget"]
DYNAMODB_TABLE_PREFIXES = [
    "lcbench_blob_singleget_meta",
    "lcbench_blob_singleget_holders",
]

WORKFLOW_COLOR = "\033[1;36m"
RESET_COLOR = "\033[0m"
DEFAULT_ALLOCATE_CONFIG = "cloudlab/.config/allocate-9node.ini"
EXPECTED_PROFILE = "LambdaChannel-9node"
RPC_HEALTH_ATTEMPTS = 6
RPC_HEALTH_TIMEOUT_SECONDS = 20
RPC_HEALTH_RETRY_DELAY_SECONDS = 5


def log(message: str) -> None:
    prefix = "[multi-node-blob-single-get]"
    if sys.stdout.isatty() and not os.environ.get("NO_COLOR"):
        prefix = f"{WORKFLOW_COLOR}{prefix}{RESET_COLOR}"
    print(f"\n{prefix} {message}", flush=True)


def local_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else (ROOT / path).resolve()


def run(command: list[str]) -> None:
    log("run: " + shlex.join(command))
    result = subprocess.run(command, cwd=ROOT, check=False)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, command)


def gc_command(args: argparse.Namespace, mode: str) -> list[str]:
    command = [
        "--config",
        args.cloudlab_config,
        "--s3-mode",
        mode,
        "--workers",
        str(args.aws_gc_workers),
        "--s3-max-concurrent-requests",
        str(args.aws_s3_max_concurrent_requests),
        "--yes",
    ]
    for prefix in S3_BUCKET_PREFIXES:
        command += ["--bucket-prefix", prefix]
    for prefix in DYNAMODB_TABLE_PREFIXES:
        command += ["--table-prefix", prefix]
    return command


def allocate_profile_name(allocate_config: str) -> str:
    if not Path(allocate_config).exists():
        raise FileNotFoundError(
            f"missing 9-node allocate config: {allocate_config}\n"
            "Create it from cloudlab/examples/allocate-9node.ini or pass --allocate-config."
        )
    cfg = configparser.ConfigParser()
    cfg.read(allocate_config)
    if not cfg.has_section("profile"):
        return ""
    return cfg["profile"].get("name", "").strip()


def require_allocate_profile(args: argparse.Namespace) -> None:
    if not args.expected_profile:
        return
    actual = allocate_profile_name(args.allocate_config)
    if actual != args.expected_profile:
        raise RuntimeError(
            f"single-getter workflow expects CloudLab profile {args.expected_profile!r}; "
            f"{args.allocate_config} currently has profile {actual!r}. "
            "Pass a 9-node allocate ini with --allocate-config."
        )


def nodes_from_config(cloudlab_config: str):
    cfg = configparser.ConfigParser()
    cfg.read(cloudlab_config)
    return read_nodes(local_path(cfg["paths"].get("nodes_file")))


def recorded_portal_experiment_id(cloudlab_config: str) -> str:
    cfg = configparser.ConfigParser()
    cfg.read(cloudlab_config)
    nodes_file = local_path(cfg["paths"].get("nodes_file"))
    nodes_cfg = configparser.ConfigParser()
    nodes_cfg.read(nodes_file)
    if not nodes_cfg.has_section("experiment"):
        return ""
    return nodes_cfg["experiment"].get("portal_experiment_id", "").strip()


def refresh_recorded_nodes(args: argparse.Namespace) -> None:
    command = ["--config", args.allocate_config]
    if args.refresh_experiment_id:
        command += ["--experiment-id", args.refresh_experiment_id]
    log("refresh nodes from CloudLab manifest")
    try:
        result = refresh_nodes.main(command)
    except Exception as exc:
        recorded_id = recorded_portal_experiment_id(args.cloudlab_config)
        if args.refresh_experiment_id and recorded_id == args.refresh_experiment_id:
            log(
                "refresh nodes failed; using existing nodes file because it "
                f"already records portal_experiment_id={recorded_id}: {exc}"
            )
            return
        raise
    log(f"refreshed nodes: {len(result.nodes)}")
    log(f"nodes file: {result.nodes_file}")


def default_rpc_url(args: argparse.Namespace) -> str:
    if args.rpc_url:
        return args.rpc_url
    nodes = nodes_from_config(args.cloudlab_config)
    return f"{nodes[0].host}:19000"


def participant_instance_ids(experiment: str) -> set[str]:
    path = Path(experiment)
    with path.open("rb") as file:
        data = tomllib.load(file)
    participants = data.get("participants", [])
    ids = {
        participant.get("instance_id", "").strip()
        for participant in participants
        if isinstance(participant, dict)
    }
    ids.discard("")
    if not ids:
        raise RuntimeError(f"experiment {experiment} does not define any [[participants]]")
    return ids


def required_participant_ids(experiments: list[str]) -> set[str]:
    required: set[str] = set()
    for experiment in experiments:
        required.update(participant_instance_ids(experiment))
    return required


def require_participant_nodes(args: argparse.Namespace) -> None:
    nodes = nodes_from_config(args.cloudlab_config)
    node_names = {node.name for node in nodes}
    required = required_participant_ids(args.experiments)
    missing = sorted(required - node_names)
    if missing:
        raise RuntimeError(
            "nodes.ini does not contain every instance required by the experiment TOMLs; "
            f"missing={missing}, available={sorted(node_names)}"
        )
    log(
        "experiment participants: "
        + ", ".join(sorted(required))
        + f" (available nodes={len(nodes)})"
    )


def check_node_rpc_health(args: argparse.Namespace) -> None:
    rpc_url = default_rpc_url(args)
    command = [args.lc_bench, "health", "--url", rpc_url]
    for attempt in range(1, RPC_HEALTH_ATTEMPTS + 1):
        log(f"check node RPC health: {rpc_url} (attempt {attempt}/{RPC_HEALTH_ATTEMPTS})")
        try:
            result = subprocess.run(
                command,
                cwd=ROOT,
                check=False,
                timeout=RPC_HEALTH_TIMEOUT_SECONDS,
            )
            if result.returncode == 0:
                return
            failure = f"exit code {result.returncode}"
        except subprocess.TimeoutExpired:
            failure = f"timed out after {RPC_HEALTH_TIMEOUT_SECONDS}s"
        if attempt == RPC_HEALTH_ATTEMPTS:
            raise RuntimeError(f"node RPC health failed after {RPC_HEALTH_ATTEMPTS} attempts: {failure}")
        log(
            "node RPC health failed "
            f"({failure}); retrying in {RPC_HEALTH_RETRY_DELAY_SECONDS}s"
        )
        time.sleep(RPC_HEALTH_RETRY_DELAY_SECONDS)


def run_proxy_command(args: argparse.Namespace, experiment: str, index: int) -> list[str]:
    command = [
        "--config",
        args.cloudlab_config,
        "--binary",
        args.lc_bench,
        "--experiment",
        experiment,
        "--csv",
        args.csv_output,
        "--log",
        str(workflow_paths.proxy_log_path(args.workflow_outputs, experiment, index)),
    ]
    if args.rpc_url:
        command += ["--rpc-url", args.rpc_url]
    return command


def collect_remote_node_logs(args: argparse.Namespace) -> None:
    cfg = configparser.ConfigParser()
    cfg.read(args.cloudlab_config)
    nodes = read_nodes(local_path(cfg["paths"].get("nodes_file")))
    remote_expr_log = cfg["runtime"].get("remote_expr_log", "/local/lc-bench-node.log")
    output_dir = args.workflow_outputs.remote_log_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    for node in nodes:
        local_log = output_dir / f"{node.name}-lc-bench-node.log"
        log(f"collect remote node log: {node.name}:{remote_expr_log} -> {local_log}")
        conn = connect(node=node, cfg=cfg, project_path=local_path)
        try:
            conn.get(remote=remote_expr_log, local=str(local_log))
        except Exception as exc:
            log(f"collect remote node log failed for {node.name}: {exc}")
        finally:
            conn.close()


def remote_instances_file(cloudlab_config: str) -> str:
    cfg = configparser.ConfigParser()
    cfg.read(cloudlab_config)
    remote_repo_dir = cfg["deploy"].get("remote_repo_dir", "/local/cloudlab-workspace")
    return f"{remote_repo_dir}/config/instances/cloudlab-c6620-9.toml"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cloudlab-config",
        default=os.environ.get("CLOUDLAB_CONFIG", "cloudlab/.config/cloudlab.ini"),
    )
    parser.add_argument(
        "--allocate-config",
        default=os.environ.get("ALLOCATE_CONFIG", DEFAULT_ALLOCATE_CONFIG),
    )
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"))
    parser.add_argument(
        "--lc-bench",
        default=os.environ.get("LC_BENCH", str(ROOT / "target/release/lc-bench")),
    )
    parser.add_argument("--csv-output", default=os.environ.get("BLOB_SINGLE_GETTER_CSV_OUTPUT"))
    parser.add_argument(
        "--aws-gc-workers",
        type=int,
        default=int(os.environ.get("AWS_GC_WORKERS", "16")),
    )
    parser.add_argument(
        "--aws-s3-max-concurrent-requests",
        type=int,
        default=int(os.environ.get("AWS_S3_MAX_CONCURRENT_REQUESTS", "64")),
    )
    parser.add_argument("--skip-allocate", action="store_true")
    parser.add_argument(
        "--refresh-experiment-id",
        default=os.environ.get("CLOUDLAB_EXPERIMENT_ID"),
        help="experiment id/name passed to refresh_nodes.py when --skip-allocate is used",
    )
    parser.add_argument("--skip-deploy", action="store_true")
    parser.add_argument("--skip-ready-portal", action="store_true")
    parser.add_argument(
        "--remote-proxy-only",
        action="store_true",
        default=os.environ.get("LC_BENCH_REMOTE_PROXY_ONLY") == "1",
        help=(
            "run only the workflow proxy experiments against already-started "
            "remote daemons; skip refresh/readiness/deploy/start/stop/AWS GC"
        ),
    )
    parser.add_argument(
        "--expected-profile",
        default=os.environ.get("BLOB_SINGLE_GETTER_EXPECTED_PROFILE", EXPECTED_PROFILE),
        help="when allocating, require the selected allocate ini to name this CloudLab profile",
    )
    parser.add_argument(
        "--skip-aws-gc",
        action="store_true",
        default=os.environ.get("LC_BENCH_SKIP_AWS_GC") == "1",
    )
    parser.add_argument(
        "--experiments",
        nargs="+",
        help="explicit single-getter experiment TOMLs; overrides --experiment-set",
    )
    parser.add_argument(
        "--experiment-set",
        choices=sorted(EXPERIMENT_SETS),
        default=os.environ.get("BLOB_SINGLE_GETTER_EXPERIMENT_SET", "all"),
    )
    args = parser.parse_args(argv)

    args.lc_bench = str(local_path(args.lc_bench))
    args.cloudlab_config = str(local_path(args.cloudlab_config))
    args.allocate_config = str(local_path(args.allocate_config))
    experiments = args.experiments or EXPERIMENT_SETS[args.experiment_set]
    args.experiments = [str(local_path(path)) for path in experiments]
    args.csv_output = str(local_path(args.csv_output)) if args.csv_output else None
    args.workflow_stamp = workflow_paths.timestamp()
    return args


def configure_workflow_outputs(args: argparse.Namespace) -> None:
    outputs = workflow_paths.workflow_outputs(
        root=ROOT,
        cloudlab_config=args.cloudlab_config,
        allocate_config=args.allocate_config,
        workflow_name="blob-single-getter",
        csv_prefix="blob-single-getter",
        stamp=args.workflow_stamp,
        csv_output=args.csv_output,
    )
    args.workflow_outputs = outputs
    args.csv_output = str(outputs.csv_output)
    log(f"remote profile: {outputs.profile_name}")
    log(f"workflow CSV output: {args.csv_output}")
    log(f"workflow log dir: {outputs.log_dir}")


def main(argv: list[str] | None = None) -> list[run_proxy_experiment.ProxyResult]:
    args = parse_args(argv)
    proxy_results: list[run_proxy_experiment.ProxyResult] = []

    if args.remote_proxy_only:
        args.skip_deploy = True
        args.skip_aws_gc = True

    run(["cargo", "test"])
    run(["cargo", "build", "--release"])

    if args.skip_deploy:
        log("skip package; reusing existing remote deployment")
    else:
        log("package local working tree")
        package_result = package_entrypoint.main(["--config", args.cloudlab_config])
        log(f"package file: {package_result.package_file}")

    if not args.skip_allocate:
        require_allocate_profile(args)
        log("allocate CloudLab 9-node profile")
        allocate_result = allocate_profile.main(["--config", args.allocate_config])
        log(f"experiment id: {allocate_result.experiment_id}")
        log(f"nodes file: {allocate_result.nodes_file}")
    else:
        log("skip allocation; using existing nodes file")
        if args.remote_proxy_only:
            log("remote proxy-only mode: skip node manifest refresh")
        else:
            refresh_recorded_nodes(args)

    configure_workflow_outputs(args)

    if args.remote_proxy_only:
        log("remote proxy-only mode: skip CloudLab node readiness checks")
    else:
        ready_args = [
            "--config",
            args.cloudlab_config,
            "--allocate-config",
            args.allocate_config,
            "--wait",
            "--require-ssh-auth",
        ]
        if args.skip_ready_portal:
            ready_args.append("--skip-portal")

        log("wait for CloudLab node readiness")
        ready_result = check_experiment_ready.main(ready_args + ["--poll-sec", "300"])
        if not ready_result.ready:
            raise RuntimeError(
                f"CloudLab nodes are not ready after {ready_result.attempts} readiness attempt(s)"
            )
    require_participant_nodes(args)

    if args.skip_deploy:
        log("skip deploy; reusing existing remote deployment")
    else:
        log("deploy bundle and build on CloudLab nodes")
        deploy_result = deploy.main(["--config", args.cloudlab_config])
        log(f"deployed nodes: {len(deploy_result.nodes)}")

    if args.remote_proxy_only:
        log("remote proxy-only mode: use already-running lc-bench node daemons")
    else:
        log("start long-lived lc-bench node daemons with 9-node instances file")
        start_result = start_expr_servers.main(
            [
                "--config",
                args.cloudlab_config,
                "--remote-instances-file",
                remote_instances_file(args.cloudlab_config),
            ]
        )
        log(f"started nodes: {len(start_result.nodes)}")

    workflow_failed = True
    try:
        if not args.skip_aws_gc:
            log("preflight AWS GC: remove empty prefixed buckets and stale tables")
            gc_result = gc_aws_resources.main(gc_command(args, "empty-only"))
            if gc_result.failures:
                raise RuntimeError(f"preflight AWS GC had {gc_result.failures} failure(s)")

        for index, experiment in enumerate(args.experiments):
            check_node_rpc_health(args)
            log(f"run proxy experiment: {experiment}")
            proxy_result = run_proxy_experiment.main(run_proxy_command(args, experiment, index))
            proxy_results.append(proxy_result)
            log(f"csv output: {proxy_result.csv_output}")
            log(f"log output: {proxy_result.log_output}")
            if proxy_result.return_code != 0:
                raise RuntimeError(
                    f"proxy experiment failed with exit code {proxy_result.return_code}: "
                    f"{proxy_result.experiment}"
                )
        workflow_failed = False
    finally:
        if workflow_failed and not args.remote_proxy_only:
            collect_remote_node_logs(args)

        if args.remote_proxy_only:
            log("remote proxy-only mode: leave lc-bench node daemons running")
        else:
            log("stop long-lived lc-bench node daemons")
            try:
                kill_result = kill_expr_servers.main(["--config", args.cloudlab_config])
                log(f"stopped nodes: {len(kill_result.nodes)}")
            except Exception as exc:
                log(f"stop node failed: {exc}")
        if not args.skip_aws_gc:
            log("final AWS GC: force-clean prefixed benchmark resources")
            try:
                final_gc_result = gc_aws_resources.main(gc_command(args, "force"))
                if final_gc_result.failures:
                    log(f"final AWS GC had {final_gc_result.failures} failure(s)")
            except Exception as exc:
                log(f"final AWS GC failed: {exc}")

    return proxy_results


def print_result(proxy_results: list[run_proxy_experiment.ProxyResult]) -> None:
    log("workflow finished")
    csv_outputs = sorted({str(result.csv_output) for result in proxy_results})
    for csv_output in csv_outputs:
        log(f"workflow csv: {csv_output}")
    for proxy_result in proxy_results:
        log(f"{proxy_result.experiment.name}: {proxy_result.log_output}")


def cli(argv: list[str] | None = None) -> int:
    print_result(main(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
