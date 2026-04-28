#!/usr/bin/env python3
"""
Run the single-node CloudLab metadata workflow.

This file is deliberately plain: it follows the same steps a person would run
by hand, using the existing Python entrypoints directly.
"""

from __future__ import annotations

import argparse
import configparser
import os
import shlex
import subprocess
import sys
import time
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
import record_single
import refresh_nodes
import run_proxy_experiment
import start_expr_servers
import workflow_paths
from nodes import read_nodes
from ssh import connect

METADATA_EXPERIMENTS = [
    "config/experiments/metadata/single-node/append.toml",
    "config/experiments/metadata/single-node/prefix-scan.toml",
    "config/experiments/metadata/single-node/competitive-claim-local.toml",
]

DYNAMODB_TABLE_PREFIXES = [
    "lcbench-metadata-append",
    "lcbench-metadata-prefix-scan",
    "lcbench-metadata-claim-local",
]

WORKFLOW_COLOR = "\033[1;35m"
RESET_COLOR = "\033[0m"
RPC_HEALTH_ATTEMPTS = 6
RPC_HEALTH_TIMEOUT_SECONDS = 20
RPC_HEALTH_RETRY_DELAY_SECONDS = 5


def log(message: str) -> None:
    prefix = "[single-node-metadata]"
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
        "--yes",
    ]
    for prefix in DYNAMODB_TABLE_PREFIXES:
        command += ["--table-prefix", prefix]
    return command


def default_rpc_url(args: argparse.Namespace) -> str:
    if args.rpc_url:
        return args.rpc_url
    cfg = configparser.ConfigParser()
    cfg.read(args.cloudlab_config)
    nodes = read_nodes(local_path(cfg["paths"].get("nodes_file")))
    return f"{nodes[0].host}:19000"


def refresh_recorded_nodes(args: argparse.Namespace) -> None:
    command = ["--config", args.allocate_config]
    if args.refresh_experiment_id:
        command += ["--experiment-id", args.refresh_experiment_id]
    log("refresh nodes from CloudLab manifest")
    result = refresh_nodes.main(command)
    log(f"refreshed nodes: {len(result.nodes)}")
    log(f"nodes file: {result.nodes_file}")


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


def settle_after_experiment(args: argparse.Namespace) -> None:
    if args.settle_sec <= 0:
        return
    log(f"settle after experiment: sleep {args.settle_sec:.1f}s")
    time.sleep(args.settle_sec)


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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cloudlab-config",
        default=os.environ.get("CLOUDLAB_CONFIG", "cloudlab/.config/cloudlab.ini"),
    )
    parser.add_argument(
        "--allocate-config",
        default=os.environ.get("ALLOCATE_CONFIG", "cloudlab/.config/allocate.ini"),
    )
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"))
    parser.add_argument(
        "--lc-bench",
        default=os.environ.get("LC_BENCH", str(ROOT / "target/release/lc-bench")),
    )
    parser.add_argument(
        "--csv-output",
        default=os.environ.get("METADATA_CSV_OUTPUT"),
        help="append all workflow datapoints to this one CSV file",
    )
    parser.add_argument(
        "--aws-gc-workers",
        type=int,
        default=int(os.environ.get("AWS_GC_WORKERS", "16")),
    )
    parser.add_argument(
        "--settle-sec",
        type=float,
        default=float(os.environ.get("METADATA_SETTLE_SEC", "3")),
        help="seconds to wait after each proxy experiment",
    )
    parser.add_argument(
        "--skip-allocate",
        action="store_true",
        help="use the existing nodes file instead of creating a new CloudLab experiment",
    )
    parser.add_argument(
        "--refresh-experiment-id",
        default=os.environ.get("CLOUDLAB_EXPERIMENT_ID"),
        help="experiment id/name passed to refresh_nodes.py when --skip-allocate is used",
    )
    parser.add_argument(
        "--skip-refresh-nodes",
        action="store_true",
        default=os.environ.get("LC_BENCH_SKIP_REFRESH_NODES") == "1",
        help="when --skip-allocate is used, trust the existing nodes.ini",
    )
    parser.add_argument(
        "--skip-deploy",
        action="store_true",
        help="reuse the existing remote deployment instead of packaging and deploying",
    )
    parser.add_argument(
        "--record-existing-host",
        default=os.environ.get("CLOUDLAB_HOST"),
        help="record this existing CloudLab host instead of allocating",
    )
    parser.add_argument("--record-existing-user", default=os.environ.get("CLOUDLAB_USER", "Finch"))
    parser.add_argument(
        "--record-existing-experiment",
        default=os.environ.get("CLOUDLAB_EXPERIMENT", "lc-test"),
    )
    parser.add_argument("--skip-ready-portal", action="store_true")
    parser.add_argument(
        "--skip-aws-gc",
        action="store_true",
        default=os.environ.get("LC_BENCH_SKIP_AWS_GC") == "1",
    )
    parser.add_argument(
        "--experiments",
        nargs="+",
        default=METADATA_EXPERIMENTS,
        help="metadata experiment TOMLs to run",
    )
    args = parser.parse_args(argv)

    args.lc_bench = str(local_path(args.lc_bench))
    args.cloudlab_config = str(local_path(args.cloudlab_config))
    args.allocate_config = str(local_path(args.allocate_config))
    args.experiments = [str(local_path(path)) for path in args.experiments]
    args.csv_output = str(local_path(args.csv_output)) if args.csv_output else None
    args.workflow_stamp = workflow_paths.timestamp()
    return args


def configure_workflow_outputs(args: argparse.Namespace) -> None:
    outputs = workflow_paths.workflow_outputs(
        root=ROOT,
        cloudlab_config=args.cloudlab_config,
        allocate_config=args.allocate_config,
        workflow_name="metadata",
        csv_prefix="metadata",
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

    # 0. Local sanity check and local release binary for the proxy.
    run(["cargo", "test"])
    run(["cargo", "build", "--release"])

    # 1. Package the local working tree for CloudLab, unless the remote
    # deployment is already good enough to reuse.
    if args.skip_deploy:
        log("skip package; reusing existing remote deployment")
    else:
        log("package local working tree")
        package_result = package_entrypoint.main(["--config", args.cloudlab_config])
        log(f"package file: {package_result.package_file}")

    # 2. Allocate a fresh node, or record an existing one if requested.
    if args.record_existing_host:
        log(f"record existing node: {args.record_existing_user}@{args.record_existing_host}")
        record_result = record_single.main(
            [
                "--config",
                args.cloudlab_config,
                "--experiment",
                args.record_existing_experiment,
                "--host",
                args.record_existing_host,
                "--user",
                args.record_existing_user,
                "--node-name",
                "node-0",
            ]
        )
        log(f"nodes file: {record_result.nodes_file}")
    elif not args.skip_allocate:
        log("allocate CloudLab profile")
        allocate_result = allocate_profile.main(
            [
                "--config",
                args.allocate_config,
            ]
        )
        log(f"experiment id: {allocate_result.experiment_id}")
        log(f"nodes file: {allocate_result.nodes_file}")
    else:
        log("skip allocation; using existing nodes file")
        if args.skip_refresh_nodes:
            log("skip node manifest refresh; trusting existing nodes file")
        else:
            refresh_recorded_nodes(args)

    configure_workflow_outputs(args)

    # 3. Wait/check readiness, then deploy and start the long-lived node daemon.
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
    ready_result = check_experiment_ready.main(ready_args + ["--poll-sec", "30"])
    if not ready_result.ready:
        raise RuntimeError(
            f"CloudLab node is not ready after {ready_result.attempts} readiness attempt(s)"
        )

    if args.skip_deploy:
        log("skip deploy; reusing existing remote deployment")
    else:
        log("deploy bundle and build on CloudLab node")
        deploy_result = deploy.main(["--config", args.cloudlab_config])
        log(f"deployed nodes: {len(deploy_result.nodes)}")

        log("wait for CloudLab node readiness after deploy")
        ready_result = check_experiment_ready.main(ready_args)
        if not ready_result.ready:
            raise RuntimeError(
                f"CloudLab node is not ready after deploy; "
                f"waited {ready_result.attempts} readiness attempt(s)"
            )

    log("start long-lived lc-bench node daemon")
    start_result = start_expr_servers.main(
        [
            "--config",
            args.cloudlab_config,
        ]
    )
    log(f"started nodes: {len(start_result.nodes)}")

    workflow_failed = True
    try:
        # 4. Keep AWS resource deletion outside the measured datapath.
        if not args.skip_aws_gc:
            log("preflight AWS GC: remove stale prefixed DynamoDB tables")
            gc_result = gc_aws_resources.main(gc_command(args, "empty-only"))
            if gc_result.failures:
                raise RuntimeError(f"preflight AWS GC had {gc_result.failures} failure(s)")

        # 5. Run each metadata experiment from the local proxy.
        for index, experiment in enumerate(args.experiments):
            check_node_rpc_health(args)
            log(f"run proxy experiment: {experiment}")
            proxy_result = run_proxy_experiment.main(run_proxy_command(args, experiment, index))
            proxy_results.append(proxy_result)
            log(f"csv output: {proxy_result.csv_output}")
            log(f"log output: {proxy_result.log_output}")
            settle_after_experiment(args)
            if proxy_result.return_code != 0:
                raise RuntimeError(
                    f"proxy experiment failed with exit code {proxy_result.return_code}: "
                    f"{proxy_result.experiment}"
                )
        workflow_failed = False
    finally:
        # 6. Always try to stop the node daemon and run local AWS GC.
        if workflow_failed:
            collect_remote_node_logs(args)

        log("stop long-lived lc-bench node daemon")
        try:
            kill_result = kill_expr_servers.main(
                [
                    "--config",
                    args.cloudlab_config,
                ]
            )
            log(f"stopped nodes: {len(kill_result.nodes)}")
        except Exception as exc:
            log(f"stop node failed: {exc}")
        if not args.skip_aws_gc:
            log("final AWS GC: force-clean prefixed DynamoDB tables")
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
