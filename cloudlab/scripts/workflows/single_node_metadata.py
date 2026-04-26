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
sys.path.insert(0, str(ENTRYPOINT_DIR))

import allocate_profile
import check_experiment_ready
import deploy
import gc_aws_resources
import kill_expr_servers
import package as package_entrypoint
import record_single
import run_proxy_experiment
import start_expr_servers

METADATA_EXPERIMENTS = [
    "config/experiments/metadata/append.toml",
    "config/experiments/metadata/prefix-scan.toml",
    "config/experiments/metadata/competitive-claim-local.toml",
]

DYNAMODB_TABLE_PREFIXES = [
    "lcbench-metadata-append",
    "lcbench-metadata-prefix-scan",
    "lcbench-metadata-claim-local",
]

WORKFLOW_COLOR = "\033[1;35m"
RESET_COLOR = "\033[0m"


def log(message: str) -> None:
    prefix = "[single-node-metadata]"
    if sys.stdout.isatty() and not os.environ.get("NO_COLOR"):
        prefix = f"{WORKFLOW_COLOR}{prefix}{RESET_COLOR}"
    print(f"\n{prefix} {message}", flush=True)


def local_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else (ROOT / path).resolve()


def default_csv_output(cloudlab_config: str, name: str) -> Path:
    cfg = configparser.ConfigParser()
    cfg.read(cloudlab_config)
    results_dir = "cloudlab/results"
    if cfg.has_section("paths"):
        results_dir = cfg["paths"].get("results_dir", results_dir)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return local_path(results_dir) / "workflow" / f"{name}-{stamp}.csv"


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


def run_proxy_command(args: argparse.Namespace, experiment: str) -> list[str]:
    command = [
        "--config",
        args.cloudlab_config,
        "--binary",
        args.lc_bench,
        "--experiment",
        experiment,
        "--csv",
        args.csv_output,
    ]
    if args.rpc_url:
        command += ["--rpc-url", args.rpc_url]
    return command


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
        "--skip-allocate",
        action="store_true",
        help="use the existing nodes file instead of creating a new CloudLab experiment",
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
    args.csv_output = str(
        local_path(args.csv_output)
        if args.csv_output
        else default_csv_output(args.cloudlab_config, "metadata")
    )
    return args


def main(argv: list[str] | None = None) -> list[run_proxy_experiment.ProxyResult]:
    args = parse_args(argv)
    proxy_results: list[run_proxy_experiment.ProxyResult] = []
    log(f"workflow CSV output: {args.csv_output}")

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
    ready_result = check_experiment_ready.main(ready_args)
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

    try:
        # 4. Keep AWS resource deletion outside the measured datapath.
        if not args.skip_aws_gc:
            log("preflight AWS GC: remove stale prefixed DynamoDB tables")
            gc_result = gc_aws_resources.main(gc_command(args, "empty-only"))
            if gc_result.failures:
                raise RuntimeError(f"preflight AWS GC had {gc_result.failures} failure(s)")

        # 5. Run each metadata experiment from the local proxy.
        for experiment in args.experiments:
            log(f"run proxy experiment: {experiment}")
            proxy_result = run_proxy_experiment.main(run_proxy_command(args, experiment))
            proxy_results.append(proxy_result)
            log(f"csv output: {proxy_result.csv_output}")
            log(f"log output: {proxy_result.log_output}")
            if proxy_result.return_code != 0:
                raise RuntimeError(
                    f"proxy experiment failed with exit code {proxy_result.return_code}: "
                    f"{proxy_result.experiment}"
                )
    finally:
        # 6. Always try to stop the node daemon and run local AWS GC.
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
