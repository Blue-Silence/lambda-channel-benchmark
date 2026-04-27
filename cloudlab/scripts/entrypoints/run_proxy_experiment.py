#!/usr/bin/env python3
"""
Run one lc-bench proxy experiment locally against a CloudLab node.

The CloudLab node daemon should already be running. This script reads
cloudlab/.generated/nodes.ini, chooses one node, runs the local lc-bench proxy
against that node's public hostname, and writes CSV/log output locally.
"""

from __future__ import annotations

import argparse
import configparser
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ENTRYPOINT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = ENTRYPOINT_DIR.parent
CLOUDLAB_DIR = SCRIPTS_DIR.parent
ROOT = CLOUDLAB_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR / "lib"))

from nodes import Node, read_nodes


CONFIG_FILE = CLOUDLAB_DIR / ".config" / "cloudlab.ini"
DEFAULT_EXPERIMENT = "config/experiments/blob/put/32b/local-file.toml"


@dataclass(frozen=True)
class ProxyResult:
    node: Node
    binary: Path
    rpc_url: str
    experiment: Path
    csv_output: Path
    log_output: Path
    return_code: int


def log(message: str) -> None:
    print(f"[local-proxy] {message}", flush=True)


def project_path(value: str) -> Path:
    path = Path(value.strip()).expanduser()
    return path if path.is_absolute() else (ROOT / path).resolve()


def read_config(path: Path) -> configparser.ConfigParser:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing config file: {path}\n"
            "Copy cloudlab/examples/cloudlab.ini to cloudlab/.config/cloudlab.ini first."
        )

    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


def select_node(nodes: list[Node], node_name: str | None) -> Node:
    if node_name is None:
        return nodes[0]
    for node in nodes:
        if node.name == node_name:
            return node
    names = ", ".join(node.name for node in nodes)
    raise ValueError(f"unknown node {node_name!r}; available nodes: {names}")


def default_binary(cfg: configparser.ConfigParser) -> Path:
    configured = ""
    if cfg.has_section("local"):
        configured = cfg.get("local", "binary", fallback="").strip()
    if configured:
        return project_path(configured)
    return ROOT / "target" / "release" / "lc-bench"


def run_proxy(
    *,
    binary: Path,
    rpc_url: str,
    experiment: Path,
    csv_output: Path,
    log_output: Path,
) -> int:
    command = [
        str(binary),
        "proxy",
        "--url",
        rpc_url,
        "--experiment",
        str(experiment),
        "--csv",
        str(csv_output),
    ]
    log("run: " + " ".join(shlex.quote(arg) for arg in command))
    log_output.parent.mkdir(parents=True, exist_ok=True)
    csv_output.parent.mkdir(parents=True, exist_ok=True)

    with log_output.open("w") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=str(ROOT),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="")
            log_file.write(line)
        return proc.wait()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=CONFIG_FILE,
        help=f"cloudlab config file, default: {CONFIG_FILE}",
    )
    parser.add_argument(
        "--node-name",
        default=None,
        help="node from nodes.ini to contact; defaults to the first node",
    )
    parser.add_argument(
        "--rpc-url",
        default=None,
        help="RPC URL passed to lc-bench proxy; defaults to <cloudlab-public-host>:19000",
    )
    parser.add_argument(
        "--binary",
        type=Path,
        default=None,
        help="local lc-bench binary; defaults to target/release/lc-bench",
    )
    parser.add_argument(
        "--experiment",
        type=Path,
        default=Path(DEFAULT_EXPERIMENT),
        help=f"local experiment TOML, default: {DEFAULT_EXPERIMENT}",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="local CSV output; defaults under [paths] results_dir",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=None,
        help="local proxy log output; defaults under [paths] results_dir",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> ProxyResult:
    args = parse_args(argv)
    cfg = read_config(args.config.expanduser().resolve())
    nodes = read_nodes(project_path(cfg["paths"].get("nodes_file")))
    node = select_node(nodes, args.node_name)

    binary = args.binary.expanduser() if args.binary else default_binary(cfg)
    binary = binary if binary.is_absolute() else (ROOT / binary).resolve()
    experiment = args.experiment.expanduser()
    experiment = experiment if experiment.is_absolute() else (ROOT / experiment).resolve()
    rpc_url = args.rpc_url or f"{node.host}:19000"

    results_dir = project_path(cfg["paths"].get("results_dir", "cloudlab/results"))
    stamp = time.strftime("%Y%m%d-%H%M%S")
    experiment_stem = experiment.stem
    run_dir = results_dir / experiment_stem
    csv_output = args.csv or (run_dir / f"{node.name}-{experiment_stem}-{stamp}.csv")
    log_output = args.log or (run_dir / f"{node.name}-{experiment_stem}-{stamp}.log")
    csv_output = csv_output if csv_output.is_absolute() else (ROOT / csv_output).resolve()
    log_output = log_output if log_output.is_absolute() else (ROOT / log_output).resolve()

    if not binary.exists():
        raise FileNotFoundError(
            f"local binary not found: {binary}\n"
            "Run: cargo build --release"
        )
    if not experiment.exists():
        raise FileNotFoundError(f"experiment TOML not found: {experiment}")

    log(f"selected node: {node.name} {node.host}:{node.port}")
    log(f"rpc url: {rpc_url}")
    log(f"csv output: {csv_output}")
    log(f"log output: {log_output}")
    rc = run_proxy(
        binary=binary,
        rpc_url=rpc_url,
        experiment=experiment,
        csv_output=csv_output,
        log_output=log_output,
    )
    return ProxyResult(
        node=node,
        binary=binary,
        rpc_url=rpc_url,
        experiment=experiment,
        csv_output=csv_output,
        log_output=log_output,
        return_code=rc,
    )


def print_result(result: ProxyResult) -> None:
    if result.return_code == 0:
        log("proxy experiment finished successfully")
    else:
        log(f"proxy experiment failed with exit code {result.return_code}")
    log(f"csv output: {result.csv_output}")
    log(f"log output: {result.log_output}")


def cli(argv: list[str] | None = None) -> int:
    result = main(argv)
    print_result(result)
    return result.return_code


if __name__ == "__main__":
    raise SystemExit(cli())
