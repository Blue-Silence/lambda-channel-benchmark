#!/usr/bin/env python3
"""
Stop lc-bench node daemons on every CloudLab node in nodes.ini.
"""

from __future__ import annotations

import argparse
import configparser
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path

ENTRYPOINT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = ENTRYPOINT_DIR.parent
CLOUDLAB_DIR = SCRIPTS_DIR.parent
ROOT = CLOUDLAB_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR / "lib"))

from nodes import Node, read_nodes
from parallel import run_on_nodes
from ssh import connect


CONFIG_FILE = CLOUDLAB_DIR / ".config" / "cloudlab.ini"


@dataclass(frozen=True)
class KillResult:
    config_path: Path
    nodes: list[Node]


def log(message: str) -> None:
    print(f"[kill-expr] {message}", flush=True)


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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=CONFIG_FILE,
        help=f"cloudlab config file, default: {CONFIG_FILE}",
    )
    return parser.parse_args(argv)


def kill_node(node: Node, cfg: configparser.ConfigParser) -> None:
    runtime = cfg["runtime"]

    remote_pid_file = runtime.get("remote_pid_file", "/local/lc-bench-node.pid")
    fallback_pattern = runtime.get("kill_fallback_pattern", "lc-bench node").strip()

    log(f"{node.name}: connect {node.user}@{node.host}:{node.port}")
    conn = connect(node=node, cfg=cfg, project_path=project_path)

    try:
        log(f"{node.name}: stop daemon")
        conn.run(
            f"if test -f {shlex.quote(remote_pid_file)}; then "
            f"pid=$(cat {shlex.quote(remote_pid_file)}); "
            "if kill -0 \"$pid\" 2>/dev/null; then kill \"$pid\"; fi; "
            f"rm -f {shlex.quote(remote_pid_file)}; "
            "fi"
        )
        if fallback_pattern:
            safe_pattern = fallback_pattern.replace("lc-bench", "[l]c-bench")
            conn.run(f"pkill -f {shlex.quote(safe_pattern)} || true")
        log(f"{node.name}: stopped")

    finally:
        conn.close()


def main(argv: list[str] | None = None) -> KillResult:
    args = parse_args(argv)
    config_path = args.config.expanduser().resolve()
    cfg = read_config(config_path)

    if not cfg.has_section("runtime"):
        raise ValueError("cloudlab.ini missing [runtime] section")

    nodes_file = project_path(cfg["paths"].get("nodes_file"))
    nodes = read_nodes(nodes_file)

    parallel = cfg.getboolean("deploy", "parallel", fallback=True)
    max_workers_raw = cfg.get("deploy", "max_workers", fallback="").strip()
    max_workers = int(max_workers_raw) if max_workers_raw else len(nodes)

    log(f"loaded {len(nodes)} node(s)")

    run_on_nodes(
        nodes=nodes,
        action_name="kill",
        parallel=parallel,
        max_workers=max_workers,
        task=lambda node: kill_node(node, cfg),
        log=log,
    )
    log("all expr servers stopped successfully")
    return KillResult(config_path=config_path, nodes=nodes)


def print_result(result: KillResult) -> None:
    log(f"stopped nodes: {len(result.nodes)}")


def cli(argv: list[str] | None = None) -> int:
    print_result(main(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
