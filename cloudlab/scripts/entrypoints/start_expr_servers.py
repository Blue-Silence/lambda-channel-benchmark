#!/usr/bin/env python3
"""
Start lc-bench node daemons on every CloudLab node in nodes.ini.

The instance inventory is a preconfigured TOML file already present in the
deployed workspace. Each remote node infers its instance id from its hostname,
so this script intentionally does not generate or rewrite topology config.
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
class StartResult:
    config_path: Path
    nodes: list[Node]


def log(message: str) -> None:
    print(f"[start-expr] {message}", flush=True)


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
    cfg.optionxform = str
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
    parser.add_argument(
        "--remote-instances-file",
        default=None,
        help="override [runtime] remote_instances_file for this start",
    )
    return parser.parse_args(argv)


def read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"runtime aws_env_file does not exist: {path}")

    env: dict[str, str] = {}
    for lineno, raw_line in enumerate(path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        if "=" not in line:
            raise ValueError(f"invalid env file line {path}:{lineno}: missing '='")

        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not name:
            raise ValueError(f"invalid env file line {path}:{lineno}: empty variable name")
        if not name.replace("_", "").isalnum() or name[0].isdigit():
            raise ValueError(f"invalid env file line {path}:{lineno}: invalid variable name {name!r}")
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        env[name] = value

    return env


def runtime_env(runtime: configparser.SectionProxy) -> dict[str, str]:
    env: dict[str, str] = {}

    aws_env_file = runtime.get("aws_env_file", fallback="").strip()
    if aws_env_file:
        env.update(read_env_file(project_path(aws_env_file)))

    env.update(
        {
            key.removeprefix("env."): value
            for key, value in runtime.items()
            if key.startswith("env.")
        }
    )

    return env


def remote_node_cmd(
    *,
    remote_binary: str,
    remote_repo_dir: str,
    remote_instances_file: str,
    remote_pid_file: str,
    remote_expr_log: str,
    extra_env: dict[str, str],
) -> str:
    args = [
        remote_binary,
        "node",
        "--instances",
        remote_instances_file,
    ]
    command = " ".join(shlex.quote(arg) for arg in args)
    env = " ".join(
        f"{name}={shlex.quote(value)}" for name, value in sorted(extra_env.items()) if value
    )
    if env:
        command = f"env {env} {command}"

    inner = (
        f"cd {shlex.quote(remote_repo_dir)}; "
        f"nohup {command} > {shlex.quote(remote_expr_log)} 2>&1 < /dev/null & "
        f"echo $! > {shlex.quote(remote_pid_file)}"
    )
    return f"bash -lc {shlex.quote(inner)}"


def start_node(node: Node, cfg: configparser.ConfigParser) -> None:
    deploy = cfg["deploy"]
    runtime = cfg["runtime"]

    remote_repo_dir = deploy.get("remote_repo_dir", "/local/cloudlab-workspace")
    remote_binary = runtime.get(
        "remote_binary",
        f"{remote_repo_dir}/target/release/lc-bench",
    )
    remote_instances_file = runtime.get(
        "remote_instances_file",
        f"{remote_repo_dir}/config/instances/local-two.toml",
    )
    remote_pid_file = runtime.get("remote_pid_file", "/local/lc-bench-node.pid")
    remote_expr_log = runtime.get("remote_expr_log", "/local/lc-bench-node.log")
    restart = runtime.getboolean("restart", fallback=True)

    extra_env = runtime_env(runtime)

    log(f"{node.name}: connect {node.user}@{node.host}:{node.port}")
    conn = connect(node=node, cfg=cfg, project_path=project_path)

    try:
        log(f"{node.name}: verify binary and instances file")
        conn.run(f"test -x {shlex.quote(remote_binary)}")
        conn.run(f"test -f {shlex.quote(remote_instances_file)}")

        if restart:
            log(f"{node.name}: stop existing daemon if present")
            conn.run(
                f"if test -f {shlex.quote(remote_pid_file)}; then "
                f"pid=$(cat {shlex.quote(remote_pid_file)}); "
                "if kill -0 \"$pid\" 2>/dev/null; then kill \"$pid\"; fi; "
                f"rm -f {shlex.quote(remote_pid_file)}; "
                "fi"
            )

        log(f"{node.name}: start daemon")
        conn.run(
            remote_node_cmd(
                remote_binary=remote_binary,
                remote_repo_dir=remote_repo_dir,
                remote_instances_file=remote_instances_file,
                remote_pid_file=remote_pid_file,
                remote_expr_log=remote_expr_log,
                extra_env=extra_env,
            )
        )
        conn.run(
            f"pid=$(cat {shlex.quote(remote_pid_file)}); "
            "sleep 1; kill -0 \"$pid\""
        )
        log(f"{node.name}: started")

    finally:
        conn.close()


def main(argv: list[str] | None = None) -> StartResult:
    args = parse_args(argv)
    config_path = args.config.expanduser().resolve()
    cfg = read_config(config_path)
    if args.remote_instances_file:
        if not cfg.has_section("runtime"):
            cfg.add_section("runtime")
        cfg["runtime"]["remote_instances_file"] = args.remote_instances_file

    nodes_file = project_path(cfg["paths"].get("nodes_file"))
    nodes = read_nodes(nodes_file)

    parallel = cfg.getboolean("deploy", "parallel", fallback=True)
    max_workers_raw = cfg.get("deploy", "max_workers", fallback="").strip()
    max_workers = int(max_workers_raw) if max_workers_raw else len(nodes)

    log(f"loaded {len(nodes)} node(s)")

    if not cfg.has_section("runtime"):
        raise ValueError("cloudlab.ini missing [runtime] section")

    run_on_nodes(
        nodes=nodes,
        action_name="start",
        parallel=parallel,
        max_workers=max_workers,
        task=lambda node: start_node(node, cfg),
        log=log,
    )
    log("all expr servers started successfully")
    return StartResult(config_path=config_path, nodes=nodes)


def print_result(result: StartResult) -> None:
    log(f"started nodes: {len(result.nodes)}")


def cli(argv: list[str] | None = None) -> int:
    print_result(main(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
