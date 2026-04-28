#!/usr/bin/env python3
"""
Stage 2: upload source bundle, extract it on CloudLab nodes, and build.

This script does only this on each node:

  1. upload source-bundle.tar.gz
  2. extract it into remote_repo_dir
  3. run cloudlab/scripts/remote/remote_build.py
  4. collect build log

Nodes are deployed in parallel when [deploy] parallel = true.
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
class DeployResult:
    config_path: Path
    nodes: list[Node]
    package_file: Path


def log(message: str) -> None:
    print(f"[deploy] {message}", flush=True)


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


def remote_build_cmd(
    *,
    remote_repo_dir: str,
    remote_build_log: str,
    skip_apt: bool,
) -> str:
    args = [
        "python3",
        f"{remote_repo_dir}/cloudlab/scripts/remote/remote_build.py",
        "--repo-dir",
        remote_repo_dir,
    ]

    if skip_apt:
        args.append("--skip-apt")

    cmd = " ".join(shlex.quote(arg) for arg in args)

    inner = (
        "set -o pipefail; "
        f"{cmd} 2>&1 | tee {shlex.quote(remote_build_log)}"
    )

    return f"bash -lc {shlex.quote(inner)}"


def prepare_local_disk_cmd(
    *,
    script_path: str,
    mount_point: str,
    min_size_gb: float,
) -> str:
    args = [
        "python3",
        script_path,
        "--mount-point",
        mount_point,
        "--min-size-gb",
        str(min_size_gb),
    ]
    return " ".join(shlex.quote(arg) for arg in args)


def deploy_node(node: Node, cfg: configparser.ConfigParser, package_file: Path) -> None:
    deploy = cfg["deploy"]
    paths = cfg["paths"]

    remote_tmp_dir = deploy.get("remote_tmp_dir", "/tmp/cloudlab-deploy")
    remote_repo_dir = deploy.get("remote_repo_dir", "/local/cloudlab-workspace")
    remote_build_log = deploy.get("remote_build_log", "/local/cloudlab-build.log")
    local_mount_point = deploy.get("local_mount_point", "/local")

    clean_remote = deploy.getboolean("clean_remote", fallback=True)
    skip_apt = deploy.getboolean("skip_apt", fallback=False)
    prepare_local_disk = deploy.getboolean("prepare_local_disk", fallback=True)
    local_disk_min_size_gb = deploy.getfloat("local_disk_min_size_gb", fallback=10.0)

    local_log_dir = project_path(paths.get("local_log_dir", "cloudlab/.generated/logs"))
    remote_package = f"{remote_tmp_dir}/{package_file.name}"
    remote_prepare_disk = f"{remote_tmp_dir}/prepare_local_disk.py"
    prepare_disk_script = ROOT / "cloudlab" / "scripts" / "remote" / "prepare_local_disk.py"

    log(f"{node.name}: connect {node.user}@{node.host}:{node.port}")
    conn = connect(node=node, cfg=cfg, project_path=project_path)

    try:
        log(f"{node.name}: prepare remote dirs")
        conn.run(f"mkdir -p {shlex.quote(remote_tmp_dir)}")

        if prepare_local_disk:
            log(f"{node.name}: prepare /local scratch disk")
            conn.put(str(prepare_disk_script), remote=remote_prepare_disk)
            conn.run(
                prepare_local_disk_cmd(
                    script_path=remote_prepare_disk,
                    mount_point=local_mount_point,
                    min_size_gb=local_disk_min_size_gb,
                )
            )

        if clean_remote:
            conn.run(f"rm -rf {shlex.quote(remote_repo_dir)}")

        conn.run(f"mkdir -p {shlex.quote(remote_repo_dir)}")

        log(f"{node.name}: upload package")
        conn.put(str(package_file), remote=remote_package)

        log(f"{node.name}: extract package")
        conn.run(
            f"tar -xzf {shlex.quote(remote_package)} "
            f"-C {shlex.quote(remote_repo_dir)} "
            f"--strip-components=1"
        )

        log(f"{node.name}: build")
        conn.run(
            remote_build_cmd(
                remote_repo_dir=remote_repo_dir,
                remote_build_log=remote_build_log,
                skip_apt=skip_apt,
            )
        )

        log(f"{node.name}: collect log")
        local_log_dir.mkdir(parents=True, exist_ok=True)
        conn.get(
            remote=remote_build_log,
            local=str(local_log_dir / f"{node.name}-build.log"),
        )

        log(f"{node.name}: done")

    finally:
        conn.close()


def main(argv: list[str] | None = None) -> DeployResult:
    args = parse_args(argv)
    config_path = args.config.expanduser().resolve()
    cfg = read_config(config_path)

    nodes_file = project_path(cfg["paths"].get("nodes_file"))
    package_file = project_path(cfg["paths"].get("package_file"))

    if not package_file.exists():
        raise FileNotFoundError(
            f"Package not found: {package_file}\n"
            "Run: python cloudlab/scripts/entrypoints/package.py first."
        )

    nodes = read_nodes(nodes_file)

    parallel = cfg.getboolean("deploy", "parallel", fallback=True)
    max_workers_raw = cfg.get("deploy", "max_workers", fallback="").strip()
    max_workers = int(max_workers_raw) if max_workers_raw else len(nodes)

    log(f"loaded {len(nodes)} node(s)")
    log(f"package: {package_file}")

    run_on_nodes(
        nodes=nodes,
        action_name="deploy",
        parallel=parallel,
        max_workers=max_workers,
        task=lambda node: deploy_node(node, cfg, package_file),
        log=log,
    )
    log("all nodes deployed successfully")
    return DeployResult(config_path=config_path, nodes=nodes, package_file=package_file)


def print_result(result: DeployResult) -> None:
    log(f"deployed nodes: {len(result.nodes)}")
    log(f"package: {result.package_file}")


def cli(argv: list[str] | None = None) -> int:
    print_result(main(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
