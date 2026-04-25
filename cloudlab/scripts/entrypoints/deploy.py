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

import configparser
import shlex
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ENTRYPOINT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = ENTRYPOINT_DIR.parent
CLOUDLAB_DIR = SCRIPTS_DIR.parent
ROOT = CLOUDLAB_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR / "lib"))

from nodes import Node, read_nodes
from ssh import connect


CONFIG_FILE = CLOUDLAB_DIR / ".config" / "cloudlab.ini"


def log(message: str) -> None:
    print(f"[deploy] {message}", flush=True)


def project_path(value: str) -> Path:
    path = Path(value.strip()).expanduser()
    return path if path.is_absolute() else (ROOT / path).resolve()


def read_config() -> configparser.ConfigParser:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"Missing config file: {CONFIG_FILE}\n"
            "Copy cloudlab/examples/cloudlab.ini to cloudlab/.config/cloudlab.ini first."
        )

    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)
    return cfg


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


def deploy_node(node: Node, cfg: configparser.ConfigParser, package_file: Path) -> None:
    deploy = cfg["deploy"]
    paths = cfg["paths"]

    remote_tmp_dir = deploy.get("remote_tmp_dir", "/tmp/cloudlab-deploy")
    remote_repo_dir = deploy.get("remote_repo_dir", "/local/cloudlab-workspace")
    remote_build_log = deploy.get("remote_build_log", "/local/cloudlab-build.log")

    clean_remote = deploy.getboolean("clean_remote", fallback=True)
    skip_apt = deploy.getboolean("skip_apt", fallback=False)

    local_log_dir = project_path(paths.get("local_log_dir", "cloudlab/.generated/logs"))
    remote_package = f"{remote_tmp_dir}/{package_file.name}"

    log(f"{node.name}: connect {node.user}@{node.host}:{node.port}")
    conn = connect(node=node, cfg=cfg, project_path=project_path)

    try:
        log(f"{node.name}: prepare remote dirs")
        conn.run(f"mkdir -p {shlex.quote(remote_tmp_dir)}")

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


def main() -> None:
    cfg = read_config()

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

    if not parallel:
        for node in nodes:
            deploy_node(node, cfg, package_file)
        return

    failures: list[tuple[Node, BaseException]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(deploy_node, node, cfg, package_file): node for node in nodes}

        for future in as_completed(futures):
            node = futures[future]

            try:
                future.result()
            except Exception as exc:
                failures.append((node, exc))
                log(f"{node.name}: FAILED: {exc}")

    if failures:
        names = ", ".join(node.name for node, _ in failures)
        raise RuntimeError(f"deploy failed on: {names}")

    log("all nodes deployed successfully")


if __name__ == "__main__":
    main()
