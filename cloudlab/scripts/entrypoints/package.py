#!/usr/bin/env python3
"""
Stage 0: create a self-contained CloudLab source bundle.

This script runs locally.

It clones:
  1. benchmark repo
  2. private p2p-data-transfer repo

Alternatively, [package] benchmark_source = local copies the current benchmark
working tree into the package so uncommitted local changes can be deployed.

Then it places the private repo at:

  workspace/.p2p-data-transfer

It also injects:

  cloudlab/scripts/remote/remote_build.py

Finally it creates:

  cloudlab/.generated/package/source-bundle.tar.gz

CloudLab nodes do not need GitHub access or private deploy keys.
"""

from __future__ import annotations

import argparse
import configparser
import shlex
import shutil
import subprocess
import tarfile
import time
from pathlib import Path


ENTRYPOINT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = ENTRYPOINT_DIR.parent
CLOUDLAB_DIR = SCRIPTS_DIR.parent
PROJECT_ROOT = CLOUDLAB_DIR.parent
DEFAULT_CONFIG = CLOUDLAB_DIR / ".config" / "cloudlab.ini"

PACKAGE_ROOT_NAME = "workspace"


def log(message: str) -> None:
    print(f"[package] {message}", flush=True)


def read_config(path: Path) -> configparser.ConfigParser:
    if not path.exists():
        raise FileNotFoundError(
            f"config file not found: {path}\n"
            f"Copy cloudlab/examples/cloudlab.ini to {path} and edit it."
        )

    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


def project_path(value: str) -> Path:
    value = value.strip()
    path = Path(value).expanduser()

    if path.is_absolute():
        return path.resolve()

    return (PROJECT_ROOT / path).resolve()


def run(cmd: list[str], cwd: Path | None = None) -> str:
    printable = " ".join(shlex.quote(x) for x in cmd)
    where = f" cwd={cwd}" if cwd else ""
    log(f"run:{where} {printable}")

    try:
        result = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            print(exc.stdout, end="")
        raise

    if result.stdout.strip():
        print(result.stdout, end="")

    return result.stdout.strip()


def git_head(repo_dir: Path) -> str:
    return run(["git", "rev-parse", "HEAD"], cwd=repo_dir).strip()


def clone_and_checkout(repo_url: str, ref: str, target_dir: Path) -> None:
    if not target_dir.exists():
        run(["git", "clone", "--recursive", repo_url, str(target_dir)])
    else:
        if not (target_dir / ".git").exists():
            raise RuntimeError(f"target exists but is not a git repo: {target_dir}")

    run(["git", "fetch", "--all", "--tags", "--prune"], cwd=target_dir)

    origin_ref = f"origin/{ref}"
    has_origin_ref = subprocess.run(
        ["git", "rev-parse", "--verify", origin_ref],
        cwd=str(target_dir),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode == 0

    if has_origin_ref:
        run(["git", "checkout", "-B", ref, origin_ref], cwd=target_dir)
        run(["git", "reset", "--hard", origin_ref], cwd=target_dir)
    else:
        run(["git", "checkout", "--force", ref], cwd=target_dir)

    run(["git", "submodule", "update", "--init", "--recursive"], cwd=target_dir)


def git_status_short(repo_dir: Path) -> str:
    return run(["git", "status", "--short"], cwd=repo_dir).strip()


def copy_local_benchmark_source(source_dir: Path, target_dir: Path) -> None:
    source_dir = source_dir.resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    log(f"copying local benchmark working tree: {source_dir}")

    for path in sorted(source_dir.rglob("*")):
        relative = path.relative_to(source_dir)
        if should_exclude(relative):
            continue

        target = target_dir / relative
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif path.is_file() or path.is_symlink():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target, follow_symlinks=False)


def copy_remote_build_script(workspace_dir: Path) -> None:
    source = SCRIPTS_DIR / "remote" / "remote_build.py"
    if not source.exists():
        raise FileNotFoundError(f"missing remote build script: {source}")

    target = workspace_dir / "cloudlab" / "scripts" / "remote" / "remote_build.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)

    log(f"injected remote build script: {target}")


def write_manifest(
    *,
    manifest_path: Path,
    manifest_inside_workspace: Path,
    benchmark_repo: str,
    benchmark_ref: str,
    benchmark_commit: str,
    p2p_repo: str,
    p2p_ref: str,
    p2p_commit: str,
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    manifest = configparser.ConfigParser()

    manifest["package"] = {
        "created_at_unix": str(int(time.time())),
        "package_root": PACKAGE_ROOT_NAME,
    }

    manifest["benchmark"] = {
        "repo": benchmark_repo,
        "ref": benchmark_ref,
        "commit": benchmark_commit,
    }

    manifest["p2p"] = {
        "repo": p2p_repo,
        "ref": p2p_ref,
        "commit": p2p_commit,
    }

    with manifest_path.open("w") as f:
        manifest.write(f)

    with manifest_inside_workspace.open("w") as f:
        manifest.write(f)

    log(f"wrote manifest: {manifest_path}")
    log(f"wrote workspace manifest: {manifest_inside_workspace}")


def should_exclude(relative_path: Path, *, exclude_private_dependency: bool = True) -> bool:
    excluded_names = {
        ".git",
        ".codex",
        ".config",
        ".allocate.ini",
        ".cloudlab.ini",
        ".generated",
        ".secrets",
        ".venv",
        "results",
        "target",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".DS_Store",
    }

    if exclude_private_dependency:
        excluded_names.add(".p2p-data-transfer")

    return any(part in excluded_names for part in relative_path.parts)


def add_tree_to_tar(tar: tarfile.TarFile, source_dir: Path) -> None:
    source_dir = source_dir.resolve()

    for path in sorted(source_dir.rglob("*")):
        relative = path.relative_to(source_dir)

        if should_exclude(relative, exclude_private_dependency=False):
            continue

        arcname = Path(PACKAGE_ROOT_NAME) / relative
        tar.add(path, arcname=str(arcname), recursive=False)


def create_tarball(source_dir: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    log(f"creating package: {output_path}")

    with tarfile.open(output_path, "w:gz") as tar:
        root_info = tarfile.TarInfo(PACKAGE_ROOT_NAME)
        root_info.type = tarfile.DIRTYPE
        root_info.mode = 0o755
        root_info.mtime = int(time.time())
        tar.addfile(root_info)

        add_tree_to_tar(tar, source_dir)

    size_mb = output_path.stat().st_size / 1024 / 1024
    log(f"package created: {output_path} ({size_mb:.2f} MiB)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"config file, default: {DEFAULT_CONFIG}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = args.config.expanduser().resolve()
    cfg = read_config(config_path)

    benchmark_source = cfg.get("package", "benchmark_source", fallback="git").strip().lower()
    benchmark_repo = cfg.get("package", "benchmark_repo")
    benchmark_ref = cfg.get("package", "benchmark_ref", fallback="main")

    p2p_repo = cfg.get("package", "p2p_repo")
    p2p_ref = cfg.get("package", "p2p_ref")

    work_dir = project_path(cfg.get("package", "work_dir"))
    clean_work = cfg.getboolean("package", "clean_work", fallback=True)

    package_file = project_path(cfg.get("paths", "package_file"))
    package_manifest = project_path(cfg.get("paths", "package_manifest"))

    workspace_dir = work_dir / PACKAGE_ROOT_NAME
    p2p_dir = workspace_dir / ".p2p-data-transfer"

    if clean_work and work_dir.exists():
        log(f"removing work dir: {work_dir}")
        shutil.rmtree(work_dir)

    work_dir.mkdir(parents=True, exist_ok=True)

    if benchmark_source == "git":
        log("cloning benchmark repo")
        clone_and_checkout(benchmark_repo, benchmark_ref, workspace_dir)
        benchmark_commit = git_head(workspace_dir)
    elif benchmark_source == "local":
        copy_local_benchmark_source(PROJECT_ROOT, workspace_dir)
        benchmark_repo = str(PROJECT_ROOT)
        benchmark_ref = "local-working-tree"
        benchmark_commit = git_head(PROJECT_ROOT)
        dirty = git_status_short(PROJECT_ROOT)
        if dirty:
            log("local benchmark working tree has uncommitted changes included in package")
    else:
        raise ValueError("unsupported [package] benchmark_source; expected 'git' or 'local'")

    if p2p_dir.exists():
        log(f"removing existing p2p dependency: {p2p_dir}")
        shutil.rmtree(p2p_dir)

    log("cloning private p2p repo")
    clone_and_checkout(p2p_repo, p2p_ref, p2p_dir)

    copy_remote_build_script(workspace_dir)

    p2p_commit = git_head(p2p_dir)

    write_manifest(
        manifest_path=package_manifest,
        manifest_inside_workspace=workspace_dir / "cloudlab_package_manifest.ini",
        benchmark_repo=benchmark_repo,
        benchmark_ref=benchmark_ref,
        benchmark_commit=benchmark_commit,
        p2p_repo=p2p_repo,
        p2p_ref=p2p_ref,
        p2p_commit=p2p_commit,
    )

    create_tarball(workspace_dir, package_file)

    log("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
