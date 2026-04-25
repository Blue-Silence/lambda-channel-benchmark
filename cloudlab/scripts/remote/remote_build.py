#!/usr/bin/env python3
"""
Remote build script.

This file is inserted into the source bundle by
cloudlab/scripts/entrypoints/package.py and runs on each CloudLab node after
the bundle has been extracted.

It does not clone anything. It only builds the unpacked workspace.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path


DEFAULT_REPO_DIR = Path("/local/cloudlab-workspace")
DEFAULT_BINARY = Path("target/release/lc-bench")


def log(message: str) -> None:
    print(f"[remote-build] {message}", flush=True)


def run(
    cmd: list[str],
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> None:
    where = f" cwd={cwd}" if cwd else ""
    printable = " ".join(str(arg) for arg in cmd)
    log(f"run:{where} {printable}")

    subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        check=True,
    )


def run_shell(
    command: str,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> None:
    where = f" cwd={cwd}" if cwd else ""
    log(f"run shell:{where} {command}")

    subprocess.run(
        ["bash", "-lc", command],
        cwd=str(cwd) if cwd else None,
        env=env,
        check=True,
    )


def ensure_basic_packages(skip_apt: bool) -> None:
    if skip_apt:
        log("skip apt package installation")
        return

    if shutil.which("apt-get") is None:
        log("apt-get not found; skip package installation")
        return

    run_shell(
        "sudo apt-get update && "
        "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y "
        "curl ca-certificates build-essential pkg-config libssl-dev"
    )


def rust_env() -> dict[str, str]:
    env = os.environ.copy()
    cargo_bin = str(Path.home() / ".cargo" / "bin")
    env["PATH"] = f"{cargo_bin}:{env.get('PATH', '')}"
    return env


def ensure_rust() -> dict[str, str]:
    env = rust_env()

    if shutil.which("cargo", path=env["PATH"]) is None:
        log("cargo not found; installing Rust with rustup")
        run_shell(
            "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs "
            "| sh -s -- -y"
        )
        env = rust_env()
    else:
        log("cargo already available")

    run(["cargo", "--version"], env=env)
    run(["rustc", "--version"], env=env)

    return env


def check_layout(repo_dir: Path) -> None:
    if not (repo_dir / "Cargo.toml").exists():
        raise FileNotFoundError(f"missing Cargo.toml in {repo_dir}")

    p2p_impl = repo_dir / ".p2p-data-transfer" / "src" / "rust_impl"
    if not p2p_impl.exists():
        raise FileNotFoundError(f"missing private dependency: {p2p_impl}")


def build(repo_dir: Path, env: dict[str, str]) -> None:
    check_layout(repo_dir)

    manifest = repo_dir / "cloudlab_package_manifest.ini"
    if manifest.exists():
        log(f"package manifest: {manifest}")
        print(manifest.read_text(), flush=True)

    run(["cargo", "build", "--release"], cwd=repo_dir, env=env)

    binary = repo_dir / DEFAULT_BINARY
    if not binary.exists():
        raise FileNotFoundError(f"expected binary not found: {binary}")

    run(["ls", "-lh", str(binary)])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--repo-dir",
        type=Path,
        default=DEFAULT_REPO_DIR,
        help=f"repo directory, default: {DEFAULT_REPO_DIR}",
    )

    parser.add_argument(
        "--skip-apt",
        action="store_true",
        help="skip apt package installation",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_dir = args.repo_dir.resolve()

    log(f"hostname: {os.uname().nodename}")
    log(f"repo dir: {repo_dir}")

    ensure_basic_packages(skip_apt=args.skip_apt)

    env = ensure_rust()

    build(repo_dir, env)

    log("build finished successfully")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        log(f"command failed with exit code {exc.returncode}")
        raise SystemExit(exc.returncode)
    except Exception as exc:
        log(f"ERROR: {exc}")
        raise
