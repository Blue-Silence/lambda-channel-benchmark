#!/usr/bin/env python3
"""
Garbage-collect AWS resources created by lc-bench experiments.

This script intentionally uses the AWS CLI instead of the Rust benchmark path.
Cleanup is resource hygiene, not part of the measured datapoint. Keep it local,
parallel, prefix-scoped, and easy to rerun after interrupted CloudLab runs.
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


ENTRYPOINT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = ENTRYPOINT_DIR.parent
CLOUDLAB_DIR = SCRIPTS_DIR.parent
ROOT = CLOUDLAB_DIR.parent

CONFIG_FILE = CLOUDLAB_DIR / ".config" / "cloudlab.ini"


@dataclass(frozen=True)
class GcResult:
    dry_run: bool
    s3_mode: str
    bucket_prefixes: list[str]
    table_prefixes: list[str]
    buckets: list[str]
    tables: list[str]
    failures: int


def log(message: str) -> None:
    print(f"[gc-aws] {message}", flush=True)


def project_path(value: str) -> Path:
    path = Path(value.strip()).expanduser()
    return path if path.is_absolute() else (ROOT / path).resolve()


def read_config(path: Path) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.optionxform = str
    if path.exists():
        cfg.read(path)
    return cfg


def read_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f"AWS env file does not exist: {path}")
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
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        env[name] = value
    return env


def aws_env(args: argparse.Namespace) -> dict[str, str]:
    env = dict(os.environ)
    cfg = read_config(args.config.expanduser().resolve())

    env_file = args.aws_env_file
    if env_file is None and cfg.has_section("runtime"):
        configured = cfg["runtime"].get("aws_env_file", "").strip()
        if configured:
            env_file = project_path(configured)

    if env_file is not None:
        env.update(read_env_file(env_file.expanduser().resolve()))

    if args.region:
        env["AWS_REGION"] = args.region
        env["AWS_DEFAULT_REGION"] = args.region
    return env


def run_json(command: list[str], env: dict[str, str]) -> object:
    proc = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        cwd=str(ROOT),
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(command)}\n{proc.stderr.strip()}"
        )
    return json.loads(proc.stdout or "null")


def run_quiet(command: list[str], env: dict[str, str]) -> tuple[int, str]:
    proc = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        cwd=str(ROOT),
        check=False,
    )
    return proc.returncode, proc.stdout.strip()


def has_prefix(name: str, prefixes: list[str]) -> bool:
    return any(name.startswith(prefix) for prefix in prefixes)


def list_buckets(env: dict[str, str], prefixes: list[str]) -> list[str]:
    data = run_json(
        ["aws", "s3api", "list-buckets", "--query", "Buckets[].Name", "--output", "json"],
        env,
    )
    if not isinstance(data, list):
        raise RuntimeError("unexpected list-buckets output")
    return sorted(name for name in data if isinstance(name, str) and has_prefix(name, prefixes))


def list_tables(env: dict[str, str], prefixes: list[str]) -> list[str]:
    data = run_json(["aws", "dynamodb", "list-tables", "--output", "json"], env)
    names = data.get("TableNames") if isinstance(data, dict) else None
    if not isinstance(names, list):
        raise RuntimeError("unexpected list-tables output")
    return sorted(name for name in names if isinstance(name, str) and has_prefix(name, prefixes))


def delete_bucket(bucket: str, mode: str, env: dict[str, str], dry_run: bool) -> tuple[str, bool, str]:
    if dry_run:
        return bucket, True, f"dry-run: would delete S3 bucket ({mode})"
    if mode == "empty-only":
        rc, output = run_quiet(["aws", "s3api", "delete-bucket", "--bucket", bucket], env)
        if rc == 0:
            return bucket, True, "deleted empty bucket"
        if "BucketNotEmpty" in output:
            return bucket, False, "skipped non-empty bucket"
        return bucket, False, output
    if mode == "force":
        rc, output = run_quiet(["aws", "s3", "rb", f"s3://{bucket}", "--force"], env)
        return bucket, rc == 0, output or "deleted bucket with --force"
    raise ValueError(f"unknown S3 cleanup mode: {mode}")


def delete_table(table: str, env: dict[str, str], dry_run: bool) -> tuple[str, bool, str]:
    if dry_run:
        return table, True, "dry-run: would delete DynamoDB table"
    rc, output = run_quiet(["aws", "dynamodb", "delete-table", "--table-name", table], env)
    return table, rc == 0, output or "delete-table submitted"


def run_parallel(
    label: str,
    names: list[str],
    workers: int,
    task,
) -> int:
    if not names:
        log(f"{label}: no matches")
        return 0

    failures = 0
    log(f"{label}: {len(names)} match(es)")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(task, name): name for name in names}
        for future in as_completed(futures):
            name, ok, message = future.result()
            status = "ok" if ok else "skip" if "skipped" in message else "FAILED"
            log(f"{label}: {status}: {name}: {message}")
            if not ok and status != "skip":
                failures += 1
    return failures


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=CONFIG_FILE,
        help=f"cloudlab config file used to discover runtime.aws_env_file, default: {CONFIG_FILE}",
    )
    parser.add_argument(
        "--aws-env-file",
        type=Path,
        default=None,
        help="optional env file with AWS_REGION/AWS credentials; values are never printed",
    )
    parser.add_argument("--region", default=None, help="override AWS region for AWS CLI commands")
    parser.add_argument(
        "--bucket-prefix",
        action="append",
        default=[],
        help="S3 bucket prefix to delete; may be repeated",
    )
    parser.add_argument(
        "--table-prefix",
        action="append",
        default=[],
        help="DynamoDB table prefix to delete; may be repeated",
    )
    parser.add_argument(
        "--s3-mode",
        choices=["empty-only", "force"],
        default="empty-only",
        help="empty-only deletes only already-empty buckets; force runs 'aws s3 rb --force'",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="parallel deletion workers, default: 8",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="actually delete resources; without this flag the script is dry-run only",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> GcResult:
    args = parse_args(argv)
    if args.workers <= 0:
        raise ValueError("--workers must be greater than zero")
    if not args.bucket_prefix and not args.table_prefix:
        raise ValueError("provide at least one --bucket-prefix or --table-prefix")

    env = aws_env(args)
    dry_run = not args.yes
    if dry_run:
        log("dry-run mode; pass --yes to delete")
    if args.region:
        log(f"region: {args.region}")

    failures = 0
    buckets: list[str] = []
    tables: list[str] = []
    if args.bucket_prefix:
        buckets = list_buckets(env, args.bucket_prefix)
        failures += run_parallel(
            "s3",
            buckets,
            args.workers,
            lambda bucket: delete_bucket(bucket, args.s3_mode, env, dry_run),
        )
    if args.table_prefix:
        tables = list_tables(env, args.table_prefix)
        failures += run_parallel(
            "dynamodb",
            tables,
            args.workers,
            lambda table: delete_table(table, env, dry_run),
        )
    return GcResult(
        dry_run=dry_run,
        s3_mode=args.s3_mode,
        bucket_prefixes=args.bucket_prefix,
        table_prefixes=args.table_prefix,
        buckets=buckets,
        tables=tables,
        failures=failures,
    )


def print_result(result: GcResult) -> None:
    log(f"dry run: {result.dry_run}")
    log(f"s3 mode: {result.s3_mode}")
    log(f"s3 buckets matched: {len(result.buckets)}")
    log(f"dynamodb tables matched: {len(result.tables)}")
    log(f"failures: {result.failures}")


def cli(argv: list[str] | None = None) -> int:
    result = main(argv)
    print_result(result)
    return 1 if result.failures else 0


if __name__ == "__main__":
    raise SystemExit(cli())
