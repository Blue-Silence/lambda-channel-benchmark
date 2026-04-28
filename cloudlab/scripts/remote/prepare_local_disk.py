#!/usr/bin/env python3
"""
Prepare a CloudLab scratch disk for benchmark state under /local.

The script is intentionally conservative. It only formats a whole disk when
lsblk reports that the disk has no partitions, no filesystem, no mountpoint, and
is not read-only. If a previously prepared LC_BENCH_LOCAL filesystem exists, it
mounts that instead.
"""

from __future__ import annotations

import argparse
import grp
import json
import os
import pwd
import shutil
import subprocess
from pathlib import Path
from typing import Any


DEFAULT_MOUNT_POINT = Path("/local")
DEFAULT_LABEL = "LC_BENCH_LOCAL"


def log(message: str) -> None:
    print(f"[prepare-local-disk] {message}", flush=True)


def run(cmd: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess[str]:
    log("run: " + " ".join(cmd))
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )


def command_exists(name: str) -> bool:
    return shutil.which(name) is not None


def mountpoint_is_exact(path: Path) -> bool:
    return subprocess.run(
        ["findmnt", "--mountpoint", str(path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def lsblk_json() -> dict[str, Any]:
    columns = "NAME,PATH,TYPE,SIZE,FSTYPE,LABEL,MOUNTPOINTS,RO"
    result = run(["lsblk", "--json", "--bytes", "--output", columns], capture=True)
    return json.loads(result.stdout or "{}")


def mountpoints(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if isinstance(value, str):
        return [value] if value else []
    return []


def iter_block_devices(devices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for device in devices:
        flattened.append(device)
        children = device.get("children") or []
        if isinstance(children, list):
            flattened.extend(iter_block_devices(children))
    return flattened


def find_labeled_device(devices: list[dict[str, Any]], label: str) -> str | None:
    for device in iter_block_devices(devices):
        path = str(device.get("path") or "")
        if not path:
            continue
        if device.get("label") == label and not mountpoints(device.get("mountpoints")):
            return path
    return None


def parse_size(device: dict[str, Any]) -> int:
    value = device.get("size") or 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def is_blank_disk(device: dict[str, Any], min_size_bytes: int) -> bool:
    if device.get("type") != "disk":
        return False
    if str(device.get("ro") or "0") not in {"0", "False", "false"}:
        return False
    if parse_size(device) < min_size_bytes:
        return False
    if device.get("children"):
        return False
    if device.get("fstype"):
        return False
    if mountpoints(device.get("mountpoints")):
        return False
    return bool(device.get("path"))


def find_blank_disk(devices: list[dict[str, Any]], min_size_bytes: int) -> str | None:
    candidates = [device for device in devices if is_blank_disk(device, min_size_bytes)]
    if not candidates:
        return None
    candidates.sort(key=parse_size, reverse=True)
    return str(candidates[0]["path"])


def chown_mountpoint(path: Path) -> None:
    user = pwd.getpwuid(os.getuid()).pw_name
    group = grp.getgrgid(os.getgid()).gr_name
    run(["sudo", "chown", f"{user}:{group}", str(path)])
    run(["sudo", "chmod", "775", str(path)])


def prepare_mountpoint(path: Path) -> None:
    run(["sudo", "mkdir", "-p", str(path)])
    if any(path.iterdir()):
        log(f"{path} is not empty; mounting over existing contents")


def mount_device(device: str, mount_point: Path) -> None:
    prepare_mountpoint(mount_point)
    run(["sudo", "mount", device, str(mount_point)])
    chown_mountpoint(mount_point)
    run(["df", "-h", str(mount_point)])


def prepare_local_disk(args: argparse.Namespace) -> None:
    mount_point = args.mount_point.resolve()

    if not command_exists("lsblk") or not command_exists("findmnt"):
        log("lsblk/findmnt unavailable; skip local disk preparation")
        return

    if mountpoint_is_exact(mount_point):
        log(f"{mount_point} is already a mountpoint")
        chown_mountpoint(mount_point)
        return

    devices = lsblk_json().get("blockdevices") or []
    if not isinstance(devices, list):
        raise RuntimeError("unexpected lsblk JSON: blockdevices is not a list")

    labeled = find_labeled_device(devices, args.label)
    if labeled:
        log(f"mounting existing prepared filesystem {labeled} at {mount_point}")
        mount_device(labeled, mount_point)
        return

    min_size_bytes = int(args.min_size_gb * 1024 * 1024 * 1024)
    blank = find_blank_disk(devices, min_size_bytes)
    if not blank:
        log("no blank unmounted whole disk found; leaving existing /local layout unchanged")
        return

    log(f"formatting blank disk {blank} as ext4 label={args.label}")
    run(["sudo", "mkfs.ext4", "-F", "-L", args.label, blank])
    mount_device(blank, mount_point)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mount-point", type=Path, default=DEFAULT_MOUNT_POINT)
    parser.add_argument("--label", default=DEFAULT_LABEL)
    parser.add_argument("--min-size-gb", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    prepare_local_disk(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
