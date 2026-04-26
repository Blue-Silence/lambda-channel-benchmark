#!/usr/bin/env python3
"""
Check whether the recorded CloudLab experiment still looks deployable.

This script is intentionally read-only. It checks:

  1. the local nodes.ini inventory,
  2. optional CloudLab Portal experiment status,
  3. DNS resolution for each node,
  4. TCP + SSH banner readiness on each node's SSH port.

Portal "ready" is not enough for deployment. The deployment scripts need SSH to
return an SSH protocol banner on port 22.
"""

from __future__ import annotations

import argparse
import configparser
import ipaddress
import json
import socket
import sys
from pathlib import Path
from typing import Any

ENTRYPOINT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = ENTRYPOINT_DIR.parent
CLOUDLAB_DIR = SCRIPTS_DIR.parent
PROJECT_ROOT = CLOUDLAB_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR / "lib"))
sys.path.insert(0, str(ENTRYPOINT_DIR))

from nodes import Node, read_nodes

import refresh_nodes


DEFAULT_CLOUDLAB_CONFIG = CLOUDLAB_DIR / ".config" / "cloudlab.ini"
DEFAULT_ALLOCATE_CONFIG = CLOUDLAB_DIR / ".config" / "allocate.ini"
FAKE_IP_NET = ipaddress.ip_network("198.18.0.0/15")
TERMINAL_FAILURE_STATUSES = {
    "canceled",
    "cancelled",
    "failed",
    "terminated",
    "terminating",
}


def log(message: str) -> None:
    print(f"[check-ready] {message}", flush=True)


def project_path(value: str) -> Path:
    value = value.strip()
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (PROJECT_ROOT / path).resolve()


def read_config(path: Path) -> configparser.ConfigParser:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


def read_experiment_fields(nodes_file: Path) -> dict[str, str]:
    cfg = configparser.ConfigParser()
    cfg.read(nodes_file)
    if not cfg.has_section("experiment"):
        return {}
    return {key: value for key, value in cfg["experiment"].items()}


def summarize_status_fields(obj: Any) -> list[tuple[str, Any]]:
    matches: list[tuple[str, Any]] = []

    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = f"{path}.{key}" if path else str(key)
                lowered = str(key).lower()
                if lowered in {"state", "status", "phase", "ready", "reason"}:
                    matches.append((child_path, child))
                walk(child, child_path)
        elif isinstance(value, list):
            for idx, child in enumerate(value):
                walk(child, f"{path}[{idx}]")

    walk(obj, "")
    return matches


def experiment_status(obj: Any) -> str:
    if isinstance(obj, dict):
        value = obj.get("status") or obj.get("state")
        if value is not None:
            return str(value).strip().lower()
    return ""


def resolve_host(host: str) -> list[str]:
    addrs: set[str] = set()
    for family, _, _, _, sockaddr in socket.getaddrinfo(host, None):
        if family in (socket.AF_INET, socket.AF_INET6):
            addrs.add(sockaddr[0])
    return sorted(addrs)


def is_fake_ip(address: str) -> bool:
    try:
        return ipaddress.ip_address(address) in FAKE_IP_NET
    except ValueError:
        return False


def check_ssh_banner(node: Node, timeout: float) -> tuple[bool, str]:
    try:
        with socket.create_connection((node.host, node.port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            try:
                banner = sock.recv(256)
            except socket.timeout:
                return False, "tcp-open-but-no-ssh-banner"
    except ConnectionRefusedError:
        return False, "connection-refused"
    except socket.timeout:
        return False, "connect-timeout"
    except OSError as exc:
        return False, f"connect-error: {exc}"

    text = banner.decode("utf-8", errors="replace").strip()
    if text.startswith("SSH-"):
        return True, text
    if not text:
        return False, "empty-banner"
    return False, f"non-ssh-banner: {text[:80]}"


def check_portal(
    *,
    allocate_config: Path,
    experiment_id: str | None,
) -> bool:
    if not allocate_config.exists():
        log(f"portal: skipped; missing allocate config {allocate_config}")
        return True

    cfg = refresh_nodes.read_config(allocate_config)
    settings = refresh_nodes.load_settings(cfg, experiment_id_override=experiment_id)

    if settings["portal_token"] is None:
        log("portal: skipped; no token found in allocate config or PORTAL_TOKEN")
        return True

    log(f"portal: querying experiment {settings['experiment_id']}")
    try:
        exp = refresh_nodes.get_experiment(
            experiment_id=settings["experiment_id"],
            portal_cli=settings["portal_cli"],
            portal_url=settings["portal_url"],
            portal_token=settings["portal_token"],
        )
    except Exception as exc:
        log(f"portal: NOT READY ({exc})")
        log(
            "portal: the local nodes.ini may be stale; refresh nodes or allocate "
            "a new experiment before deploy"
        )
        return False

    debug_path = CLOUDLAB_DIR / ".generated" / "portal_ready_get.json"
    debug_path.parent.mkdir(parents=True, exist_ok=True)
    debug_path.write_text(json.dumps(exp, indent=2, sort_keys=True) + "\n")
    log(f"portal: wrote debug json {debug_path}")

    status_fields = summarize_status_fields(exp)
    status = experiment_status(exp)
    if status in TERMINAL_FAILURE_STATUSES:
        log(f"portal: NOT READY terminal status={status}")
        return False

    if not status_fields:
        log("portal: no obvious status/state fields found in response")
        return True

    log("portal: status-like fields:")
    for key, value in status_fields[:20]:
        log(f"  {key} = {value!r}")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check whether recorded CloudLab nodes are ready for deploy."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CLOUDLAB_CONFIG,
        help=f"cloudlab config file, default: {DEFAULT_CLOUDLAB_CONFIG}",
    )
    parser.add_argument(
        "--allocate-config",
        type=Path,
        default=DEFAULT_ALLOCATE_CONFIG,
        help=f"allocate config file, default: {DEFAULT_ALLOCATE_CONFIG}",
    )
    parser.add_argument(
        "--experiment-id",
        default=None,
        help="Portal experiment id/name; defaults to nodes.ini then allocate.ini",
    )
    parser.add_argument(
        "--skip-portal",
        action="store_true",
        help="skip CloudLab Portal query and only check local nodes + SSH",
    )
    parser.add_argument(
        "--ssh-timeout",
        type=float,
        default=8.0,
        help="seconds for TCP connect and SSH banner read",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cloudlab_config = args.config.expanduser().resolve()
    allocate_config = args.allocate_config.expanduser().resolve()

    cfg = read_config(cloudlab_config)
    nodes_file = project_path(cfg.get("paths", "nodes_file"))
    nodes = read_nodes(nodes_file)
    exp_fields = read_experiment_fields(nodes_file)

    experiment_id = (
        args.experiment_id
        or exp_fields.get("portal_experiment_id")
        or exp_fields.get("name")
        or None
    )

    log(f"nodes file: {nodes_file}")
    if exp_fields:
        log(f"experiment: {exp_fields.get('name', '<unknown>')}")
        if exp_fields.get("portal_experiment_id"):
            log(f"portal experiment id: {exp_fields['portal_experiment_id']}")

    all_ready = True
    if not args.skip_portal:
        all_ready = check_portal(
            allocate_config=allocate_config,
            experiment_id=experiment_id,
        )

    for node in nodes:
        log(f"node {node.name}: {node.user}@{node.host}:{node.port}")
        try:
            addresses = resolve_host(node.host)
        except OSError as exc:
            all_ready = False
            log(f"  dns: FAILED: {exc}")
            continue

        if not addresses:
            all_ready = False
            log("  dns: FAILED: no addresses")
            continue

        log(f"  dns: {', '.join(addresses)}")
        fake_addresses = [addr for addr in addresses if is_fake_ip(addr)]
        if fake_addresses:
            all_ready = False
            log(
                "  dns: WARNING fake-ip address detected; configure proxy/DNS DIRECT "
                f"for CloudLab ({', '.join(fake_addresses)})"
            )

        ok, detail = check_ssh_banner(node, args.ssh_timeout)
        if ok:
            log(f"  ssh: READY ({detail})")
        else:
            all_ready = False
            log(f"  ssh: NOT READY ({detail})")

    if all_ready:
        log("ready: all recorded nodes returned SSH banners")
        return 0

    log("not ready: fix Portal/node/SSH readiness before deploy")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
