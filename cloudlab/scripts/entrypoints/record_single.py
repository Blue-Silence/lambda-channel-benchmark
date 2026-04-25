#!/usr/bin/env python3
"""
Record an already allocated single CloudLab node into nodes.ini.

This is a manual/debug helper. It does not allocate CloudLab resources.

Example:

  python cloudlab/scripts/entrypoints/record_single.py \
    --experiment lc-single-build \
    --host pcxxx.utah.cloudlab.us \
    --user myuser
"""

from __future__ import annotations

import argparse
import configparser
import sys
from pathlib import Path

ENTRYPOINT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = ENTRYPOINT_DIR.parent
CLOUDLAB_DIR = SCRIPTS_DIR.parent
PROJECT_ROOT = CLOUDLAB_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR / "lib"))

from nodes import Node, write_nodes


DEFAULT_CONFIG = CLOUDLAB_DIR / ".config" / "cloudlab.ini"


def log(message: str) -> None:
    print(f"[record-single] {message}", flush=True)


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


def parse_host_port(value: str) -> tuple[str, int]:
    if ":" not in value:
        return value, 22

    host, port_str = value.rsplit(":", 1)
    if not host:
        raise ValueError(f"invalid host:port: {value}")

    return host, int(port_str)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"config file, default: {DEFAULT_CONFIG}",
    )
    parser.add_argument(
        "--experiment",
        required=True,
        help="experiment name written to nodes.ini",
    )
    parser.add_argument(
        "--host",
        required=True,
        help="CloudLab host, optionally host:port",
    )
    parser.add_argument(
        "--user",
        required=True,
        help="SSH username",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="SSH port; overrides host:port",
    )
    parser.add_argument(
        "--node-name",
        default="node-0",
        help="node name written to nodes.ini",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    cfg = read_config(args.config.expanduser().resolve())
    nodes_file = project_path(cfg.get("paths", "nodes_file"))

    host, parsed_port = parse_host_port(args.host)
    port = args.port or parsed_port

    node = Node(
        name=args.node_name,
        host=host,
        user=args.user,
        port=port,
    )

    write_nodes(
        output_path=nodes_file,
        experiment=args.experiment,
        topology="manual_single_node",
        nodes=[node],
    )

    log(f"recorded node: {node.name} {node.user}@{node.host}:{node.port}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
