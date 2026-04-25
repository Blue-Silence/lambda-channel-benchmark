#!/usr/bin/env python3
"""
Common helpers for reading and writing CloudLab node lists.

The node file is an INI file, normally:

  cloudlab/.generated/nodes.ini

Example:

  [experiment]
  name = lc-test
  topology = profile
  created_at_unix = 1777111111
  portal_experiment_id = ...
  project = SimBricks
  profile_name = my-profile
  profile_project = SimBricks

  [node.node-0]
  name = node-0
  host = pcxxx.utah.cloudlab.us
  user = myuser
  port = 22
"""

from __future__ import annotations

import configparser
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Node:
    name: str
    host: str
    user: str
    port: int = 22


def write_nodes(
    *,
    output_path: Path,
    experiment: str,
    topology: str,
    nodes: list[Node],
    extra_experiment_fields: dict[str, str] | None = None,
) -> None:
    if not nodes:
        raise ValueError("nodes must not be empty")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    cfg = configparser.ConfigParser()

    experiment_fields = {
        "name": experiment,
        "topology": topology,
        "created_at_unix": str(int(time.time())),
    }

    if extra_experiment_fields:
        for key, value in extra_experiment_fields.items():
            experiment_fields[key] = str(value)

    cfg["experiment"] = experiment_fields

    for node in nodes:
        cfg[f"node.{node.name}"] = {
            "name": node.name,
            "host": node.host,
            "user": node.user,
            "port": str(node.port),
        }

    with output_path.open("w") as f:
        cfg.write(f)

    print(f"[nodes] wrote {output_path}", flush=True)


def read_nodes(nodes_file: Path) -> list[Node]:
    if not nodes_file.exists():
        raise FileNotFoundError(
            f"nodes file not found: {nodes_file}\n"
            "Use cloudlab/scripts/entrypoints/refresh_nodes.py for an existing experiment, "
            "cloudlab/scripts/entrypoints/allocate_profile.py for a new experiment, "
            "or cloudlab/scripts/entrypoints/record_single.py for manual/debug mode."
        )

    cfg = configparser.ConfigParser()
    cfg.read(nodes_file)

    nodes: list[Node] = []

    for section in cfg.sections():
        if not section.startswith("node."):
            continue

        default_name = section.removeprefix("node.")
        name = cfg.get(section, "name", fallback=default_name)
        host = cfg.get(section, "host", fallback="").strip()
        user = cfg.get(section, "user", fallback="").strip()
        port = cfg.getint(section, "port", fallback=22)

        if not host:
            raise ValueError(f"[{section}] missing host in {nodes_file}")

        if not user:
            raise ValueError(f"[{section}] missing user in {nodes_file}")

        nodes.append(Node(name=name, host=host, user=user, port=port))

    if not nodes:
        raise ValueError(f"no [node.*] sections found in {nodes_file}")

    return nodes
