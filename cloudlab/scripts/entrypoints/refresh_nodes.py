#!/usr/bin/env python3
"""
Refresh cloudlab/.generated/nodes.ini from an existing CloudLab experiment.

This script does not allocate resources. It reads an experiment manifest through
portal-cli, extracts node hostnames, and rewrites the local nodes.ini used by
deploy.py.
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

ENTRYPOINT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = ENTRYPOINT_DIR.parent
CLOUDLAB_DIR = SCRIPTS_DIR.parent
PROJECT_ROOT = CLOUDLAB_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR / "lib"))

from nodes import Node, write_nodes


DEFAULT_CONFIG = CLOUDLAB_DIR / ".config" / "allocate.ini"
DEFAULT_PORTAL_URL = "https://www.cloudlab.us"


def log(message: str) -> None:
    print(f"[refresh-nodes] {message}", flush=True)


def read_config(path: Path) -> configparser.ConfigParser:
    if not path.exists():
        raise FileNotFoundError(
            f"config file not found: {path}\n"
            f"Copy cloudlab/examples/allocate.ini to {path} and edit it."
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


def optional_project_path(value: str) -> Path | None:
    value = (value or "").strip()

    if not value:
        return None

    return project_path(value)


def read_token_file(path: Path | None) -> str | None:
    if path is None:
        return None

    if not path.exists():
        return None

    token = path.read_text().strip()
    return token or None


def parse_json_output(text: str) -> Any:
    text = text.strip()

    if not text:
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"(\{.*\}|\[.*\])", text, flags=re.DOTALL)
    if not match:
        raise ValueError(f"portal-cli did not return JSON:\n{text}")

    return json.loads(match.group(1))


def run_portal(
    args: list[str],
    *,
    portal_cli: str,
    portal_url: str,
    portal_token: str | None,
) -> Any:
    cmd = [portal_cli]

    if portal_url:
        cmd.extend(["--portal-url", portal_url])

    cmd.extend(["--output", "json"])
    cmd.extend(args)

    log("run: " + " ".join(cmd))

    env = os.environ.copy()
    if portal_token is not None:
        env["PORTAL_TOKEN"] = portal_token

    try:
        result = subprocess.run(
            cmd,
            env=env,
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

    return parse_json_output(result.stdout)


def dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    log(f"wrote debug json: {path}")


def collect_strings(obj: Any) -> list[str]:
    strings: list[str] = []

    if isinstance(obj, str):
        strings.append(obj)
    elif isinstance(obj, dict):
        for value in obj.values():
            strings.extend(collect_strings(value))
    elif isinstance(obj, list):
        for item in obj:
            strings.extend(collect_strings(item))

    return strings


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def parse_nodes_from_manifest_xml(xml_text: str, ssh_user: str) -> list[Node]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    nodes: list[Node] = []

    for elem in root.iter():
        if local_name(elem.tag) != "node":
            continue

        node_name = (
            elem.attrib.get("client_id")
            or elem.attrib.get("client-id")
            or elem.attrib.get("component_id")
            or elem.attrib.get("component-id")
        )

        if not node_name:
            continue

        hostname: str | None = None

        for child in elem.iter():
            if local_name(child.tag) != "host":
                continue

            hostname = (
                child.attrib.get("name")
                or child.attrib.get("hostname")
                or child.attrib.get("fqdn")
            )

            if hostname:
                break

        if hostname:
            nodes.append(Node(name=node_name, host=hostname, user=ssh_user, port=22))

    return nodes


def parse_nodes_from_manifest_json(manifest: Any, ssh_user: str) -> list[Node]:
    all_nodes: list[Node] = []
    seen: set[str] = set()

    for text in collect_strings(manifest):
        if "<node" not in text or "<host" not in text:
            continue

        for node in parse_nodes_from_manifest_xml(text, ssh_user):
            if node.name in seen:
                continue

            seen.add(node.name)
            all_nodes.append(node)

    return all_nodes


def get_experiment(
    *,
    experiment_id: str,
    portal_cli: str,
    portal_url: str,
    portal_token: str | None,
) -> Any:
    return run_portal(
        [
            "experiment",
            "get",
            "--experiment-id",
            experiment_id,
        ],
        portal_cli=portal_cli,
        portal_url=portal_url,
        portal_token=portal_token,
    )


def get_manifests(
    *,
    experiment_id: str,
    portal_cli: str,
    portal_url: str,
    portal_token: str | None,
) -> Any:
    return run_portal(
        [
            "experiment",
            "manifests",
            "get",
            "--experiment-id",
            experiment_id,
        ],
        portal_cli=portal_cli,
        portal_url=portal_url,
        portal_token=portal_token,
    )


def optional_config_value(
    cfg: configparser.ConfigParser,
    section: str,
    option: str,
    fallback: str = "",
) -> str:
    if not cfg.has_section(section):
        return fallback
    return cfg.get(section, option, fallback=fallback).strip()


def load_settings(
    cfg: configparser.ConfigParser,
    *,
    experiment_id_override: str | None,
) -> dict[str, Any]:
    nodes_file = project_path(cfg.get("paths", "nodes_file"))

    portal_cli = cfg.get("portal", "portal_cli", fallback="portal-cli").strip()
    portal_url = cfg.get("portal", "portal_url", fallback=DEFAULT_PORTAL_URL).strip()
    portal_url = portal_url or DEFAULT_PORTAL_URL

    token_file = optional_project_path(
        cfg.get("portal", "token_file", fallback="cloudlab/.secrets/cloudlab.jwt")
    )
    portal_token = read_token_file(token_file) or os.environ.get("PORTAL_TOKEN")

    experiment = cfg.get("experiment", "name").strip()
    project = cfg.get("experiment", "project", fallback="").strip()
    ssh_user = cfg.get("experiment", "ssh_user").strip()

    configured_experiment_id = cfg.get("experiment", "experiment_id", fallback="").strip()
    experiment_id = experiment_id_override or configured_experiment_id or experiment

    profile_name = optional_config_value(cfg, "profile", "name")
    profile_project = optional_config_value(cfg, "profile", "project", project)

    if not experiment:
        raise ValueError("[experiment] name is empty")
    if not experiment_id:
        raise ValueError("[experiment] experiment_id or name must be set")
    if not ssh_user or ssh_user == "replace-with-cloudlab-username":
        raise ValueError("[experiment] ssh_user must be set")

    extra_fields = {
        "portal_experiment_id": experiment_id,
    }

    if project:
        extra_fields["project"] = project
    if profile_name and profile_name != "replace-with-profile-name":
        extra_fields["profile_name"] = profile_name
    if profile_project:
        extra_fields["profile_project"] = profile_project

    return {
        "nodes_file": nodes_file,
        "portal_cli": portal_cli,
        "portal_url": portal_url,
        "token_file": token_file,
        "portal_token": portal_token,
        "experiment": experiment,
        "experiment_id": experiment_id,
        "ssh_user": ssh_user,
        "extra_fields": extra_fields,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh nodes.ini from an existing CloudLab experiment."
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"allocate config file, default: {DEFAULT_CONFIG}",
    )
    parser.add_argument(
        "--experiment-id",
        default=None,
        help="CloudLab experiment id; overrides [experiment] experiment_id/name",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    config_path = args.config.expanduser().resolve()
    cfg = read_config(config_path)
    settings = load_settings(cfg, experiment_id_override=args.experiment_id)

    if settings["portal_token"] is None:
        log(
            "warning: no portal token found; set [portal] token_file "
            "or PORTAL_TOKEN if portal-cli is not configured elsewhere"
        )

    log("refresh request:")
    log(f"  config        = {config_path}")
    log(f"  portal url    = {settings['portal_url']}")
    log(f"  token file    = {settings['token_file']}")
    log(f"  experiment    = {settings['experiment']}")
    log(f"  experiment id = {settings['experiment_id']}")
    log(f"  ssh user      = {settings['ssh_user']}")

    exp = get_experiment(
        experiment_id=settings["experiment_id"],
        portal_cli=settings["portal_cli"],
        portal_url=settings["portal_url"],
        portal_token=settings["portal_token"],
    )
    dump_json(CLOUDLAB_DIR / ".generated/portal_get.json", exp)

    manifest = get_manifests(
        experiment_id=settings["experiment_id"],
        portal_cli=settings["portal_cli"],
        portal_url=settings["portal_url"],
        portal_token=settings["portal_token"],
    )
    dump_json(CLOUDLAB_DIR / ".generated/portal_manifests.json", manifest)

    nodes = parse_nodes_from_manifest_json(manifest, settings["ssh_user"])
    if not nodes:
        raise RuntimeError("no nodes with hostnames found in CloudLab manifest")

    for node in nodes:
        log(f"  {node.name}: {node.user}@{node.host}:{node.port}")

    write_nodes(
        output_path=settings["nodes_file"],
        experiment=settings["experiment"],
        topology="profile",
        nodes=nodes,
        extra_experiment_fields=settings["extra_fields"],
    )

    log(f"wrote nodes for {len(nodes)} node(s)")
    log("next stage: python cloudlab/scripts/entrypoints/deploy.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
