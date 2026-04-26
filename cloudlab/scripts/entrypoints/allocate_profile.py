#!/usr/bin/env python3
"""
Instantiate an existing CloudLab profile using portal-cli.

This script is topology-agnostic:

  - The topology is defined by an existing CloudLab profile.
  - This script instantiates that profile.
  - It reads the experiment manifest.
  - It writes all discovered nodes into:

      cloudlab/.generated/nodes.ini

Default config:

      cloudlab/.config/allocate.ini

Example:

  cp cloudlab/examples/allocate.ini cloudlab/.config/allocate.ini
  vim cloudlab/.config/allocate.ini

  python cloudlab/scripts/entrypoints/allocate_profile.py

Then deploy:

  python cloudlab/scripts/entrypoints/deploy.py
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
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


@dataclass(frozen=True)
class AllocateResult:
    config_path: Path
    nodes_file: Path
    experiment: str
    experiment_id: str
    nodes: list[Node]


def log(message: str) -> None:
    print(f"[allocate-profile] {message}", flush=True)


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
    """
    portal-cli normally prints JSON. This parser is defensive because some
    environments may print warnings before the JSON payload.
    """
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
    echo_output: bool = True,
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

    if echo_output and result.stdout.strip():
        print(result.stdout, end="")

    return parse_json_output(result.stdout)


def dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    log(f"wrote debug json: {path}")


def find_key_recursive(obj: Any, candidates: set[str]) -> str | None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in candidates and isinstance(value, str) and value:
                return value

        for value in obj.values():
            found = find_key_recursive(value, candidates)
            if found:
                return found

    elif isinstance(obj, list):
        for item in obj:
            found = find_key_recursive(item, candidates)
            if found:
                return found

    return None


def extract_experiment_id(create_result: Any, experiment_name: str) -> str:
    candidates = {
        "experiment_id",
        "experimentId",
        "experimentID",
        "uuid",
        "id",
        "urn",
    }

    found = find_key_recursive(create_result, candidates)
    if found:
        return found

    return experiment_name


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
    """
    Parse GENI manifest XML.

    Expected pattern:

      <node client_id="node-0" ...>
        <host name="pcxxx.utah.cloudlab.us"/>
      </node>
    """
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

        host_name: str | None = None
        login_hostname: str | None = None

        for child in elem.iter():
            if local_name(child.tag) == "login":
                username = child.attrib.get("username")
                if username == ssh_user:
                    login_hostname = (
                        child.attrib.get("hostname")
                        or child.attrib.get("name")
                        or child.attrib.get("fqdn")
                    )
            elif local_name(child.tag) == "host":
                host_name = (
                    child.attrib.get("name")
                    or child.attrib.get("hostname")
                    or child.attrib.get("fqdn")
                )

        hostname = login_hostname or host_name
        if hostname:
            nodes.append(
                Node(
                    name=node_name,
                    host=hostname,
                    user=ssh_user,
                    port=22,
                )
            )

    return nodes


def parse_nodes_from_manifest_json(manifest: Any, ssh_user: str) -> list[Node]:
    """
    Search portal-cli JSON output for embedded GENI manifest XML, then parse all
    nodes from it.
    """
    all_nodes: list[Node] = []
    seen: set[str] = set()

    for text in collect_strings(manifest):
        if "<node" not in text or "<host" not in text:
            continue

        nodes = parse_nodes_from_manifest_xml(text, ssh_user)

        for node in nodes:
            if node.name in seen:
                continue

            seen.add(node.name)
            all_nodes.append(node)

    return all_nodes


def create_experiment(
    *,
    experiment: str,
    project: str,
    profile_name: str,
    profile_project: str,
    duration_hours: int | None,
    ssh_pubkey: Path | None,
    portal_cli: str,
    portal_url: str,
    portal_token: str | None,
) -> str:
    args = [
        "experiment",
        "create",
        "--name",
        experiment,
        "--project",
        project,
        "--profile-name",
        profile_name,
        "--profile-project",
        profile_project,
    ]

    if duration_hours is not None:
        args.extend(["--duration", str(duration_hours)])

    if ssh_pubkey is not None:
        if not ssh_pubkey.exists():
            raise FileNotFoundError(f"SSH public key not found: {ssh_pubkey}")

        args.extend(["--sshpubkey", ssh_pubkey.read_text().strip()])

    result = run_portal(
        args,
        portal_cli=portal_cli,
        portal_url=portal_url,
        portal_token=portal_token,
    )
    dump_json(CLOUDLAB_DIR / ".generated/portal_create.json", result)

    experiment_id = extract_experiment_id(result, experiment)
    log(f"experiment id: {experiment_id}")
    return experiment_id


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
        echo_output=False,
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
        echo_output=False,
    )


TERMINAL_FAILURE_STATUSES = {
    "canceled",
    "cancelled",
    "failed",
    "terminated",
    "terminating",
}


class TerminalExperimentStatus(RuntimeError):
    pass


def log_status_fields(obj: Any) -> None:
    fields: list[tuple[str, Any]] = []

    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = f"{path}.{key}" if path else str(key)
                lowered = str(key).lower()
                if lowered in {
                    "status",
                    "state",
                    "phase",
                    "ready",
                    "reason",
                    "message",
                    "error",
                    "errors",
                    "failure_reason",
                    "details",
                }:
                    fields.append((child_path, child))
                walk(child, child_path)
        elif isinstance(value, list):
            for idx, child in enumerate(value):
                walk(child, f"{path}[{idx}]")

    walk(obj, "")
    if not fields:
        log("portal returned no reason/error/message fields")
        return

    log("portal status-like fields:")
    for key, value in fields[:30]:
        log(f"  {key} = {value!r}")


def experiment_status(exp: Any) -> str:
    if isinstance(exp, dict):
        value = exp.get("status") or exp.get("state")
        if value is not None:
            return str(value).strip().lower()
    return ""


def wait_for_nodes(
    *,
    experiment_id: str,
    ssh_user: str,
    portal_cli: str,
    portal_url: str,
    portal_token: str | None,
    timeout_sec: int,
    poll_sec: int,
) -> list[Node]:
    deadline = time.time() + timeout_sec
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            exp = get_experiment(
                experiment_id=experiment_id,
                portal_cli=portal_cli,
                portal_url=portal_url,
                portal_token=portal_token,
            )
            dump_json(CLOUDLAB_DIR / ".generated/portal_get.json", exp)

            status = experiment_status(exp)
            if status:
                log(f"experiment status: {status}")
            if status in TERMINAL_FAILURE_STATUSES:
                log_status_fields(exp)
                log(
                    "leaving failed CloudLab experiment for debugging; "
                    "terminate it manually after inspecting Portal"
                )
                raise TerminalExperimentStatus(
                    f"CloudLab experiment {experiment_id} entered terminal status: {status}"
                )

            manifest = get_manifests(
                experiment_id=experiment_id,
                portal_cli=portal_cli,
                portal_url=portal_url,
                portal_token=portal_token,
            )
            dump_json(CLOUDLAB_DIR / ".generated/portal_manifests.json", manifest)

            nodes = parse_nodes_from_manifest_json(manifest, ssh_user)
            if nodes:
                log(f"found {len(nodes)} node(s) in manifest")
                for node in nodes:
                    log(f"  {node.name}: {node.user}@{node.host}:{node.port}")
                return nodes

            if status:
                log(
                    "manifest available, but no nodes with hostnames found yet; "
                    f"status={status}; retrying..."
                )
            else:
                log("manifest available, but no nodes with hostnames found yet; retrying...")

        except TerminalExperimentStatus:
            raise
        except Exception as exc:
            last_error = exc
            log(f"experiment not ready yet: {exc}")

        time.sleep(poll_sec)

    if last_error is not None:
        raise TimeoutError(
            f"timed out waiting for nodes; last error: {last_error}"
        )

    raise TimeoutError("timed out waiting for nodes")


def load_allocate_settings(cfg: configparser.ConfigParser) -> dict[str, Any]:
    nodes_file = project_path(cfg.get("paths", "nodes_file"))

    portal_cli = cfg.get("portal", "portal_cli", fallback="portal-cli").strip()
    portal_url = cfg.get("portal", "portal_url", fallback=DEFAULT_PORTAL_URL).strip()
    portal_url = portal_url or DEFAULT_PORTAL_URL

    token_file = optional_project_path(
        cfg.get("portal", "token_file", fallback="cloudlab/.secrets/cloudlab.jwt")
    )
    portal_token = read_token_file(token_file) or os.environ.get("PORTAL_TOKEN")

    experiment = cfg.get("experiment", "name").strip()
    project = cfg.get("experiment", "project").strip()
    duration_hours = cfg.getint("experiment", "duration_hours", fallback=4)
    ssh_user = cfg.get("experiment", "ssh_user").strip()
    ssh_pubkey = optional_project_path(cfg.get("experiment", "ssh_pubkey", fallback=""))

    profile_name = cfg.get("profile", "name").strip()
    profile_project = cfg.get("profile", "project", fallback=project).strip()

    timeout_sec = cfg.getint("poll", "timeout_sec", fallback=45 * 60)
    poll_sec = cfg.getint("poll", "poll_sec", fallback=30)

    if not experiment:
        raise ValueError("[experiment] name is empty")
    if not project:
        raise ValueError("[experiment] project is empty")
    if not ssh_user or ssh_user == "replace-with-cloudlab-username":
        raise ValueError("[experiment] ssh_user must be set")
    if not profile_name or profile_name == "replace-with-profile-name":
        raise ValueError("[profile] name must be set")
    if not profile_project:
        raise ValueError("[profile] project is empty")

    return {
        "nodes_file": nodes_file,
        "portal_cli": portal_cli,
        "portal_url": portal_url,
        "token_file": token_file,
        "portal_token": portal_token,
        "experiment": experiment,
        "project": project,
        "duration_hours": duration_hours,
        "ssh_user": ssh_user,
        "ssh_pubkey": ssh_pubkey,
        "profile_name": profile_name,
        "profile_project": profile_project,
        "timeout_sec": timeout_sec,
        "poll_sec": poll_sec,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Instantiate an existing CloudLab profile and write nodes.ini."
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"allocate config file, default: {DEFAULT_CONFIG}",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> AllocateResult:
    args = parse_args(argv)

    config_path = args.config.expanduser().resolve()
    cfg = read_config(config_path)
    settings = load_allocate_settings(cfg)

    if settings["portal_token"] is None:
        log(
            "warning: no portal token found; set [portal] token_file "
            "or PORTAL_TOKEN if portal-cli is not configured elsewhere"
        )

    log("allocation request:")
    log(f"  config          = {config_path}")
    log(f"  portal url      = {settings['portal_url']}")
    log(f"  token file      = {settings['token_file']}")
    log(f"  experiment      = {settings['experiment']}")
    log(f"  project         = {settings['project']}")
    log(f"  profile name    = {settings['profile_name']}")
    log(f"  profile project = {settings['profile_project']}")
    log(f"  ssh user        = {settings['ssh_user']}")
    log(f"  duration hours  = {settings['duration_hours']}")

    experiment_id = create_experiment(
        experiment=settings["experiment"],
        project=settings["project"],
        profile_name=settings["profile_name"],
        profile_project=settings["profile_project"],
        duration_hours=settings["duration_hours"],
        ssh_pubkey=settings["ssh_pubkey"],
        portal_cli=settings["portal_cli"],
        portal_url=settings["portal_url"],
        portal_token=settings["portal_token"],
    )

    nodes = wait_for_nodes(
        experiment_id=experiment_id,
        ssh_user=settings["ssh_user"],
        portal_cli=settings["portal_cli"],
        portal_url=settings["portal_url"],
        portal_token=settings["portal_token"],
        timeout_sec=settings["timeout_sec"],
        poll_sec=settings["poll_sec"],
    )

    write_nodes(
        output_path=settings["nodes_file"],
        experiment=settings["experiment"],
        topology="profile",
        nodes=nodes,
        extra_experiment_fields={
            "portal_experiment_id": experiment_id,
            "project": settings["project"],
            "profile_name": settings["profile_name"],
            "profile_project": settings["profile_project"],
        },
    )

    log(f"wrote nodes for {len(nodes)} node(s)")
    return AllocateResult(
        config_path=config_path,
        nodes_file=settings["nodes_file"],
        experiment=settings["experiment"],
        experiment_id=experiment_id,
        nodes=nodes,
    )


def print_result(result: AllocateResult) -> None:
    log(f"experiment: {result.experiment}")
    log(f"portal experiment id: {result.experiment_id}")
    log(f"nodes file: {result.nodes_file}")
    log(f"nodes: {len(result.nodes)}")
    log("next stage: python cloudlab/scripts/entrypoints/deploy.py")


def cli(argv: list[str] | None = None) -> int:
    print_result(main(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
