from __future__ import annotations

import configparser
import re
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WorkflowOutputPaths:
    stamp: str
    profile_name: str
    workflow_name: str
    csv_output: Path
    log_dir: Path
    proxy_log_dir: Path
    remote_log_dir: Path


def project_path(root: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else (root / path).resolve()


def sanitize_path_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip())
    cleaned = cleaned.strip("-._")
    return cleaned or "unknown"


def timestamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def read_config(path: str | Path) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


def results_dir(root: Path, cloudlab_config: str | Path) -> Path:
    cfg = read_config(cloudlab_config)
    configured = "cloudlab/results"
    if cfg.has_section("paths"):
        configured = cfg["paths"].get("results_dir", configured)
    return project_path(root, configured)


def nodes_file(root: Path, cloudlab_config: str | Path) -> Path:
    cfg = read_config(cloudlab_config)
    return project_path(root, cfg["paths"].get("nodes_file"))


def experiment_fields(root: Path, cloudlab_config: str | Path) -> dict[str, str]:
    path = nodes_file(root, cloudlab_config)
    if not path.exists():
        return {}
    cfg = read_config(path)
    if not cfg.has_section("experiment"):
        return {}
    return {key: value for key, value in cfg["experiment"].items()}


def allocate_profile_name(allocate_config: str | Path | None) -> str:
    if not allocate_config:
        return ""
    path = Path(allocate_config)
    if not path.exists():
        return ""
    cfg = read_config(path)
    if not cfg.has_section("profile"):
        return ""
    return cfg["profile"].get("name", "").strip()


def remote_profile_name(
    *,
    root: Path,
    cloudlab_config: str | Path,
    allocate_config: str | Path | None = None,
) -> str:
    fields = experiment_fields(root, cloudlab_config)
    value = fields.get("profile_name", "").strip()
    if value:
        return sanitize_path_part(value)
    topology = fields.get("topology", "").strip()
    if topology and topology != "profile":
        return sanitize_path_part(topology)
    value = allocate_profile_name(allocate_config)
    if value:
        return sanitize_path_part(value)
    if topology:
        return sanitize_path_part(topology)
    return "unknown-profile"


def workflow_outputs(
    *,
    root: Path,
    cloudlab_config: str | Path,
    allocate_config: str | Path | None,
    workflow_name: str,
    csv_prefix: str,
    stamp: str,
    csv_output: str | Path | None = None,
) -> WorkflowOutputPaths:
    profile = remote_profile_name(
        root=root,
        cloudlab_config=cloudlab_config,
        allocate_config=allocate_config,
    )
    base = results_dir(root, cloudlab_config) / "workflow" / profile
    workflow_dir = base / workflow_name
    log_dir = base / "logs" / f"{workflow_name}-{stamp}"
    resolved_csv = (
        project_path(root, csv_output)
        if csv_output
        else workflow_dir / f"{csv_prefix}-{stamp}.csv"
    )
    return WorkflowOutputPaths(
        stamp=stamp,
        profile_name=profile,
        workflow_name=workflow_name,
        csv_output=resolved_csv,
        log_dir=log_dir,
        proxy_log_dir=log_dir / "proxy",
        remote_log_dir=log_dir / "remote",
    )


def proxy_log_path(layout: WorkflowOutputPaths, experiment: str | Path, index: int) -> Path:
    stem = sanitize_path_part(Path(experiment).stem)
    return layout.proxy_log_dir / f"{index:02d}-{stem}.log"
