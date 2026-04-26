from __future__ import annotations

import configparser

from fabric import Connection

from nodes import Node


def connect(
    *,
    node: Node,
    cfg: configparser.ConfigParser,
    project_path,
) -> Connection:
    ssh_key = cfg.get("deploy", "ssh_key", fallback="").strip()
    connect_timeout = cfg.getint("deploy", "connect_timeout", fallback=30)

    kwargs = {"timeout": connect_timeout}

    if ssh_key:
        kwargs["key_filename"] = str(project_path(ssh_key))

    return Connection(
        host=node.host,
        user=node.user,
        port=node.port,
        connect_kwargs=kwargs,
    )
