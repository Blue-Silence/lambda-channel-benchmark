from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, TypeVar

from nodes import Node


T = TypeVar("T")


def run_on_nodes(
    *,
    nodes: list[Node],
    action_name: str,
    parallel: bool,
    max_workers: int,
    task: Callable[[Node], T],
    log: Callable[[str], None],
) -> None:
    if not parallel:
        for node in nodes:
            task(node)
        return

    failures: list[tuple[Node, BaseException]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(task, node): node for node in nodes}
        for future in as_completed(futures):
            node = futures[future]
            try:
                future.result()
            except Exception as exc:
                failures.append((node, exc))
                log(f"{node.name}: FAILED: {exc}")

    if failures:
        names = ", ".join(node.name for node, _ in failures)
        raise RuntimeError(f"{action_name} failed on: {names}")
