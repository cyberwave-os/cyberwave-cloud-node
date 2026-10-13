"""Cleaning up after workloads that are no longer running.

A workload can start containers through the host docker socket. When its
process dies without tearing them down (a recreated node container, a stale
workload killed by the node), they keep running. The profile's ``cleanup``
command removes them; the node runs it at startup and before each spawn.

The node gives every workload ``CYBERWAVE_CLOUD_NODE_ID`` and
``CYBERWAVE_WORKLOAD_UUID``, which runners turn into container labels, and
gives ``cleanup`` the node id and ``CYBERWAVE_LIVE_WORKLOAD_UUIDS``. Edge-core
reads the same names (``cyberwave_edge_core.container_spec``); its tests check
they match.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Collection, Mapping
from pathlib import Path

logger = logging.getLogger(__name__)

CLOUD_NODE_ID_ENV = "CYBERWAVE_CLOUD_NODE_ID"
WORKLOAD_UUID_ENV = "CYBERWAVE_WORKLOAD_UUID"
LIVE_WORKLOADS_ENV = "CYBERWAVE_LIVE_WORKLOAD_UUIDS"
NODE_ID_FILENAME = "cloud_node_id"

# Upper bound on one cleanup run.
CLEANUP_TIMEOUT_S = 120.0


def load_cloud_node_id(state_dir: Path, env: Mapping[str, str] | None = None) -> str:
    """Return this node's id: ``CYBERWAVE_CLOUD_NODE_ID``, else a uuid persisted in *state_dir*.

    The id must survive a restart so the node still recognises the containers
    its earlier workloads labelled.
    """
    env = os.environ if env is None else env
    configured = (env.get(CLOUD_NODE_ID_ENV) or "").strip()
    if configured:
        return configured
    path = state_dir / NODE_ID_FILENAME
    try:
        existing = path.read_text().strip()
        if existing:
            return existing
    except FileNotFoundError:
        pass
    except OSError as exc:
        logger.warning("Cannot read cloud node id from %s: %s", path, exc)
    node_id = str(uuid.uuid4())
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(node_id + "\n")
    except OSError as exc:
        logger.warning("Cannot persist cloud node id to %s: %s", path, exc)
    return node_id


def cleanup_env(
    base: Mapping[str, str], node_id: str, live_workloads: Collection[str]
) -> dict[str, str]:
    """Environment for the ``cleanup`` command."""
    env = dict(base)
    env.pop(WORKLOAD_UUID_ENV, None)
    env[CLOUD_NODE_ID_ENV] = node_id
    env[LIVE_WORKLOADS_ENV] = ",".join(sorted(live_workloads))
    return env
