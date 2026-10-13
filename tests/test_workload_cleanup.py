"""The node runs the profile's ``cleanup`` command for workloads that are not live.

The dependency stubs mirror ``test_controller_hygiene.py``.
"""

# ruff: noqa: E402

import asyncio
import json
import logging
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

try:
    import paho.mqtt.client as mqtt  # pyright: ignore[reportMissingImports]
except ModuleNotFoundError:
    mqtt = SimpleNamespace(MQTT_ERR_SUCCESS=0, Client=Mock())
    sys.modules.setdefault("paho", Mock())
    sys.modules.setdefault("paho.mqtt", Mock())
    sys.modules.setdefault("paho.mqtt.client", mqtt)
    sys.modules.setdefault("paho.mqtt.packettypes", SimpleNamespace(PacketTypes=Mock()))
    sys.modules.setdefault("paho.mqtt.properties", SimpleNamespace(Properties=Mock()))

if "psutil" not in sys.modules:
    sys.modules["psutil"] = SimpleNamespace(
        Process=Mock(),
        NoSuchProcess=type("NoSuchProcess", (Exception,), {}),
        AccessDenied=type("AccessDenied", (Exception,), {}),
        TimeoutExpired=type("TimeoutExpired", (Exception,), {}),
        STATUS_ZOMBIE="zombie",
    )

sys.modules.setdefault("yaml", Mock())
mock_dotenv = Mock()
mock_dotenv.load_dotenv = Mock()
sys.modules.setdefault("dotenv", mock_dotenv)
mock_httpx = Mock()
mock_httpx.Client = Mock()
mock_httpx.AsyncClient = Mock()
mock_httpx.RequestError = Exception
mock_httpx.TimeoutException = Exception
mock_httpx.Response = Mock
sys.modules.setdefault("httpx", mock_httpx)

from cyberwave_cloud_node import workload_cleanup as wc
from cyberwave_cloud_node.cloud_node import CloudNode
from cyberwave_cloud_node.config import CloudNodeConfig

NODE = "node-a"


class CloudNodeIdTests(unittest.TestCase):
    def test_env_wins(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            node_id = wc.load_cloud_node_id(Path(tmp), {wc.CLOUD_NODE_ID_ENV: " fixed "})
            self.assertEqual(node_id, "fixed")
            self.assertFalse((Path(tmp) / wc.NODE_ID_FILENAME).exists())

    def test_persisted_id_survives_a_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "state"
            first = wc.load_cloud_node_id(state, {})
            self.assertTrue(first)
            self.assertEqual(wc.load_cloud_node_id(state, {}), first)

    def test_an_unwritable_state_dir_still_yields_an_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            blocker = Path(tmp) / "file"
            blocker.write_text("")
            with self.assertLogs(wc.logger, logging.WARNING):
                self.assertTrue(wc.load_cloud_node_id(blocker / "state", {}))


class CleanupEnvTests(unittest.TestCase):
    def test_carries_the_node_and_the_live_workloads(self) -> None:
        env = wc.cleanup_env({"PATH": "/bin", wc.WORKLOAD_UUID_ENV: "stale"}, NODE, {"b", "a"})
        self.assertEqual(env[wc.CLOUD_NODE_ID_ENV], NODE)
        self.assertEqual(env[wc.LIVE_WORKLOADS_ENV], "a,b")
        self.assertNotIn(wc.WORKLOAD_UUID_ENV, env)
        self.assertEqual(env["PATH"], "/bin")


def _make_node(tmp_dir: str, cleanup: str | None) -> CloudNode:
    return CloudNode(
        config=CloudNodeConfig(simulate="python run_sim.py --params {body}", cleanup=cleanup),
        client=Mock(),
        working_dir=Path(tmp_dir),
    )


def _state_entry(pid: int, workload_uuid: str, tmp_dir: str) -> dict:
    return {
        "pid": pid,
        "request_id": "old-req",
        "workload_type": "simulate",
        "started_at": time.time(),
        "command": "python run_sim.py",
        "stdout_file": str(Path(tmp_dir) / "o.log"),
        "stderr_file": str(Path(tmp_dir) / "e.log"),
        "params": {},
        "workload_uuid": workload_uuid,
    }


# Records what the cleanup command saw, so the tests run the real subprocess.
_RECORD = 'printf "%s|%s" "$CYBERWAVE_CLOUD_NODE_ID" "$CYBERWAVE_LIVE_WORKLOAD_UUIDS" > seen'


class RecoveryCleanupTests(unittest.TestCase):
    """``_recover_local_workloads`` runs cleanup after reattaching."""

    def _recover(self, tmp_dir: str, state: dict | None, alive: set[int], cleanup: str | None):
        env = {"CYBERWAVE_EDGE_CONFIG_DIR": tmp_dir, wc.CLOUD_NODE_ID_ENV: NODE}
        with patch.dict(os.environ, env):
            if state is not None:
                (Path(tmp_dir) / "active_workloads.json").write_text(json.dumps(state))
            node = _make_node(tmp_dir, cleanup)
            node._is_process_alive = lambda pid: pid in alive  # type: ignore[method-assign]
            node._classify_recovered_workload = Mock(return_value=None)  # type: ignore[method-assign]
            node._handle_orphaned_completion = AsyncMock()  # type: ignore[method-assign]
            asyncio.run(node._recover_local_workloads())
        return node

    def test_cleanup_gets_the_node_id_and_the_reattached_workloads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = {
                "101": _state_entry(101, "wl-live", tmp),
                "102": _state_entry(102, "wl-dead", tmp),
            }
            node = self._recover(tmp, state, alive={101}, cleanup=_RECORD)
            self.assertEqual(list(node._active_workloads), [101])
            self.assertEqual((Path(tmp) / "seen").read_text(), f"{NODE}|wl-live")

    def test_runs_without_a_state_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._recover(tmp, None, alive=set(), cleanup=_RECORD)
            self.assertEqual((Path(tmp) / "seen").read_text(), f"{NODE}|")

    def test_no_cleanup_command_is_a_no_op(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch("asyncio.create_subprocess_shell") as spawn:
                self._recover(tmp, None, alive=set(), cleanup=None)
            spawn.assert_not_called()

    def test_a_failing_cleanup_never_fails_startup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertLogs("cyberwave_cloud_node.cloud_node", logging.WARNING) as logs:
                self._recover(tmp, None, alive=set(), cleanup="echo boom; exit 3")
            self.assertIn("boom", "\n".join(logs.output))

    def test_a_hanging_cleanup_is_killed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with (
                patch("cyberwave_cloud_node.cloud_node.CLEANUP_TIMEOUT_S", 0.5),
                self.assertLogs("cyberwave_cloud_node.cloud_node", logging.WARNING),
            ):
                started = time.monotonic()
                self._recover(tmp, None, alive=set(), cleanup="sleep 30")
            self.assertLess(time.monotonic() - started, 10)


class StartCleanupTests(unittest.TestCase):
    def test_cleanup_runs_before_the_spawn(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"CYBERWAVE_EDGE_CONFIG_DIR": tmp}):
                node = _make_node(tmp, cleanup="true")
                order: list[str] = []
                node._is_node_busy_after_self_heal = AsyncMock(return_value=False)  # type: ignore[method-assign]
                node._run_workload_cleanup = AsyncMock(  # type: ignore[method-assign]
                    side_effect=lambda: order.append("cleanup")
                )
                node._spawn_workload_process = AsyncMock(  # type: ignore[method-assign]
                    side_effect=lambda *a: order.append("spawn")
                )
                asyncio.run(
                    node._start_workload_if_free(
                        "simulate", {"workload_uuid": "wl-1"}, "req-1", "wl-1", stop_hint=""
                    )
                )
            self.assertEqual(order, ["cleanup", "spawn"])


class SpawnEnvTests(unittest.TestCase):
    def test_the_workload_learns_its_node_and_workload_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = {"CYBERWAVE_EDGE_CONFIG_DIR": tmp}
            with patch.dict(os.environ, env):
                os.environ.pop(wc.CLOUD_NODE_ID_ENV, None)
                os.environ.pop(wc.WORKLOAD_UUID_ENV, None)
                node = _make_node(tmp, cleanup=None)
                node._save_workload_state = AsyncMock()  # type: ignore[method-assign]
                popen = Mock(return_value=SimpleNamespace(pid=4242))
                with patch("cyberwave_cloud_node.cloud_node.subprocess.Popen", popen):
                    asyncio.run(
                        node._spawn_workload_process("simulate", {"workload_uuid": "wl-1"}, "req-1")
                    )
                child_env = popen.call_args.kwargs["env"]
                self.assertEqual(child_env[wc.CLOUD_NODE_ID_ENV], node.cloud_node_id)
                self.assertEqual(child_env[wc.WORKLOAD_UUID_ENV], "wl-1")
                persisted = (Path(tmp) / wc.NODE_ID_FILENAME).read_text().strip()
                self.assertEqual(persisted, node.cloud_node_id)


if __name__ == "__main__":
    unittest.main()
