"""Controller-host hygiene tests.

Covers two fixes for the recurring RL controller deploy-timeout symptom:

1. A busy / mis-configured node reports the incoming workload as ``failed`` with
   an actionable ``failure_detail`` instead of silently letting the backend time
   the deploy out.
2. On startup, ``_recover_local_workloads`` reconciles each recovered live PID
   against the backend and kills the process group of any stale orphan (terminal,
   missing, or bound to a different instance) while reattaching valid ones.

The dependency stubs below mirror ``test_mqtt_reconnect.py`` — heavy optional
deps are stubbed into ``sys.modules`` before importing the cloud node.
"""

# ruff: noqa: E402

import asyncio
import json
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

# psutil is used by process-liveness checks; a lightweight stub is enough since
# the tests patch _is_process_alive / _kill_stale_process_group directly.
if "psutil" not in sys.modules:
    psutil_stub = SimpleNamespace(
        Process=Mock(),
        NoSuchProcess=type("NoSuchProcess", (Exception,), {}),
        AccessDenied=type("AccessDenied", (Exception,), {}),
        TimeoutExpired=type("TimeoutExpired", (Exception,), {}),
        STATUS_ZOMBIE="zombie",
    )
    sys.modules["psutil"] = psutil_stub

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

from cyberwave_cloud_node.cloud_node import (  # noqa: E402
    ActiveWorkload,
    CloudNode,
)
from cyberwave_cloud_node.config import CloudNodeConfig  # noqa: E402


def _make_node(tmp_dir: str, **config_kwargs) -> CloudNode:
    with patch(
        "cyberwave_cloud_node.cloud_node.Path.home",
        return_value=Path(tmp_dir),
    ):
        return CloudNode(
            config=CloudNodeConfig(**config_kwargs),
            client=Mock(),
            working_dir=Path(tmp_dir),
        )


def _dummy_active_workload(pid: int, tmp_dir: str) -> ActiveWorkload:
    return ActiveWorkload(
        pid=pid,
        request_id="old-req",
        workload_type="inference",
        started_at=time.time(),
        command="python controller_deploy.py",
        stdout_file=Path(tmp_dir) / "o.log",
        stderr_file=Path(tmp_dir) / "e.log",
    )


class StartupFailureReportingTests(unittest.TestCase):
    """A start rejection must mark the workload failed with an actionable reason."""

    def test_busy_node_reports_startup_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = _make_node(tmp_dir, inference="python controller_deploy.py {body}")
            node._mqtt_client = AsyncMock()
            # An existing active workload makes the node report itself busy.
            node._active_workloads[123] = _dummy_active_workload(123, tmp_dir)

            asyncio.run(node._handle_inference({"workload_uuid": "wl-2"}, request_id="req-2"))

        node._mqtt_client.update_workload_status.assert_awaited_once()
        kwargs = node._mqtt_client.update_workload_status.await_args.kwargs
        self.assertEqual(kwargs["workload_uuid"], "wl-2")
        self.assertEqual(kwargs["status"], "failed")
        self.assertIn("busy", kwargs["additional_data"]["error"].lower())
        # Mirror into failure_detail so operator diagnostics keep the reason too.
        self.assertEqual(
            kwargs["additional_data"]["failure_detail"],
            kwargs["additional_data"]["error"],
        )
        # The busy workload must not be spawned.
        self.assertNotIn("wl-2", {w.workload_uuid for w in node._active_workloads.values()})

    def test_unconfigured_inference_reports_startup_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = _make_node(tmp_dir)  # no inference command configured
            node._mqtt_client = AsyncMock()

            asyncio.run(node._handle_inference({"workload_uuid": "wl-3"}, request_id="req-3"))

        node._mqtt_client.update_workload_status.assert_awaited_once()
        kwargs = node._mqtt_client.update_workload_status.await_args.kwargs
        self.assertEqual(kwargs["workload_uuid"], "wl-3")
        self.assertEqual(kwargs["status"], "failed")
        self.assertIn("not configured", kwargs["additional_data"]["error"].lower())

    def test_missing_workload_uuid_skips_backend_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = _make_node(tmp_dir, inference="python controller_deploy.py {body}")
            node._mqtt_client = AsyncMock()
            node._active_workloads[123] = _dummy_active_workload(123, tmp_dir)

            # No workload_uuid: still rejects, but there is nothing to fail on the
            # backend, so no status update is published.
            asyncio.run(node._handle_inference({}, request_id="req-x"))

        node._mqtt_client.update_workload_status.assert_not_awaited()


class RecoverLocalWorkloadsHygieneTests(unittest.TestCase):
    """Recovered live PIDs are reconciled against the backend before reattaching."""

    def _write_state(self, tmp_dir: str, workload_uuid: str, pid: int = 424242) -> None:
        state_dir = Path(tmp_dir) / ".cyberwave"
        state_dir.mkdir(parents=True, exist_ok=True)
        (state_dir / "active_workloads.json").write_text(
            json.dumps(
                {
                    str(pid): {
                        "pid": pid,
                        "request_id": "old-req",
                        "workload_type": "inference",
                        "started_at": time.time(),
                        "command": "python controller_deploy.py",
                        "stdout_file": str(state_dir / "o.log"),
                        "stderr_file": str(state_dir / "e.log"),
                        "params": {},
                        "workload_uuid": workload_uuid,
                    }
                }
            )
        )

    def _run_recovery(self, node: CloudNode, tmp_dir: str) -> None:
        with patch(
            "cyberwave_cloud_node.cloud_node.Path.home",
            return_value=Path(tmp_dir),
        ):
            asyncio.run(node._recover_local_workloads())

    def test_terminal_backend_workload_is_killed_not_reattached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._write_state(tmp_dir, "wl-terminal", pid=424242)
            node = _make_node(tmp_dir)
            node._is_process_alive = Mock(return_value=True)
            node._kill_stale_process_group = AsyncMock()
            node.client.get_workload = Mock(return_value={"status": "cancelled"})

            self._run_recovery(node, tmp_dir)

            node._kill_stale_process_group.assert_awaited_once_with(424242)
            self.assertEqual(node._active_workloads, {})
            # State file rewritten without the killed orphan.
            saved = json.loads((Path(tmp_dir) / ".cyberwave" / "active_workloads.json").read_text())
            self.assertEqual(saved, {})

    def test_missing_backend_workload_is_killed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._write_state(tmp_dir, "wl-gone", pid=424243)
            node = _make_node(tmp_dir)
            node._is_process_alive = Mock(return_value=True)
            node._kill_stale_process_group = AsyncMock()
            node.client.get_workload = Mock(return_value=None)  # HTTP 404

            self._run_recovery(node, tmp_dir)

            node._kill_stale_process_group.assert_awaited_once_with(424243)
            self.assertEqual(node._active_workloads, {})

    def test_mismatched_instance_workload_is_killed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._write_state(tmp_dir, "wl-other", pid=424244)
            node = _make_node(tmp_dir)
            node.instance_uuid = "this-instance"
            node._is_process_alive = Mock(return_value=True)
            node._kill_stale_process_group = AsyncMock()
            node.client.get_workload = Mock(
                return_value={"status": "running", "instance_uuid": "some-other-instance"}
            )

            self._run_recovery(node, tmp_dir)

            node._kill_stale_process_group.assert_awaited_once_with(424244)
            self.assertEqual(node._active_workloads, {})

    def test_valid_running_workload_is_reattached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._write_state(tmp_dir, "wl-live", pid=424245)
            node = _make_node(tmp_dir)
            node.instance_uuid = "this-instance"
            node._is_process_alive = Mock(return_value=True)
            node._kill_stale_process_group = AsyncMock()
            node.client.get_workload = Mock(
                return_value={"status": "running", "instance_uuid": "this-instance"}
            )

            self._run_recovery(node, tmp_dir)

            node._kill_stale_process_group.assert_not_awaited()
            self.assertIn(424245, node._active_workloads)
            self.assertEqual(node._active_workloads[424245].workload_uuid, "wl-live")

    def test_backend_unreachable_reattaches_to_be_safe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._write_state(tmp_dir, "wl-unknown", pid=424246)
            node = _make_node(tmp_dir)
            node._is_process_alive = Mock(return_value=True)
            node._kill_stale_process_group = AsyncMock()
            node.client.get_workload = Mock(side_effect=RuntimeError("network down"))

            self._run_recovery(node, tmp_dir)

            # An inconclusive backend check must never kill a live process.
            node._kill_stale_process_group.assert_not_awaited()
            self.assertIn(424246, node._active_workloads)

    def test_workload_without_uuid_is_reattached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            # State entry with no workload_uuid -> nothing to reconcile against.
            state_dir = Path(tmp_dir) / ".cyberwave"
            state_dir.mkdir(parents=True, exist_ok=True)
            (state_dir / "active_workloads.json").write_text(
                json.dumps(
                    {
                        "424247": {
                            "pid": 424247,
                            "request_id": "old-req",
                            "workload_type": "inference",
                            "started_at": time.time(),
                            "command": "python controller_deploy.py",
                            "stdout_file": str(state_dir / "o.log"),
                            "stderr_file": str(state_dir / "e.log"),
                            "params": {},
                            "workload_uuid": None,
                        }
                    }
                )
            )
            node = _make_node(tmp_dir)
            node._is_process_alive = Mock(return_value=True)
            node._kill_stale_process_group = AsyncMock()
            node.client.get_workload = Mock()

            self._run_recovery(node, tmp_dir)

            node.client.get_workload.assert_not_called()
            node._kill_stale_process_group.assert_not_awaited()
            self.assertIn(424247, node._active_workloads)


class ConsoleMirrorTests(unittest.TestCase):
    """Workload output is mirrored to the node console with a source prefix.

    The workload runs as a detached subprocess whose fds point at log files, so
    ``docker logs`` only shows the supervisor. Mirroring surfaces the workload
    output there too, tagged per line so it stays distinguishable, while the
    per-file stdout/stderr separation is preserved.
    """

    def _mirror(self, node, workload, text, log_type, **kwargs):
        from io import StringIO

        target = "stderr" if log_type == "stderr" else "stdout"
        buf = StringIO()
        with patch(f"cyberwave_cloud_node.cloud_node.sys.{target}", buf):
            node._mirror_workload_output(workload, text, log_type, **kwargs)
        return buf.getvalue()

    def test_complete_lines_are_prefixed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = _make_node(tmp_dir)
            wl = _dummy_active_workload(1, tmp_dir)
            wl.workload_uuid = "wl-abc"
            out = self._mirror(node, wl, "[rltask] step 1\n[rltask] step 2\n", "stdout")
        self.assertEqual(
            out,
            "[workload wl-abc stdout] [rltask] step 1\n"
            "[workload wl-abc stdout] [rltask] step 2\n",
        )

    def test_partial_line_is_held_then_completed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = _make_node(tmp_dir)
            wl = _dummy_active_workload(1, tmp_dir)
            wl.workload_uuid = "wl-abc"
            # A tail read lands mid-line: only the completed line is emitted now.
            first = self._mirror(node, wl, "done line\npartial", "stdout")
            self.assertEqual(first, "[workload wl-abc stdout] done line\n")
            # The rest of the line arrives next tick and is emitted whole.
            second = self._mirror(node, wl, " rest\n", "stdout")
            self.assertEqual(second, "[workload wl-abc stdout] partial rest\n")

    def test_flush_partial_emits_unterminated_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = _make_node(tmp_dir)
            wl = _dummy_active_workload(1, tmp_dir)
            wl.workload_uuid = "wl-abc"
            # Crash output with no trailing newline must still reach the console.
            out = self._mirror(
                node, wl, "Traceback (most recent call last):", "stderr", flush_partial=True
            )
        self.assertEqual(
            out, "[workload wl-abc stderr] Traceback (most recent call last):\n"
        )

    def test_stdout_and_stderr_use_separate_residuals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = _make_node(tmp_dir)
            wl = _dummy_active_workload(1, tmp_dir)
            wl.workload_uuid = "wl-abc"
            self._mirror(node, wl, "out-partial", "stdout")
            self._mirror(node, wl, "err-partial", "stderr")
            # Residuals must not bleed across streams.
            self.assertEqual(wl.stdout_residual, "out-partial")
            self.assertEqual(wl.stderr_residual, "err-partial")


if __name__ == "__main__":
    unittest.main()
