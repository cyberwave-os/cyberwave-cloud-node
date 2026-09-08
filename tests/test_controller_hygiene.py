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
from cyberwave_cloud_node.mqtt import MQTTError  # noqa: E402


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

    def test_config_dir_override_isolates_workload_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_dir = Path(tmp_dir) / "shared-home" / ".cyberwave"
            shared_dir.mkdir(parents=True)
            shared_state = shared_dir / "active_workloads.json"
            shared_state.write_text('{"sentinel": true}')
            isolated_dir = Path(tmp_dir) / "sim-node-state"

            with (
                patch.dict(
                    os.environ,
                    {"CYBERWAVE_EDGE_CONFIG_DIR": str(isolated_dir)},
                ),
                patch(
                    "cyberwave_cloud_node.cloud_node.Path.home",
                    return_value=Path(tmp_dir) / "shared-home",
                ),
            ):
                node = _make_node(tmp_dir)
                asyncio.run(node._save_workload_state())

            self.assertEqual(shared_state.read_text(), '{"sentinel": true}')
            self.assertEqual(
                json.loads((isolated_dir / "active_workloads.json").read_text()),
                {},
            )

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


class StaleWorkloadSelfHealTests(unittest.TestCase):
    """The node self-heals stale workloads before rejecting work or heartbeating.

    A tracked workload whose backend row is terminal/gone/reassigned is a ghost
    that would otherwise keep the node "busy" forever. The node must reconcile
    against the backend and free itself BEFORE it reports state (heartbeat) or
    rejects an incoming start for being busy.
    """

    def _busy_node(self, tmp_dir: str, workload_uuid: str | None, pid: int = 555) -> CloudNode:
        node = _make_node(tmp_dir, inference="python controller_deploy.py {body}")
        node._mqtt_client = AsyncMock()
        node.instance_uuid = "this-instance"
        workload = _dummy_active_workload(pid, tmp_dir)
        workload.workload_uuid = workload_uuid
        node._active_workloads[pid] = workload
        node._save_workload_state = AsyncMock()
        return node

    def test_stale_workload_is_freed_and_new_workload_spawns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-stale")
            node.client.get_workload = Mock(return_value={"status": "cancelled"})
            node._kill_stale_process_group = AsyncMock()
            node._is_process_alive = Mock(return_value=False)  # kill succeeded
            node._spawn_workload_process = AsyncMock()

            asyncio.run(node._handle_inference({"workload_uuid": "wl-new"}, request_id="req-n"))

            node._kill_stale_process_group.assert_awaited_once_with(555)
            self.assertEqual(node._active_workloads, {})
            node._save_workload_state.assert_awaited_once()
            # The incoming workload starts instead of being rejected.
            node._spawn_workload_process.assert_awaited_once()
            node._mqtt_client.update_workload_status.assert_not_awaited()

    def test_genuinely_busy_rejection_carries_host_busy_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-live")
            # Backend confirms the tracked workload is still valid for this node.
            node.client.get_workload = Mock(
                return_value={"status": "running", "instance_uuid": "this-instance"}
            )
            node._kill_stale_process_group = AsyncMock()
            node._spawn_workload_process = AsyncMock()

            asyncio.run(node._handle_inference({"workload_uuid": "wl-new"}, request_id="req-n"))

            node._kill_stale_process_group.assert_not_awaited()
            node._spawn_workload_process.assert_not_awaited()
            kwargs = node._mqtt_client.update_workload_status.await_args.kwargs
            self.assertEqual(kwargs["status"], "failed")
            # Machine-readable class so the backend requeues instead of
            # terminally failing the workload.
            self.assertEqual(kwargs["additional_data"]["rejection_reason"], "host_busy")
            # Identifies the rejecting host so the backend can ignore this
            # report if it is redelivered after the workload moved on.
            self.assertEqual(kwargs["additional_data"]["rejecting_instance_uuid"], "this-instance")

    def test_unkillable_stale_workload_keeps_node_truthfully_busy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-stale")
            node.client.get_workload = Mock(return_value={"status": "cancelled"})
            node._kill_stale_process_group = AsyncMock()
            node._is_process_alive = Mock(return_value=True)  # survived SIGKILL
            node._spawn_workload_process = AsyncMock()

            asyncio.run(node._handle_inference({"workload_uuid": "wl-new"}, request_id="req-n"))

            # Still tracked, still busy, incoming workload rejected.
            self.assertIn(555, node._active_workloads)
            node._save_workload_state.assert_not_awaited()
            node._spawn_workload_process.assert_not_awaited()
            kwargs = node._mqtt_client.update_workload_status.await_args.kwargs
            self.assertEqual(kwargs["status"], "failed")
            # The completion claim taken for the kill is released again, so the
            # monitor loop still completes the process when it finally exits.
            self.assertFalse(node._active_workloads[555].completion_started)

    def test_completion_is_claimed_before_the_stale_process_is_killed(self) -> None:
        """Closes the race with the monitor loop's liveness poll.

        The monitor loop checks liveness every few seconds. If it saw the killed
        process before the reconcile finished bookkeeping, it would claim the
        completion and report a status for a workload the backend already
        terminalized. The claim must therefore be taken before the kill.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-stale")
            node.client.get_workload = Mock(return_value={"status": "cancelled"})
            node._is_process_alive = Mock(return_value=False)
            workload = node._active_workloads[555]
            claimed_at_kill_time: list[bool] = []

            async def _kill(pid: int) -> None:
                claimed_at_kill_time.append(workload.completion_started)
                # The node must still count itself busy while the process dies.
                self.assertTrue(node._is_node_busy())

            node._kill_stale_process_group = _kill

            freed = asyncio.run(node._reconcile_stale_workloads())

            self.assertEqual(freed, 1)
            self.assertEqual(claimed_at_kill_time, [True])
            self.assertEqual(node._active_workloads, {})

    def test_workload_already_claimed_by_monitor_loop_is_left_alone(self) -> None:
        """A process that exited on its own belongs to the normal completion flow."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-stale")
            node.client.get_workload = Mock(return_value={"status": "cancelled"})
            node._kill_stale_process_group = AsyncMock()
            node._active_workloads[555].completion_started = True

            freed = asyncio.run(node._reconcile_stale_workloads())

            self.assertEqual(freed, 0)
            node._kill_stale_process_group.assert_not_awaited()

    def test_cancelled_self_heal_leaves_the_workload_completable(self) -> None:
        """A pass cancelled mid-kill must not leave an un-completable claim.

        The heartbeat loop time-boxes the reconcile with ``wait_for``, and the
        kill waits up to 5 s for the process group to die. A ``CancelledError``
        landing in that window used to leave ``completion_started`` set while
        the workload was still tracked: ``_claim_workload_completion`` then
        returns False forever, every later self-heal skips it, and the node
        reports itself busy with a dead PID until it restarts. Note
        ``CancelledError`` is a ``BaseException``, so the ``except Exception``
        handlers around this code never caught it.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-stale")
            node.client.get_workload = Mock(return_value={"status": "cancelled"})
            node._is_process_alive = Mock(return_value=True)
            workload = node._active_workloads[555]

            # Stand in for a process that ignores SIGTERM: the real kill waits
            # up to 5 s in asyncio.sleep, and the time box fires inside that
            # window. Stubbed rather than real so the test never signals an
            # arbitrary PID on the machine running it.
            async def _slow_kill(pid: int) -> None:
                await asyncio.sleep(3600)

            node._kill_stale_process_group = _slow_kill

            async def _cancel_mid_kill() -> None:
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(node._reconcile_stale_workloads(), timeout=0.1)

            asyncio.run(_cancel_mid_kill())

            # The claim is rolled back, so the workload is completable again.
            self.assertFalse(workload.completion_started)
            self.assertIn(555, node._active_workloads)

            # The monitor loop can now complete it and free the node.
            node._is_process_alive = Mock(return_value=False)
            node._collect_workload_output = AsyncMock(return_value=("", ""))
            node._publish_workload_result = AsyncMock()
            node._upload_workload_results = AsyncMock()
            asyncio.run(node._handle_workload_completion(workload, exit_code=0))

            self.assertFalse(node._is_node_busy())

    def test_inconclusive_backend_check_keeps_workload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-unknown")
            node.client.get_workload = Mock(side_effect=RuntimeError("network down"))
            node._kill_stale_process_group = AsyncMock()

            freed = asyncio.run(node._reconcile_stale_workloads())

            self.assertEqual(freed, 0)
            node._kill_stale_process_group.assert_not_awaited()
            self.assertIn(555, node._active_workloads)

    def test_start_path_self_heal_is_time_boxed(self) -> None:
        """The busy check runs while holding ``_start_lock``.

        Each ghost can cost a 30 s backend call plus a 5 s kill, so an unbounded
        pass here queues incoming start commands behind the lock for tens of
        seconds — long enough for the dispatch waiting on it to be treated as
        lost. It must be bounded exactly like the heartbeat's pass.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-stale")
            # Budget floor is 5.0 s; shrink it so the test stays fast.
            node._self_heal_budget_seconds = Mock(return_value=0.2)

            async def _hanging_reconcile(budget: float | None = None) -> int:
                await asyncio.sleep(3600)
                return 0

            node._reconcile_stale_workloads = _hanging_reconcile

            async def _run() -> bool:
                return await asyncio.wait_for(node._is_node_busy_after_self_heal(), timeout=5)

            # Returns the pre-cleanup answer instead of hanging on the reconcile.
            self.assertTrue(asyncio.run(_run()))

    def test_self_heal_budget_stops_picking_up_new_candidates(self) -> None:
        """A node with several ghosts must not stall its caller.

        The pass stops taking new candidates once the budget is spent and leaves
        the rest for the next pass, instead of running every ghost's backend
        call and kill back to back.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-stale-1", pid=601)
            for pid in (602, 603):
                extra = _dummy_active_workload(pid, tmp_dir)
                extra.workload_uuid = f"wl-stale-{pid}"
                node._active_workloads[pid] = extra
            node.client.get_workload = Mock(return_value={"status": "cancelled"})
            node._is_process_alive = Mock(return_value=False)
            killed: list[int] = []

            async def _kill(pid: int) -> None:
                killed.append(pid)
                # Each kill spends the whole budget, so only the first candidate
                # is examined and the rest are deferred.
                await asyncio.sleep(0.3)

            node._kill_stale_process_group = _kill

            freed = asyncio.run(node._reconcile_stale_workloads(budget=0.2))

            self.assertEqual(len(killed), 1)
            self.assertEqual(freed, 1)
            # The deferred ghosts are still tracked and completable.
            self.assertEqual(len(node._active_workloads), 2)
            for workload in node._active_workloads.values():
                self.assertFalse(workload.completion_started)

    def test_heartbeat_runs_self_heal_before_sending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-stale")
            node.config.heartbeat_interval = 0
            node._running = True
            calls: list[str] = []

            async def _reconcile(budget: float | None = None) -> int:
                calls.append("reconcile")
                return 0

            async def _send_heartbeat(**kwargs):
                calls.append("heartbeat")
                node._running = False
                return SimpleNamespace(payload={"message": "ok"})

            node._reconcile_stale_workloads = _reconcile
            node._mqtt_client.send_heartbeat = _send_heartbeat

            asyncio.run(node._heartbeat_loop())

            # State is reported only after the self-heal pass ran.
            self.assertEqual(calls, ["reconcile", "heartbeat"])

    def test_heartbeat_is_not_blocked_by_a_hanging_self_heal(self) -> None:
        """An unbounded reconcile would delay the beat past the stale window.

        The reconcile makes a synchronous backend call per workload (30 s client
        timeout). If the backend hangs, waiting for it would stop the heartbeat
        long enough for the backend to reap the instance — the exact outcome the
        heartbeat exists to prevent.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = self._busy_node(tmp_dir, "wl-stale")
            node.config.heartbeat_interval = 0
            node._running = True
            calls: list[str] = []

            async def _hanging_reconcile(budget: float | None = None) -> int:
                calls.append("reconcile")
                await asyncio.sleep(3600)
                return 0

            async def _send_heartbeat(**kwargs):
                calls.append("heartbeat")
                node._running = False
                return SimpleNamespace(payload={"message": "ok"})

            node._reconcile_stale_workloads = _hanging_reconcile
            node._mqtt_client.send_heartbeat = _send_heartbeat

            async def _run() -> None:
                # The loop must finish well inside the 3600 s the reconcile
                # would otherwise hold it for.
                await _REAL_WAIT_FOR(node._heartbeat_loop(), timeout=30)

            with patch(
                "cyberwave_cloud_node.cloud_node.asyncio.wait_for",
                new=_instant_timeout_wait_for,
            ):
                asyncio.run(_run())

            self.assertEqual(calls, ["reconcile", "heartbeat"])


_REAL_WAIT_FOR = asyncio.wait_for


async def _instant_timeout_wait_for(awaitable, timeout):
    """``asyncio.wait_for`` with the timeout collapsed to ~0.

    Keeps the heartbeat test fast without pinning a wall-clock bound: what
    matters is that the loop *has* a bound, not its exact value.
    """
    return await _REAL_WAIT_FOR(awaitable, timeout=0.01)


class FinalizingWorkloadBusyStateTests(unittest.TestCase):
    """A workload uploading its results still occupies the node.

    When a workload process exits, the node keeps working: it collects output
    and uploads the result files, which for multi-GB artifacts runs for minutes
    and owns the host's disk and uplink. Reporting the node idle during that
    window lets the backend schedule a second workload on top of the upload —
    and lets the node accept it.
    """

    def _node_with_workload(self, tmp_dir: str) -> tuple[CloudNode, ActiveWorkload]:
        node = _make_node(tmp_dir, inference="python controller_deploy.py {body}")
        node._mqtt_client = AsyncMock()
        node.instance_uuid = "this-instance"
        node._save_workload_state = AsyncMock()
        workload = _dummy_active_workload(777, tmp_dir)
        workload.workload_uuid = "wl-uploading"
        node._active_workloads[777] = workload
        return node, workload

    def test_claiming_completion_keeps_the_node_busy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node, workload = self._node_with_workload(tmp_dir)

            claimed = asyncio.run(node._claim_workload_completion(workload))

            self.assertTrue(claimed)
            # No longer a live process, but still occupying the host.
            self.assertEqual(node._active_workloads, {})
            self.assertIn(777, node._finalizing_workloads)
            self.assertTrue(node._is_node_busy())
            self.assertEqual(node._get_active_workload_count(), 1)

    def test_node_is_free_again_once_finalizing_is_released(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node, workload = self._node_with_workload(tmp_dir)

            async def _claim_then_release() -> None:
                await node._claim_workload_completion(workload)
                await node._release_finalizing_workload(workload)

            asyncio.run(_claim_then_release())

            self.assertFalse(node._is_node_busy())
            self.assertEqual(node._finalizing_workloads, {})

    def test_incoming_workload_is_rejected_while_results_upload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node, workload = self._node_with_workload(tmp_dir)
            node._spawn_workload_process = AsyncMock()
            node._reconcile_stale_workloads = AsyncMock(return_value=0)

            async def _upload_then_start() -> None:
                await node._claim_workload_completion(workload)
                await node._handle_inference({"workload_uuid": "wl-new"}, request_id="req-n")

            asyncio.run(_upload_then_start())

            node._spawn_workload_process.assert_not_awaited()
            # A finalizing workload has no live process, so there is nothing to
            # reconcile — and nothing that may be killed.
            node._reconcile_stale_workloads.assert_not_awaited()
            kwargs = node._mqtt_client.update_workload_status.await_args.kwargs
            self.assertEqual(kwargs["status"], "failed")
            # Still classified as host_busy so the backend requeues it.
            self.assertEqual(kwargs["additional_data"]["rejection_reason"], "host_busy")
            error = kwargs["additional_data"]["error"]
            # Legacy backends classify off this prefix; keep it verbatim.
            self.assertTrue(error.startswith("Controller host is busy with"))
            self.assertIn("uploading results", error)

    def test_completion_releases_the_node_even_when_it_raises(self) -> None:
        """A crashed completion must not leave the node permanently "busy"."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            node, workload = self._node_with_workload(tmp_dir)
            # First statement inside the completion body, after the claim.
            node._save_workload_state = AsyncMock(side_effect=RuntimeError("boom"))

            asyncio.run(node._handle_workload_completion(workload, exit_code=0))

            self.assertEqual(node._finalizing_workloads, {})
            self.assertFalse(node._is_node_busy())

    def test_unacknowledged_completion_preserves_evidence_but_frees_node(self) -> None:
        for error in (asyncio.TimeoutError(), MQTTError("Completion rejected while assigned")):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as tmp_dir:
                node, workload = self._node_with_workload(tmp_dir)
                node.config.upload_results = False
                node._buffer_log = AsyncMock()
                node._flush_logs = AsyncMock()
                node._mqtt_client.complete_workload.side_effect = error
                workload.stdout_file.write_text("Measured process output")
                workload.stderr_file.write_text("Diagnostic warning")
                params_file = workload.stdout_file.with_suffix(".params.json")
                params_file.write_text('{"private_input": "retained locally"}')

                asyncio.run(node._handle_workload_completion(workload, exit_code=0))

                self.assertEqual(workload.stdout_file.read_text(), "Measured process output")
                self.assertEqual(workload.stderr_file.read_text(), "Diagnostic warning")
                self.assertTrue(params_file.exists())
                self.assertFalse(node._is_node_busy())
                node._mqtt_client.complete_workload.assert_awaited_once()

    def test_missing_mqtt_preserves_completed_workload_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node, workload = self._node_with_workload(tmp_dir)
            node.config.upload_results = False
            node._mqtt_client = None
            node._buffer_log = AsyncMock()
            node._flush_logs = AsyncMock()
            workload.stdout_file.write_text("Measured process output")
            workload.stderr_file.write_text("")

            asyncio.run(node._handle_workload_completion(workload, exit_code=0))

            self.assertTrue(workload.stdout_file.exists())
            self.assertTrue(workload.stderr_file.exists())
            self.assertFalse(node._is_node_busy())

    def test_acknowledged_completion_cleans_up_completed_workload_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node, workload = self._node_with_workload(tmp_dir)
            node.config.upload_results = False
            node._buffer_log = AsyncMock()
            node._flush_logs = AsyncMock()
            workload.stdout_file.write_text("Measured process output")
            workload.stderr_file.write_text("")
            params_file = workload.stdout_file.with_suffix(".params.json")
            params_file.write_text("{}")

            asyncio.run(node._handle_workload_completion(workload, exit_code=0))

            self.assertFalse(workload.stdout_file.exists())
            self.assertFalse(workload.stderr_file.exists())
            self.assertFalse(params_file.exists())
            self.assertFalse(node._is_node_busy())

    def test_duplicate_completion_does_not_release_the_owners_claim(self) -> None:
        """The losing claimant must not free a workload it does not own."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            node, workload = self._node_with_workload(tmp_dir)

            async def _claim_then_duplicate() -> None:
                await node._claim_workload_completion(workload)
                # A second completion for the same workload (monitor loop vs
                # cancel path) arrives while the upload is still running.
                await node._handle_workload_completion(workload, exit_code=0)

            asyncio.run(_claim_then_duplicate())

            self.assertIn(777, node._finalizing_workloads)
            self.assertTrue(node._is_node_busy())


class ConcurrentStartTests(unittest.TestCase):
    """The busy check and the spawn that follows it are one critical section.

    MQTT delivery is at-least-once and each command runs in its own task, so two
    start commands can be in flight at once. The busy check is no longer
    instantaneous — it reconciles against the backend first — so without a lock
    both would observe an idle node and both spawn a process on it.
    """

    def test_concurrent_starts_do_not_both_spawn(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = _make_node(tmp_dir, inference="python controller_deploy.py {body}")
            node._mqtt_client = AsyncMock()
            node.instance_uuid = "this-instance"
            node._save_workload_state = AsyncMock()

            async def _slow_spawn(workload_type, params, request_id):
                # Stand in for the real spawn: registers the workload only
                # after an await, which is where the race used to open.
                await asyncio.sleep(0.05)
                wl = _dummy_active_workload(id(params), tmp_dir)
                wl.workload_uuid = params.get("workload_uuid")
                node._active_workloads[wl.pid] = wl

            node._spawn_workload_process = Mock(side_effect=_slow_spawn)

            async def _race() -> None:
                await asyncio.gather(
                    node._handle_inference({"workload_uuid": "wl-a"}, request_id="a"),
                    node._handle_inference({"workload_uuid": "wl-b"}, request_id="b"),
                )

            asyncio.run(_race())

            self.assertEqual(node._spawn_workload_process.call_count, 1)
            self.assertEqual(len(node._active_workloads), 1)
            # The loser is reported as host_busy so the backend requeues it.
            kwargs = node._mqtt_client.update_workload_status.await_args.kwargs
            self.assertEqual(kwargs["status"], "failed")
            self.assertEqual(kwargs["additional_data"]["rejection_reason"], "host_busy")


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
            "[workload wl-abc stdout] [rltask] step 1\n[workload wl-abc stdout] [rltask] step 2\n",
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
        self.assertEqual(out, "[workload wl-abc stderr] Traceback (most recent call last):\n")

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
