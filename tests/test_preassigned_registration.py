"""A node pinned to one instance via CYBERWAVE_CLOUD_NODE_INSTANCE_UUID never creates another.

Imports happen after dependency stubs are installed (same stubs as test_mqtt_reconnect).
"""

# ruff: noqa: E402

import asyncio
import os
import sys
import tempfile
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

sys.modules.setdefault("psutil", Mock())
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

from cyberwave_cloud_node import config as config_mod
from cyberwave_cloud_node.cloud_node import CloudNode, CloudNodeError
from cyberwave_cloud_node.config import (
    INSTANCE_UUID_ENV,
    MAX_HEARTBEAT_AUTH_RETRIES,
    CloudNodeConfig,
    get_instance_uuid,
    get_preassigned_instance_uuid,
)
from cyberwave_cloud_node.mqtt import (
    CloudNodeInstanceGoneError,
    CloudNodeMQTTClient,
    CloudNodeRegistrationRejectedError,
    MQTTError,
)

PREASSIGNED = "7f0c2a52-3a8e-4c6c-9f0e-1d2b3c4d5e6f"
OTHER = "0a0a0a0a-0000-4000-8000-000000000000"


def _ok(uuid: str = PREASSIGNED) -> SimpleNamespace:
    return SimpleNamespace(
        success=True, payload={"uuid": uuid, "slug": "edge-controller", "status": "ready"}
    )


def _rejected(code: int) -> CloudNodeRegistrationRejectedError:
    return CloudNodeRegistrationRejectedError(
        f"Instance registration rejected (code={code}): nope", code=code
    )


class _NodeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        state_path = patch(
            "cyberwave_cloud_node.cloud_node._cloud_node_state_dir",
            return_value=self.tmp,
        )
        state_path.start()
        self.addCleanup(state_path.stop)
        env = patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop(INSTANCE_UUID_ENV, None)
        # Registration waits between attempts; never really sleep in a test.
        sleep = patch("cyberwave_cloud_node.cloud_node.asyncio.sleep", new=AsyncMock())
        self.sleep = sleep.start()
        self.addCleanup(sleep.stop)

    def _node(self, *, preassigned: bool, stored_uuid: str | None = None) -> CloudNode:
        if preassigned:
            os.environ[INSTANCE_UUID_ENV] = PREASSIGNED
        with (
            patch("cyberwave_cloud_node.cloud_node.Path.home", return_value=self.tmp),
            patch.object(config_mod.creds_module, "get_instance_uuid", return_value=stored_uuid),
        ):
            node = CloudNode(
                config=CloudNodeConfig(heartbeat_interval=0),
                client=Mock(),
                working_dir=self.tmp,
            )
        node._running = True
        node._mqtt_client = AsyncMock()
        return node


class ConfigTests(unittest.TestCase):
    def test_only_the_environment_counts_as_pre_assigned(self) -> None:
        with (
            patch.dict(os.environ),
            patch.object(config_mod.creds_module, "get_instance_uuid", return_value=OTHER),
        ):
            os.environ.pop(INSTANCE_UUID_ENV, None)
            self.assertIsNone(get_preassigned_instance_uuid())
            self.assertEqual(get_instance_uuid(), OTHER)  # stored identity unchanged

            os.environ[INSTANCE_UUID_ENV] = PREASSIGNED
            self.assertEqual(get_preassigned_instance_uuid(), PREASSIGNED)
            self.assertEqual(get_instance_uuid(), PREASSIGNED)

            os.environ[INSTANCE_UUID_ENV] = ""
            self.assertIsNone(get_preassigned_instance_uuid())

    def test_the_pinned_uuid_is_canonicalised(self) -> None:
        """The backend echoes the canonical form; a hand-typed spelling must match it."""

        with patch.dict(os.environ):
            for spelling in (
                PREASSIGNED.upper(),
                f" {PREASSIGNED}\n",
                "{" + PREASSIGNED + "}",
                PREASSIGNED.replace("-", ""),
            ):
                os.environ[INSTANCE_UUID_ENV] = spelling
                self.assertEqual(get_preassigned_instance_uuid(), PREASSIGNED, spelling)

            os.environ[INSTANCE_UUID_ENV] = "not-a-uuid"
            self.assertEqual(get_preassigned_instance_uuid(), "not-a-uuid")


class RegisterInstanceCodeTests(unittest.TestCase):
    def _client(self, payload: dict) -> CloudNodeMQTTClient:
        with patch("cyberwave_cloud_node.mqtt.mqtt.Client", return_value=Mock()):
            client = CloudNodeMQTTClient(host="mqtt.example.com", port=1883)
        client.publish_request = AsyncMock(
            return_value=SimpleNamespace(success=False, payload=payload)
        )
        return client

    def test_401_403_404_are_a_rejection_with_the_code(self) -> None:
        for code in (401, 403, 404):
            client = self._client({"code": code, "message": "backend says no"})
            with self.assertRaises(CloudNodeRegistrationRejectedError) as ctx:
                asyncio.run(client.register_instance(instance_uuid=PREASSIGNED, profile_slug="p"))
            self.assertEqual(ctx.exception.code, code)
            self.assertIn("backend says no", str(ctx.exception))
            # Still an MQTTError: callers that do not distinguish keep retrying.
            self.assertIsInstance(ctx.exception, MQTTError)

    def test_other_failures_stay_a_plain_mqtt_error(self) -> None:
        for payload in ({"code": 400, "message": "Instance busy"}, {"message": "?"}):
            client = self._client(payload)
            with self.assertRaises(MQTTError) as ctx:
                asyncio.run(client.register_instance(instance_uuid=PREASSIGNED, profile_slug="p"))
            self.assertNotIsInstance(ctx.exception, CloudNodeRegistrationRejectedError)


class PreassignedRegistrationTests(_NodeTestCase):
    def test_a_pre_assigned_uuid_registers_without_creating_an_instance(self) -> None:
        node = self._node(preassigned=True)
        node._mqtt_client.register_instance = AsyncMock(return_value=_ok())

        asyncio.run(node._register_via_mqtt())

        self.assertEqual(node.instance_uuid, PREASSIGNED)
        node._mqtt_client.request_instance.assert_not_awaited()
        self.assertEqual(
            node._mqtt_client.register_instance.await_args.kwargs["instance_uuid"],
            PREASSIGNED,
        )

    def test_not_found_and_not_authorised_fail_at_once(self) -> None:
        for code in (404, 403):
            with self.subTest(code=code):
                node = self._node(preassigned=True)
                node._mqtt_client.register_instance = AsyncMock(side_effect=_rejected(code))

                with self.assertRaises(CloudNodeError) as ctx:
                    asyncio.run(node._register_via_mqtt())

                message = str(ctx.exception)
                self.assertIn(PREASSIGNED, message)
                self.assertIn(f"code={code}", message)
                self.assertIn("will not create another", message)
                self.assertEqual(node._mqtt_client.register_instance.await_count, 1)
                node._mqtt_client.request_instance.assert_not_awaited()
                self.assertEqual(node.instance_uuid, PREASSIGNED)

    def test_a_refusal_names_the_reserved_environment_when_one_is_set(self) -> None:
        """The backend checks the reservation first, so its 404 may not be the instance's."""

        env_uuid = "e0e0e0e0-0000-4000-8000-000000000000"
        node = self._node(preassigned=True)
        node._mqtt_client.register_instance = AsyncMock(side_effect=_rejected(404))

        with self.assertRaises(CloudNodeError) as ctx:
            asyncio.run(node._register_via_mqtt())
        self.assertNotIn("CYBERWAVE_NODE_ENVIRONMENT_UUID", str(ctx.exception))

        node.config.reserved_environment_uuid = env_uuid
        with self.assertRaises(CloudNodeError) as ctx:
            asyncio.run(node._register_via_mqtt())
        self.assertIn(env_uuid, str(ctx.exception))
        self.assertIn("CYBERWAVE_NODE_ENVIRONMENT_UUID", str(ctx.exception))

    def test_an_uppercase_pinned_uuid_accepts_the_canonical_echo(self) -> None:
        os.environ[INSTANCE_UUID_ENV] = PREASSIGNED.upper()
        node = self._node(preassigned=False)
        node._mqtt_client.register_instance = AsyncMock(return_value=_ok())

        asyncio.run(node._register_via_mqtt())

        self.assertEqual(node.instance_uuid, PREASSIGNED)

    def test_a_rejected_token_gets_a_bounded_grace_then_fails(self) -> None:
        node = self._node(preassigned=True)
        node._mqtt_client.register_instance = AsyncMock(side_effect=_rejected(401))

        with self.assertRaisesRegex(CloudNodeError, "code=401"):
            asyncio.run(node._register_via_mqtt())

        self.assertEqual(
            node._mqtt_client.register_instance.await_count, MAX_HEARTBEAT_AUTH_RETRIES
        )
        self.assertEqual(self.sleep.await_count, MAX_HEARTBEAT_AUTH_RETRIES - 1)
        node._mqtt_client.request_instance.assert_not_awaited()

    def test_a_token_that_starts_working_in_the_grace_registers(self) -> None:
        node = self._node(preassigned=True)
        node._mqtt_client.register_instance = AsyncMock(
            side_effect=[_rejected(401), _rejected(401), _ok()]
        )

        asyncio.run(node._register_via_mqtt())

        self.assertEqual(node.instance_uuid, PREASSIGNED)
        node._mqtt_client.request_instance.assert_not_awaited()

    def test_a_transient_failure_is_still_retried(self) -> None:
        """Broker/consumer down or a 400 is not a verdict on the instance."""

        node = self._node(preassigned=True)
        node._mqtt_client.register_instance = AsyncMock(
            side_effect=[asyncio.TimeoutError(), MQTTError("busy"), _ok()]
        )

        asyncio.run(node._register_via_mqtt())

        self.assertEqual(node.instance_uuid, PREASSIGNED)
        node._mqtt_client.request_instance.assert_not_awaited()

    def test_a_response_naming_another_instance_is_refused(self) -> None:
        node = self._node(preassigned=True)
        node._mqtt_client.register_instance = AsyncMock(return_value=_ok(OTHER))

        with self.assertRaises(CloudNodeError) as ctx:
            asyncio.run(node._register_via_mqtt())

        self.assertIn(OTHER, str(ctx.exception))
        self.assertEqual(node.instance_uuid, PREASSIGNED)

    def test_a_cleared_uuid_never_falls_through_to_creation(self) -> None:
        node = self._node(preassigned=True)
        node.instance_uuid = ""

        with self.assertRaises(CloudNodeError):
            asyncio.run(node._register_via_mqtt())

        node._mqtt_client.request_instance.assert_not_awaited()
        node._mqtt_client.register_instance.assert_not_awaited()

    def test_reregister_refuses_a_pre_assigned_instance(self) -> None:
        node = self._node(preassigned=True)

        with self.assertRaises(CloudNodeError):
            asyncio.run(node._reregister())

        self.assertEqual(node.instance_uuid, PREASSIGNED)
        node._mqtt_client.request_instance.assert_not_awaited()

    def test_instance_gone_shuts_a_pre_assigned_node_down(self) -> None:
        node = self._node(preassigned=True)
        node._mqtt_client.send_heartbeat = AsyncMock(side_effect=CloudNodeInstanceGoneError("gone"))
        node._reregister = AsyncMock()  # type: ignore[method-assign]

        asyncio.run(node._heartbeat_loop())

        node._reregister.assert_not_awaited()
        node._mqtt_client.request_instance.assert_not_awaited()
        self.assertFalse(node._running)
        self.assertTrue(node._shutdown_event.is_set())
        self.assertIn(PREASSIGNED, node._fatal_error or "")

    def test_a_gone_instance_is_not_reported_failed_or_terminated(self) -> None:
        """The backend would only answer "not found" for it."""

        node = self._node(preassigned=True)
        node._mqtt_client.send_heartbeat = AsyncMock(side_effect=CloudNodeInstanceGoneError("gone"))
        node._flush_logs = AsyncMock()  # type: ignore[method-assign]
        asyncio.run(node._heartbeat_loop())

        asyncio.run(node._notify_failed("gone"))
        asyncio.run(node._shutdown())

        node._mqtt_client.notify_failed.assert_not_awaited()
        node._mqtt_client.notify_terminated.assert_not_awaited()
        node._mqtt_client.disconnect.assert_awaited_once()

    def test_a_fatal_error_makes_run_async_raise(self) -> None:
        """So the CLI returns 1 and the launcher sees the node died."""

        node = self._node(preassigned=True)
        node._running = False
        node._manifest = None
        node.config.install_script = None
        node._setup_signal_handlers = Mock()  # type: ignore[method-assign]
        for name in (
            "_connect_mqtt",
            "_recover_local_workloads",
            "_subscribe_to_commands",
            "_notify_failed",
            "_shutdown",
        ):
            setattr(node, name, AsyncMock())

        async def _registered() -> None:
            node.instance_uuid = PREASSIGNED

        async def _gone() -> None:
            node._fatal_error = "instance gone"
            node._shutdown_event.set()

        async def _idle() -> None:
            await asyncio.Event().wait()

        node._register_via_mqtt = _registered  # type: ignore[method-assign]
        node._heartbeat_loop = _gone  # type: ignore[method-assign]
        for name in (
            "_log_flush_loop",
            "_mqtt_reconnect_loop",
            "_workload_monitor_loop",
            "_workload_log_stream_loop",
        ):
            setattr(node, name, _idle)

        with self.assertRaisesRegex(CloudNodeError, "instance gone"):
            asyncio.run(node.run_async())
        node._notify_failed.assert_awaited_once_with("instance gone")
        node._shutdown.assert_awaited_once()


class StoredIdentityUnchangedTests(_NodeTestCase):
    """Without the environment variable nothing about registration changes."""

    def test_a_rejected_stored_uuid_is_retried_not_fatal(self) -> None:
        node = self._node(preassigned=False, stored_uuid=OTHER)
        self.assertEqual(node.instance_uuid, OTHER)
        self.assertEqual(node._preassigned_instance_uuid, "")
        node._mqtt_client.register_instance = AsyncMock(side_effect=[_rejected(404), _ok(OTHER)])

        asyncio.run(node._register_via_mqtt())

        self.assertEqual(node._mqtt_client.register_instance.await_count, 2)
        self.assertEqual(node.instance_uuid, OTHER)

    def test_no_uuid_at_all_still_creates_an_instance(self) -> None:
        node = self._node(preassigned=False)
        self.assertEqual(node.instance_uuid, "")
        node._mqtt_client.request_instance = AsyncMock(
            return_value=SimpleNamespace(
                success=True, payload={"uuid": OTHER, "slug": "fresh", "status": "provisioning"}
            )
        )
        node._mqtt_client.register_instance = AsyncMock(return_value=_ok(OTHER))

        asyncio.run(node._register_via_mqtt())

        node._mqtt_client.request_instance.assert_awaited_once()
        self.assertEqual(node.instance_uuid, OTHER)

    def test_instance_gone_still_reregisters(self) -> None:
        node = self._node(preassigned=False, stored_uuid=OTHER)
        node._mqtt_client.send_heartbeat = AsyncMock(side_effect=CloudNodeInstanceGoneError("gone"))

        async def _fresh() -> None:
            node._running = False

        node._reregister = AsyncMock(side_effect=_fresh)  # type: ignore[method-assign]

        asyncio.run(node._heartbeat_loop())

        node._reregister.assert_awaited_once()
        self.assertIsNone(node._fatal_error)


if __name__ == "__main__":
    unittest.main()
