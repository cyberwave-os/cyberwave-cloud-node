import asyncio
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import ANY, AsyncMock, Mock, call, patch

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

from cyberwave_cloud_node.cloud_node import ActiveWorkload, CloudNode
from cyberwave_cloud_node.config import CloudNodeConfig
from cyberwave_cloud_node.mqtt import CloudNodeMQTTClient


class MQTTReconnectTests(unittest.TestCase):
    def test_config_reads_simulate_command(self) -> None:
        config = CloudNodeConfig.from_dict(
            {
                "cyberwave-cloud-node": {
                    "simulate": "python run_mujoco_workload.py --params {body}"
                }
            }
        )

        self.assertEqual(
            config.simulate, "python run_mujoco_workload.py --params {body}"
        )

    def test_config_reads_mqtt_username_from_environment(self) -> None:
        original = os.environ.get("CYBERWAVE_MQTT_USERNAME")
        os.environ["CYBERWAVE_MQTT_USERNAME"] = "explicit-mqtt-username"
        try:
            config = CloudNodeConfig.from_dict({"cyberwave-cloud-node": {}})
        finally:
            if original is None:
                del os.environ["CYBERWAVE_MQTT_USERNAME"]
            else:
                os.environ["CYBERWAVE_MQTT_USERNAME"] = original

        self.assertEqual(config.mqtt_username, "explicit-mqtt-username")

    def test_connect_mqtt_uses_generated_bootstrap_username_when_identity_missing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "cyberwave_cloud_node.cloud_node.Path.home",
                return_value=Path(tmp_dir),
            ):
                node = CloudNode(
                    config=CloudNodeConfig(mqtt_password="api-token"),
                    client=Mock(),
                    working_dir=Path(tmp_dir),
                )

            mock_mqtt_client = AsyncMock()
            with (
                patch(
                    "cyberwave_cloud_node.cloud_node.get_api_token",
                    return_value="api-token",
                ),
                patch(
                    "cyberwave_cloud_node.cloud_node.CloudNodeMQTTClient",
                    return_value=mock_mqtt_client,
                ) as mock_client_cls,
            ):
                asyncio.run(node._connect_mqtt())

        self.assertTrue(mock_client_cls.called)
        username = mock_client_cls.call_args.kwargs["username"]
        self.assertTrue(username)
        self.assertTrue(username.startswith("cloud-node-bootstrap-"))
        mock_mqtt_client.connect.assert_awaited_once()

    def test_connect_mqtt_prefers_slug_for_bootstrap_username(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "cyberwave_cloud_node.cloud_node.Path.home",
                return_value=Path(tmp_dir),
            ):
                node = CloudNode(
                    config=CloudNodeConfig(mqtt_password="api-token"),
                    slug="mujoco-sim-worker",
                    client=Mock(),
                    working_dir=Path(tmp_dir),
                )

            mock_mqtt_client = AsyncMock()
            with (
                patch(
                    "cyberwave_cloud_node.cloud_node.get_api_token",
                    return_value="api-token",
                ),
                patch(
                    "cyberwave_cloud_node.cloud_node.CloudNodeMQTTClient",
                    return_value=mock_mqtt_client,
                ) as mock_client_cls,
            ):
                asyncio.run(node._connect_mqtt())

        self.assertEqual(
            mock_client_cls.call_args.kwargs["username"], "mujoco-sim-worker"
        )
        mock_mqtt_client.connect.assert_awaited_once()

    def test_workload_received_dispatches_simulate_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "cyberwave_cloud_node.cloud_node.Path.home",
                return_value=Path(tmp_dir),
            ):
                node = CloudNode(
                    config=CloudNodeConfig(),
                    client=Mock(),
                    working_dir=Path(tmp_dir),
                )

            node._handle_simulate = AsyncMock()

            asyncio.run(
                node._handle_workload_received(
                    {
                        "workload_uuid": "workload-123",
                        "command_type": "simulate",
                        "command_params": {"environment_uuid": "env-123"},
                    },
                    "request-123",
                )
            )

        node._handle_simulate.assert_awaited_once()
        params, request_id = node._handle_simulate.await_args.args
        self.assertEqual(request_id, "request-123")
        self.assertEqual(params["environment_uuid"], "env-123")
        self.assertEqual(params["workload_uuid"], "workload-123")

    def test_resubscribes_command_topics_on_reconnect(self) -> None:
        mock_paho_client = Mock()
        mock_paho_client.subscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)
        mock_paho_client.message_callback_remove.side_effect = KeyError(
            "missing callback"
        )

        with patch(
            "cyberwave_cloud_node.mqtt.mqtt.Client", return_value=mock_paho_client
        ):
            client = CloudNodeMQTTClient(host="mqtt.example.com", port=1883)

        client._connected = True
        callback = Mock()
        topic = "cyberwave/cloud-node/test-instance/command"

        client.subscribe_command(topic, callback)

        self.assertEqual(client._command_callbacks, {topic: callback})
        self.assertEqual(mock_paho_client.message_callback_add.call_count, 1)
        mock_paho_client.message_callback_add.reset_mock()
        mock_paho_client.message_callback_remove.reset_mock()
        mock_paho_client.subscribe.reset_mock()

        client._on_connect(
            mock_paho_client,
            None,
            {},
            0,
            SimpleNamespace(),
        )

        self.assertEqual(client._command_callbacks, {topic: callback})
        self.assertEqual(
            mock_paho_client.subscribe.call_args_list,
            [
                call(client._response_topic, qos=1),
                call(topic, qos=1),
            ],
        )
        mock_paho_client.message_callback_remove.assert_called_once_with(topic)
        self.assertEqual(mock_paho_client.message_callback_add.call_count, 1)

    def test_configures_paho_reconnect_backoff(self) -> None:
        mock_paho_client = Mock()

        with patch(
            "cyberwave_cloud_node.mqtt.mqtt.Client", return_value=mock_paho_client
        ):
            CloudNodeMQTTClient(host="mqtt.example.com", port=1883)

        mock_paho_client.reconnect_delay_set.assert_called_once_with(
            min_delay=2, max_delay=3600
        )

    def test_mqtt_reconnect_loop_does_not_manually_reconnect(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "cyberwave_cloud_node.cloud_node.Path.home",
                return_value=Path(tmp_dir),
            ):
                node = CloudNode(
                    config=CloudNodeConfig(),
                    client=Mock(),
                    working_dir=Path(tmp_dir),
                )
            node._running = True
            node.instance_uuid = "instance-123"
            node._mqtt_client = Mock(connected=False)
            node._mqtt_client.connect = AsyncMock()

            async def fake_sleep(_: int) -> None:
                node._running = False

            with patch(
                "cyberwave_cloud_node.cloud_node.asyncio.sleep",
                new=AsyncMock(side_effect=fake_sleep),
            ):
                asyncio.run(node._mqtt_reconnect_loop())

        node._mqtt_client.connect.assert_not_awaited()

    def test_cancel_simulate_workload_waits_for_process_exit_before_backend_update(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "cyberwave_cloud_node.cloud_node.Path.home",
                return_value=Path(tmp_dir),
            ):
                node = CloudNode(
                    config=CloudNodeConfig(upload_results=False),
                    client=Mock(),
                    working_dir=Path(tmp_dir),
                )

            stdout_file = Path(tmp_dir) / "simulate.stdout.log"
            stderr_file = Path(tmp_dir) / "simulate.stderr.log"
            stdout_file.write_text("")
            stderr_file.write_text("")
            workload = ActiveWorkload(
                pid=4321,
                request_id="simulate-request",
                workload_type="simulate",
                started_at=0.0,
                command="python run_mujoco_workload.py",
                stdout_file=stdout_file,
                stderr_file=stderr_file,
                params={},
                workload_uuid="workload-123",
            )
            node._mqtt_client = AsyncMock()
            node._active_workloads[workload.pid] = workload

            mock_process = Mock()
            with (
                patch.object(node, "_is_process_alive", return_value=True),
                patch(
                    "cyberwave_cloud_node.cloud_node.psutil.Process",
                    return_value=mock_process,
                ),
            ):
                success, message = asyncio.run(node._cancel_workload(workload, "SIGTERM"))

        self.assertTrue(success)
        self.assertIn("cancellation requested", message.lower())
        self.assertIn(workload.pid, node._active_workloads)
        node._mqtt_client.update_workload_status.assert_not_awaited()
        node._mqtt_client.complete_workload.assert_not_awaited()

    def test_cancel_success_workload_types_are_generic_for_cancel_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "cyberwave_cloud_node.cloud_node.Path.home",
                return_value=Path(tmp_dir),
            ):
                node = CloudNode(
                    config=CloudNodeConfig(upload_results=False),
                    client=Mock(),
                    working_dir=Path(tmp_dir),
                )

            stdout_file = Path(tmp_dir) / "inference.stdout.log"
            stderr_file = Path(tmp_dir) / "inference.stderr.log"
            stdout_file.write_text("")
            stderr_file.write_text("")
            workload = ActiveWorkload(
                pid=9876,
                request_id="inference-request",
                workload_type="inference",
                started_at=0.0,
                command="python inference.py",
                stdout_file=stdout_file,
                stderr_file=stderr_file,
                params={},
                workload_uuid="workload-456",
            )
            node._mqtt_client = AsyncMock()
            node._active_workloads[workload.pid] = workload

            mock_process = Mock()
            with (
                patch.object(
                    CloudNode,
                    "CANCEL_COUNTS_AS_SUCCESS_WORKLOAD_TYPES",
                    {"simulate", "inference"},
                    create=True,
                ),
                patch.object(node, "_is_process_alive", return_value=True),
                patch(
                    "cyberwave_cloud_node.cloud_node.psutil.Process",
                    return_value=mock_process,
                ),
            ):
                success, message = asyncio.run(node._cancel_workload(workload, "SIGTERM"))

        self.assertTrue(success)
        self.assertIn("cancellation requested", message.lower())
        self.assertTrue(workload.cancel_requested)
        self.assertEqual(workload.cancel_signal, "SIGTERM")
        self.assertIn(workload.pid, node._active_workloads)
        node._mqtt_client.update_workload_status.assert_not_awaited()

    def test_cancel_success_workload_types_are_generic_for_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "cyberwave_cloud_node.cloud_node.Path.home",
                return_value=Path(tmp_dir),
            ):
                node = CloudNode(
                    config=CloudNodeConfig(upload_results=False),
                    client=Mock(),
                    working_dir=Path(tmp_dir),
                )

            stdout_file = Path(tmp_dir) / "inference.stdout.log"
            stderr_file = Path(tmp_dir) / "inference.stderr.log"
            stdout_file.write_text("")
            stderr_file.write_text("")
            workload = ActiveWorkload(
                pid=2468,
                request_id="inference-request",
                workload_type="inference",
                started_at=0.0,
                command="python inference.py",
                stdout_file=stdout_file,
                stderr_file=stderr_file,
                params={},
                workload_uuid="workload-789",
                cancel_requested=True,
                cancel_signal="SIGTERM",
            )
            node._mqtt_client = AsyncMock()
            node._buffer_log = AsyncMock()

            with (
                patch.object(
                    CloudNode,
                    "CANCEL_COUNTS_AS_SUCCESS_WORKLOAD_TYPES",
                    {"simulate", "inference"},
                    create=True,
                ),
                self.assertLogs("cyberwave_cloud_node.cloud_node", level="INFO") as captured,
            ):
                asyncio.run(node._handle_workload_completion(workload, exit_code=1))

        joined_logs = "\n".join(captured.output)
        self.assertIn("success=True", joined_logs)
        node._mqtt_client.complete_workload.assert_awaited_once_with(
            workload_uuid="workload-789",
            success=True,
            exit_code=1,
            timeout=30.0,
        )

    def test_failed_workload_completion_reports_failure_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "cyberwave_cloud_node.cloud_node.Path.home",
                return_value=Path(tmp_dir),
            ):
                node = CloudNode(
                    config=CloudNodeConfig(upload_results=False),
                    client=Mock(),
                    working_dir=Path(tmp_dir),
                )

            stdout_file = Path(tmp_dir) / "inference.stdout.log"
            stderr_file = Path(tmp_dir) / "inference.stderr.log"
            stdout_file.write_text("")
            stderr_file.write_text("ImportError: pymeshlab failed to import")
            workload = ActiveWorkload(
                pid=9753,
                request_id="inference-request",
                workload_type="inference",
                started_at=0.0,
                command="python inference.py",
                stdout_file=stdout_file,
                stderr_file=stderr_file,
                params={},
                workload_uuid="workload-failed",
            )
            node._mqtt_client = AsyncMock()
            node._buffer_log = AsyncMock()

            asyncio.run(node._handle_workload_completion(workload, exit_code=1))

        node._mqtt_client.complete_workload.assert_awaited_once_with(
            workload_uuid="workload-failed",
            success=False,
            exit_code=1,
            timeout=30.0,
        )

    def test_completion_is_deduplicated_after_cancel_handles_dead_process(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "cyberwave_cloud_node.cloud_node.Path.home",
                return_value=Path(tmp_dir),
            ):
                node = CloudNode(
                    config=CloudNodeConfig(upload_results=False),
                    client=Mock(),
                    working_dir=Path(tmp_dir),
                )

            stdout_file = Path(tmp_dir) / "simulate.stdout.log"
            stderr_file = Path(tmp_dir) / "simulate.stderr.log"
            stdout_file.write_text("")
            stderr_file.write_text("")
            workload = ActiveWorkload(
                pid=1357,
                request_id="simulate-request",
                workload_type="simulate",
                started_at=0.0,
                command="python run_mujoco_workload.py",
                stdout_file=stdout_file,
                stderr_file=stderr_file,
                params={},
                workload_uuid="workload-dedupe",
            )
            node._mqtt_client = AsyncMock()
            node._buffer_log = AsyncMock()
            node._active_workloads[workload.pid] = workload

            with patch.object(node, "_is_process_alive", return_value=False):
                success, _message = asyncio.run(node._cancel_workload(workload, "SIGTERM"))

            self.assertTrue(success)

            asyncio.run(node._handle_workload_completion(workload, exit_code=0))

        node._mqtt_client.complete_workload.assert_awaited_once_with(
            workload_uuid="workload-dedupe",
            success=True,
            exit_code=ANY,
            timeout=30.0,
        )

    def test_complete_workload_publishes_success_metadata(self) -> None:
        mock_paho_client = Mock()

        with patch(
            "cyberwave_cloud_node.mqtt.mqtt.Client", return_value=mock_paho_client
        ):
            client = CloudNodeMQTTClient(
                host="mqtt.example.com",
                port=1883,
                client_id="cloud_node_test",
                topic_prefix="local",
            )

        client.publish_request = AsyncMock(
            return_value=SimpleNamespace(success=True, payload={})
        )

        asyncio.run(client.complete_workload("workload-123", success=False, exit_code=1))

        client.publish_request.assert_awaited_once_with(
            "localcyberwave/cloud-workload/workload-123/update-status",
            {"status": "completed", "success": False, "exit_code": 1},
            timeout=30.0,
        )

    def test_matches_response_correlation_data_when_bytearray(self) -> None:
        mock_paho_client = Mock()

        with patch(
            "cyberwave_cloud_node.mqtt.mqtt.Client", return_value=mock_paho_client
        ):
            client = CloudNodeMQTTClient(
                host="mqtt.example.com",
                port=1883,
                client_id="cloud_node_test",
                topic_prefix="local",
            )

        loop = asyncio.new_event_loop()
        try:
            client._loop = loop
            correlation_data = b"\x83o\x1fL\xeb\xffG\xd4\xa7\xb7\x12\xb8\xfe\x16G&"
            future: asyncio.Future = loop.create_future()
            client._pending_requests[correlation_data] = future

            message = SimpleNamespace(
                topic=client._response_topic,
                payload=b'{"success": true, "uuid": "instance-123"}',
                properties=SimpleNamespace(CorrelationData=bytearray(correlation_data)),
            )

            client._on_message(mock_paho_client, None, message)
            loop.run_until_complete(asyncio.sleep(0))

            self.assertTrue(future.done())
            response = future.result()
            self.assertTrue(response.success)
            self.assertEqual(response.payload["uuid"], "instance-123")
            self.assertEqual(response.correlation_data, correlation_data)
        finally:
            loop.close()

    def test_logs_unmatched_response_details(self) -> None:
        mock_paho_client = Mock()

        with patch(
            "cyberwave_cloud_node.mqtt.mqtt.Client", return_value=mock_paho_client
        ):
            client = CloudNodeMQTTClient(
                host="mqtt.example.com",
                port=1883,
                client_id="cloud_node_test",
                topic_prefix="local",
            )

        message = SimpleNamespace(
            topic=client._response_topic,
            payload=b'{"success": true}',
            properties=SimpleNamespace(CorrelationData=memoryview(b"unmatched-correlation-data")),
        )

        with self.assertLogs("cyberwave_cloud_node.mqtt", level="INFO") as captured:
            client._on_message(mock_paho_client, None, message)

        joined_logs = "\n".join(captured.output)
        self.assertIn("Received MQTT response on topic", joined_logs)
        self.assertIn("raw_correlation_type=memoryview", joined_logs)
        self.assertIn("No pending request matched response", joined_logs)

    def test_logs_response_topic_even_without_properties(self) -> None:
        mock_paho_client = Mock()

        with patch(
            "cyberwave_cloud_node.mqtt.mqtt.Client", return_value=mock_paho_client
        ):
            client = CloudNodeMQTTClient(
                host="mqtt.example.com",
                port=1883,
                client_id="cloud_node_test",
                topic_prefix="local",
            )

        message = SimpleNamespace(
            topic=client._response_topic,
            payload=b'{"success": true}',
            properties=None,
        )

        with self.assertLogs("cyberwave_cloud_node.mqtt", level="INFO") as captured:
            client._on_message(mock_paho_client, None, message)

        joined_logs = "\n".join(captured.output)
        self.assertIn("Received MQTT response message", joined_logs)
        self.assertIn("properties_present=False", joined_logs)
