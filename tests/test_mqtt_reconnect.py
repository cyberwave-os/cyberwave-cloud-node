import asyncio
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

import paho.mqtt.client as mqtt  # pyright: ignore[reportMissingImports]

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

from cyberwave_cloud_node.cloud_node import CloudNode
from cyberwave_cloud_node.config import CloudNodeConfig
from cyberwave_cloud_node.mqtt import CloudNodeMQTTClient


class MQTTReconnectTests(unittest.TestCase):
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

    def test_mqtt_reconnect_loop_relies_on_client_resubscribe(self) -> None:
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
            node._subscribe_to_commands = AsyncMock()

            async def fake_sleep(_: int) -> None:
                node._running = False

            with patch(
                "cyberwave_cloud_node.cloud_node.asyncio.sleep",
                new=AsyncMock(side_effect=fake_sleep),
            ):
                asyncio.run(node._mqtt_reconnect_loop())

        node._mqtt_client.connect.assert_awaited_once()
        node._subscribe_to_commands.assert_not_called()
