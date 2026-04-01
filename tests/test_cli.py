import sys
import unittest
from unittest.mock import patch

sys.modules.setdefault("psutil", unittest.mock.Mock())
sys.modules.setdefault("yaml", unittest.mock.Mock())
sys.modules.setdefault("httpx", unittest.mock.Mock())
sys.modules.setdefault("dotenv", unittest.mock.Mock())
sys.modules.setdefault("paho", unittest.mock.Mock())
sys.modules.setdefault("paho.mqtt", unittest.mock.Mock())
sys.modules.setdefault("paho.mqtt.client", unittest.mock.Mock())
sys.modules.setdefault("paho.mqtt.packettypes", unittest.mock.Mock())
sys.modules.setdefault("paho.mqtt.properties", unittest.mock.Mock())

from cyberwave_cloud_node import cli


class CLIVersionTests(unittest.TestCase):
    def test_get_cli_version_prefers_build_override(self) -> None:
        with (
            patch(
                "cyberwave_cloud_node.cli.get_build_version_override",
                return_value="0.2.23.123",
            ),
            patch("cyberwave_cloud_node.cli.version", return_value="0.2.23"),
        ):
            self.assertEqual(cli.get_cli_version(), "0.2.23.123")

    def test_get_cli_version_falls_back_to_installed_metadata(self) -> None:
        with (
            patch("cyberwave_cloud_node.cli.get_build_version_override", return_value=None),
            patch("cyberwave_cloud_node.cli.version", return_value="0.2.23"),
        ):
            self.assertEqual(cli.get_cli_version(), "0.2.23")
