import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
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

from cyberwave_cloud_node import cli  # noqa: E402
from cyberwave_cloud_node import config as config_module  # noqa: E402
from cyberwave_cloud_node.credentials import Credentials  # noqa: E402
from cyberwave_cloud_node.credentials import _resolve_config_dir  # noqa: E402


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


class ConfigDirResolutionTests(unittest.TestCase):
    def test_env_override_takes_priority(self) -> None:
        with patch.dict(os.environ, {"CYBERWAVE_EDGE_CONFIG_DIR": "/custom/path"}):
            result = _resolve_config_dir()
        self.assertEqual(result, Path("/custom/path"))

    def test_macos_uses_home_directory(self) -> None:
        with (
            patch.dict(os.environ, {}, clear=True),
            patch(
                "cyberwave_cloud_node.credentials.platform.system",
                return_value="Darwin",
            ),
            patch(
                "cyberwave_cloud_node.credentials._resolve_sudo_user_home",
                return_value=None,
            ),
            patch(
                "cyberwave_cloud_node.credentials.Path.home",
                return_value=Path("/Users/testuser"),
            ),
        ):
            result = _resolve_config_dir()
        self.assertEqual(result, Path("/Users/testuser/.cyberwave"))

    def test_linux_uses_system_dir_when_writable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            system_dir = Path(tmp_dir) / "etc" / "cyberwave"
            system_dir.mkdir(parents=True)
            with (
                patch.dict(os.environ, {}, clear=True),
                patch(
                    "cyberwave_cloud_node.credentials.platform.system",
                    return_value="Linux",
                ),
                patch(
                    "cyberwave_cloud_node.credentials._SYSTEM_CONFIG_DIR",
                    system_dir,
                ),
                patch("cyberwave_cloud_node.credentials.os.access", return_value=True),
            ):
                result = _resolve_config_dir()
            self.assertEqual(result, system_dir)

    def test_linux_falls_back_to_home_when_system_dir_not_writable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            system_dir = Path(tmp_dir) / "etc" / "cyberwave"
            system_dir.mkdir(parents=True)
            home_dir = Path(tmp_dir) / "home" / "testuser"
            home_dir.mkdir(parents=True)
            with (
                patch.dict(os.environ, {}, clear=True),
                patch(
                    "cyberwave_cloud_node.credentials.platform.system",
                    return_value="Linux",
                ),
                patch(
                    "cyberwave_cloud_node.credentials._SYSTEM_CONFIG_DIR",
                    system_dir,
                ),
                patch(
                    "cyberwave_cloud_node.credentials._USER_CONFIG_DIR",
                    home_dir / ".cyberwave",
                ),
                patch("cyberwave_cloud_node.credentials.os.access", return_value=False),
            ):
                result = _resolve_config_dir()
            self.assertEqual(result, home_dir / ".cyberwave")


class StoredCredentialsRuntimeEnvTests(unittest.TestCase):
    def test_config_getters_fallback_to_shared_credentials_envs(self) -> None:
        shared_credentials = Credentials.from_dict(
            {
                "token": "token-123",
                "envs": {
                    "CYBERWAVE_BASE_URL": "http://localhost:8000",
                    "CYBERWAVE_MQTT_HOST": "localhost",
                    "CYBERWAVE_MQTT_PORT": "1883",
                },
            }
        )

        with (
            patch.dict(os.environ, {}, clear=True),
            patch(
                "cyberwave_cloud_node.config.creds_module.load_credentials",
                return_value=shared_credentials,
            ),
        ):
            self.assertEqual(config_module.get_api_url(), "http://localhost:8000")
            self.assertEqual(config_module.get_mqtt_host(), "localhost")
            self.assertEqual(config_module.get_mqtt_port(), 1883)


class StartNodeTests(unittest.TestCase):
    def test_start_node_logs_instance_uuid_from_helper(self) -> None:
        args = SimpleNamespace(
            config=None,
            profile=None,
            mqtt_host=None,
            mqtt_port=None,
            slug=None,
        )

        config = SimpleNamespace(
            profile_slug="mujoco-sim",
            mqtt_host="localhost",
            mqtt_port=1883,
            inference=None,
            simulate=None,
            training=None,
        )
        logger = unittest.mock.Mock()
        node = unittest.mock.Mock()

        with (
            patch("cyberwave_cloud_node.cli.load_dotenv_files"),
            patch("cyberwave_cloud_node.cli.init_sentry"),
            patch("cyberwave_cloud_node.cli.get_api_token", return_value="token-123"),
            patch("cyberwave_cloud_node.cli.get_instance_slug", return_value=None),
            patch("cyberwave_cloud_node.cli.get_instance_uuid", return_value="instance-123"),
            patch("cyberwave_cloud_node.cli.CloudNodeConfig.from_file", side_effect=FileNotFoundError),
            patch("cyberwave_cloud_node.cli.CloudNodeConfig.from_env", return_value=config),
            patch("cyberwave_cloud_node.cli.CloudNode", return_value=node),
            patch("cyberwave_cloud_node.cli.logging.getLogger", return_value=logger),
        ):
            result = cli.start_node(args)

        self.assertEqual(result, 0)
        logger.info.assert_any_call("Instance UUID: instance-123")
        node.run.assert_called_once_with()
