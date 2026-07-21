"""Tests for environment-reserved node registration payloads.

A node started with CYBERWAVE_NODE_ENVIRONMENT_UUID (or --environment / the
``environment_uuid`` config key) must send that UUID in its create/register
payloads so the backend reserves the instance for that environment.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from cyberwave_cloud_node import client as client_mod
from cyberwave_cloud_node.client import CloudNodeClient
from cyberwave_cloud_node.config import CloudNodeConfig

ENV_UUID = "11111111-2222-3333-4444-555555555555"


def _make_client() -> CloudNodeClient:
    client = CloudNodeClient(
        base_url="http://localhost:8000",
        token="cw_test",
        workspace_slug="admins-workspace",
    )
    client._client = Mock()
    return client


class CreateInstancePayloadTests(unittest.TestCase):
    def test_create_instance_includes_environment_uuid(self) -> None:
        client = _make_client()
        client._client.post.return_value = SimpleNamespace(
            status_code=200,
            json=lambda: {"uuid": "inst-uuid", "slug": "inst-slug"},
        )

        client.create_instance(profile_slug="default", environment_uuid=ENV_UUID)

        _, kwargs = client._client.post.call_args
        self.assertEqual(kwargs["json"]["environment_uuid"], ENV_UUID)

    def test_create_instance_omits_environment_uuid_when_unset(self) -> None:
        client = _make_client()
        client._client.post.return_value = SimpleNamespace(
            status_code=200,
            json=lambda: {"uuid": "inst-uuid", "slug": "inst-slug"},
        )

        client.create_instance(profile_slug="default")

        _, kwargs = client._client.post.call_args
        self.assertNotIn("environment_uuid", kwargs["json"])


class RegisterPayloadTests(unittest.TestCase):
    def _register_response(self) -> SimpleNamespace:
        return SimpleNamespace(
            status_code=200,
            json=lambda: {
                "success": True,
                "message": "ok",
                "uuid": "inst-uuid",
                "slug": "inst-slug",
            },
        )

    def test_register_includes_explicit_environment_uuid(self) -> None:
        client = _make_client()
        client._client.post.return_value = self._register_response()

        with patch.object(client_mod, "get_instance_uuid", return_value="inst-uuid"):
            client.register(
                profile_slug="default",
                slug="inst-slug",
                save_identity=False,
                environment_uuid=ENV_UUID,
            )

        _, kwargs = client._client.post.call_args
        self.assertEqual(kwargs["json"]["environment_uuid"], ENV_UUID)

    def test_register_defaults_environment_uuid_from_env_var(self) -> None:
        client = _make_client()
        client._client.post.return_value = self._register_response()

        with (
            patch.object(client_mod, "get_instance_uuid", return_value="inst-uuid"),
            patch.object(
                client_mod, "get_reserved_environment_uuid", return_value=ENV_UUID
            ),
        ):
            client.register(
                profile_slug="default", slug="inst-slug", save_identity=False
            )

        _, kwargs = client._client.post.call_args
        self.assertEqual(kwargs["json"]["environment_uuid"], ENV_UUID)

    def test_register_omits_environment_uuid_when_unset(self) -> None:
        client = _make_client()
        client._client.post.return_value = self._register_response()

        with (
            patch.object(client_mod, "get_instance_uuid", return_value="inst-uuid"),
            patch.object(
                client_mod, "get_reserved_environment_uuid", return_value=None
            ),
        ):
            client.register(
                profile_slug="default", slug="inst-slug", save_identity=False
            )

        _, kwargs = client._client.post.call_args
        self.assertNotIn("environment_uuid", kwargs["json"])


class ConfigTests(unittest.TestCase):
    def test_from_env_reads_reserved_environment_uuid(self) -> None:
        with patch.dict(
            "os.environ", {"CYBERWAVE_NODE_ENVIRONMENT_UUID": ENV_UUID}, clear=False
        ):
            config = CloudNodeConfig.from_env()
        self.assertEqual(config.reserved_environment_uuid, ENV_UUID)

    def test_from_dict_prefers_config_key_over_env_var(self) -> None:
        with patch.dict(
            "os.environ", {"CYBERWAVE_NODE_ENVIRONMENT_UUID": "other"}, clear=False
        ):
            config = CloudNodeConfig.from_dict(
                {"cyberwave-cloud-node": {"environment_uuid": ENV_UUID}}
            )
        self.assertEqual(config.reserved_environment_uuid, ENV_UUID)

    def test_default_is_none(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            config = CloudNodeConfig.from_env()
        self.assertIsNone(config.reserved_environment_uuid)


if __name__ == "__main__":
    unittest.main()
