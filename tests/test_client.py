import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from cyberwave_cloud_node import client as client_mod
from cyberwave_cloud_node.client import CloudNodeClient, CloudNodeClientError


class _FakeRequestError(Exception):
    """Stand-in for httpx.RequestError.

    Other test modules defensively replace ``httpx`` in ``sys.modules`` with a
    Mock, which can bind a non-exception ``httpx.RequestError`` into client.py
    depending on collection order. Patching the bound attribute to a real
    exception class keeps the connection-error test deterministic.
    """


def _make_client() -> CloudNodeClient:
    client = CloudNodeClient(
        base_url="http://localhost:8000",
        token="cw_test",
        workspace_slug="admins-workspace",
    )
    client._client = Mock()
    return client


class ValidateCredentialsTests(unittest.TestCase):
    def test_valid_token_passes(self) -> None:
        client = _make_client()
        client._client.get.return_value = SimpleNamespace(status_code=200)
        # Should not raise
        client.validate_credentials()
        client._client.get.assert_called_once()

    def test_rejected_token_raises_with_status(self) -> None:
        for code in (401, 403):
            client = _make_client()
            client._client.get.return_value = SimpleNamespace(status_code=code)
            with self.assertRaises(CloudNodeClientError) as ctx:
                client.validate_credentials()
            self.assertEqual(ctx.exception.status_code, code)

    def test_connection_error_raises_with_none_status(self) -> None:
        client = _make_client()
        client._client.get.side_effect = _FakeRequestError("unreachable")
        with patch.object(client_mod.httpx, "RequestError", _FakeRequestError):
            with self.assertRaises(CloudNodeClientError) as ctx:
                client.validate_credentials()
        self.assertIsNone(ctx.exception.status_code)

    def test_unexpected_status_raises_with_status(self) -> None:
        client = _make_client()
        client._client.get.return_value = SimpleNamespace(status_code=500)
        with self.assertRaises(CloudNodeClientError) as ctx:
            client.validate_credentials()
        self.assertEqual(ctx.exception.status_code, 500)


if __name__ == "__main__":
    unittest.main()
