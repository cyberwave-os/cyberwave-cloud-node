"""Per-workload token credential scoping for module-dispatch workers.

The subprocess dispatch path injects the backend-minted, workload-scoped token
as the worker's ``CYBERWAVE_API_KEY``. Module dispatch runs the worker in the
node's own process, so it must instead swap the scoped token into the process
environment for the duration of the call (``_scoped_api_credential`` /
``_run_module_fn``) so the worker's SDK clients authenticate with the
short-lived token rather than the node's service token.

The dependency stubs below mirror ``test_controller_hygiene.py``.
"""

# ruff: noqa: E402

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

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

from cyberwave_cloud_node.cloud_node import (  # noqa: E402
    CloudNode,
    _scoped_api_credential,
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


class ScopedApiCredentialTests(unittest.TestCase):
    def test_sets_both_credentials_then_restores(self) -> None:
        with patch.dict(
            os.environ,
            {"CYBERWAVE_API_KEY": "service-tok", "CYBERWAVE_MQTT_PASSWORD": "svc-pw"},
            clear=False,
        ):
            with _scoped_api_credential("workload-tok"):
                # Inside: both the REST token and the broker password are the
                # scoped token, so workers reading either var stay compatible.
                self.assertEqual(os.environ["CYBERWAVE_API_KEY"], "workload-tok")
                self.assertEqual(os.environ["CYBERWAVE_MQTT_PASSWORD"], "workload-tok")
            # After: the node's own credentials are restored verbatim.
            self.assertEqual(os.environ["CYBERWAVE_API_KEY"], "service-tok")
            self.assertEqual(os.environ["CYBERWAVE_MQTT_PASSWORD"], "svc-pw")

    def test_restores_absent_vars_to_absent(self) -> None:
        for var in ("CYBERWAVE_API_KEY", "CYBERWAVE_MQTT_PASSWORD"):
            os.environ.pop(var, None)
        with _scoped_api_credential("workload-tok"):
            self.assertEqual(os.environ["CYBERWAVE_API_KEY"], "workload-tok")
            self.assertEqual(os.environ["CYBERWAVE_MQTT_PASSWORD"], "workload-tok")
        # Vars that did not exist before must not linger afterwards.
        self.assertNotIn("CYBERWAVE_API_KEY", os.environ)
        self.assertNotIn("CYBERWAVE_MQTT_PASSWORD", os.environ)

    def test_restores_even_when_fn_raises(self) -> None:
        with patch.dict(os.environ, {"CYBERWAVE_API_KEY": "service-tok"}, clear=False):
            with self.assertRaises(RuntimeError):
                with _scoped_api_credential("workload-tok"):
                    raise RuntimeError("boom")
            self.assertEqual(os.environ["CYBERWAVE_API_KEY"], "service-tok")


class RunModuleFnTests(unittest.TestCase):
    def test_worker_observes_scoped_token_and_env_restored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = _make_node(tmp_dir)
            seen: dict[str, str | None] = {}

            def worker(**params):
                seen["token"] = os.environ.get("CYBERWAVE_API_KEY")
                return {"ok": True}

            with patch.dict(
                os.environ, {"CYBERWAVE_API_KEY": "service-tok"}, clear=False
            ):
                loop = asyncio.new_event_loop()
                try:
                    result = loop.run_until_complete(
                        node._run_module_fn(loop, worker, {}, "workload-tok")
                    )
                finally:
                    loop.close()
                # The worker ran with the scoped token...
                self.assertEqual(seen["token"], "workload-tok")
                self.assertEqual(result, {"ok": True})
                # ...and the node's own credential is intact afterwards.
                self.assertEqual(os.environ["CYBERWAVE_API_KEY"], "service-tok")

    def test_without_token_worker_inherits_ambient_credential(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = _make_node(tmp_dir)
            seen: dict[str, str | None] = {}

            def worker(**params):
                seen["token"] = os.environ.get("CYBERWAVE_API_KEY")
                return "done"

            with patch.dict(
                os.environ, {"CYBERWAVE_API_KEY": "service-tok"}, clear=False
            ):
                loop = asyncio.new_event_loop()
                try:
                    result = loop.run_until_complete(
                        node._run_module_fn(loop, worker, {}, None)
                    )
                finally:
                    loop.close()
                # No token minted → today's fallback: ambient node credential.
                self.assertEqual(seen["token"], "service-tok")
                self.assertEqual(result, "done")


if __name__ == "__main__":
    unittest.main()
