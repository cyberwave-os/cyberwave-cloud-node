"""Main Cloud Node service implementation.

This module provides the CloudNode class that orchestrates:
- Running the install script on startup
- Registering with the Cyberwave backend
- Connecting to MQTT to receive workload commands
- Sending periodic heartbeats
- Processing inference and training workloads
- Handling graceful shutdown
"""

import asyncio
import json
import logging
import os
import shlex
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from cyberwave import Cyberwave  # type: ignore[import-untyped]

from .client import CloudNodeClient, CloudNodeClientError
from .config import CloudNodeConfig, get_api_token, get_api_url

logger = logging.getLogger(__name__)


class CloudNodeError(Exception):
    """Raised when a Cloud Node operation fails."""

    pass


@dataclass
class WorkloadResult:
    """Result of executing a workload (inference or training)."""

    success: bool
    output: Optional[str] = None
    error: Optional[str] = None
    return_code: Optional[int] = None


class CloudNode:
    """Cyberwave Cloud Node service.

    Manages the lifecycle of a cloud node instance:
    1. Runs install script (if configured)
    2. Registers with the Cyberwave backend
    3. Connects to MQTT to receive workload commands
    4. Sends periodic heartbeats
    5. Executes inference/training workloads
    6. Handles graceful shutdown

    Usage:
        # From cyberwave.yml config file
        node = CloudNode.from_config_file()
        node.run()

        # Or programmatically
        node = CloudNode(
            slug="my-gpu-node",
            config=CloudNodeConfig(
                inference="python inference.py --params {body}",
            ),
        )
        node.run()
    """

    def __init__(
        self,
        slug: str,
        config: CloudNodeConfig,
        client: Optional[CloudNodeClient] = None,
        working_dir: Optional[Path] = None,
    ):
        """Initialize a Cloud Node.

        Args:
            slug: Unique identifier for this node within the workspace
            config: Node configuration (from cyberwave.yml or programmatic)
            client: Optional pre-configured CloudNodeClient (for REST API calls)
            working_dir: Working directory for running commands. Defaults to current directory.
        """
        self.slug = slug
        self.config = config
        self.client = client or CloudNodeClient()
        self.working_dir = working_dir or Path.cwd()

        # Instance UUID will be assigned by the backend during registration.
        # Initialize as empty - will be populated after successful registration.
        self.instance_uuid: str = ""

        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # Cyberwave SDK client for MQTT
        self._cyberwave: Optional[Cyberwave] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

        # Determine topic prefix from CYBERWAVE_ENVIRONMENT variable
        env_value = os.getenv("CYBERWAVE_ENVIRONMENT", "").strip()
        topic_prefix = ""
        if not env_value or env_value.lower() == "production":
            topic_prefix = ""
        else:
            topic_prefix = env_value
        self._topic_prefix = topic_prefix

    @classmethod
    def from_config_file(
        cls,
        slug: str,
        config_path: Optional[Path] = None,
        client: Optional[CloudNodeClient] = None,
    ) -> "CloudNode":
        """Create a CloudNode from a cyberwave.yml config file.

        Args:
            slug: Unique identifier for this node
            config_path: Path to config file. Defaults to cyberwave.yml in current directory.
            client: Optional pre-configured CloudNodeClient

        Returns:
            CloudNode instance
        """
        config = CloudNodeConfig.from_file(config_path)
        working_dir = config_path.parent if config_path else Path.cwd()
        return cls(slug=slug, config=config, client=client, working_dir=working_dir)

    @classmethod
    def from_env(cls, slug: str, client: Optional[CloudNodeClient] = None) -> "CloudNode":
        """Create a CloudNode from environment variables.

        Args:
            slug: Unique identifier for this node
            client: Optional pre-configured CloudNodeClient

        Returns:
            CloudNode instance
        """
        config = CloudNodeConfig.from_env()
        return cls(slug=slug, config=config, client=client)

    def run(self) -> None:
        """Run the Cloud Node service synchronously.

        This is the main entry point for running the node. It will:
        1. Run the install script (if configured)
        2. Register with the backend
        3. Connect to MQTT and subscribe to command topics
        4. Start the heartbeat loop
        5. Block until shutdown signal is received
        """
        asyncio.run(self.run_async())

    async def run_async(self) -> None:
        """Run the Cloud Node service asynchronously."""
        self._running = True
        self._event_loop = asyncio.get_running_loop()
        self._setup_signal_handlers()

        try:
            # Step 1: Run install script
            if self.config.install_script:
                await self._run_install_script()

            # Step 2: Connect to MQTT
            await self._connect_mqtt()

            # Step 3: Register with backend (REST API)
            await self._register()

            # Step 4: Subscribe to command topics
            await self._subscribe_to_commands()

            # Step 5: Start heartbeat loop
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            logger.info(f"Cloud Node '{self.slug}' is running. Waiting for commands via MQTT...")

            # Wait for shutdown signal
            await self._shutdown_event.wait()

        except Exception as e:
            logger.error(f"Cloud Node error: {e}")
            await self._notify_failed(str(e))
            raise

        finally:
            await self._shutdown()

    async def _connect_mqtt(self) -> None:
        """Connect to the MQTT broker using the Cyberwave SDK."""
        logger.info(f"Connecting to MQTT broker at {self.config.mqtt_host}:{self.config.mqtt_port}")

        token = get_api_token()
        if not token:
            raise CloudNodeError(
                "API token is required. Set CYBERWAVE_API_TOKEN environment variable."
            )

        self._cyberwave = Cyberwave(
            token=token,
            base_url=get_api_url(),
            mqtt_host=self.config.mqtt_host,
            mqtt_port=self.config.mqtt_port,
            mqtt_username=self.config.mqtt_username,
            mqtt_password=self.config.mqtt_password,
        )

        if not self._cyberwave.mqtt.connected:
            self._cyberwave.mqtt.connect()

        logger.info("Connected to MQTT broker")

    async def _subscribe_to_commands(self) -> None:
        """Subscribe to MQTT topics for receiving workload commands."""
        if not self._cyberwave:
            raise CloudNodeError("MQTT client not connected")

        def on_command(data: Any) -> None:
            """Handle incoming command messages."""
            try:
                payload = data if isinstance(data, dict) else {}

                # Ignore status messages (responses)
                if "status" in payload:
                    return

                command = payload.get("command")
                request_id = payload.get("request_id")
                params = payload.get("params", {})

                if not command:
                    logger.warning("Command message missing 'command' field")
                    return

                logger.info(f"Received command: {command} (request_id: {request_id})")

                if self._event_loop is None:
                    logger.error("Event loop not available")
                    return

                if command == "inference":
                    asyncio.run_coroutine_threadsafe(
                        self._handle_inference(params, request_id), self._event_loop
                    )
                elif command == "training":
                    asyncio.run_coroutine_threadsafe(
                        self._handle_training(params, request_id), self._event_loop
                    )
                elif command == "status":
                    asyncio.run_coroutine_threadsafe(
                        self._handle_status(request_id), self._event_loop
                    )
                else:
                    logger.warning(f"Unknown command: {command}")
                    self._publish_response(
                        request_id, success=False, error=f"Unknown command: {command}"
                    )

            except Exception as e:
                logger.error(f"Error processing command: {e}", exc_info=True)

        # Subscribe to cloud-node command topic
        # Topic pattern: cyberwave/cloud-node/{instance_uuid}/command
        topic = f"{self._topic_prefix}cyberwave/cloud-node/{self.instance_uuid}/command"
        self._cyberwave.mqtt.subscribe(topic, on_command)
        logger.info(f"Subscribed to command topic: {topic}")

        # Also subscribe to topic using slug for easier addressing
        slug_topic = f"{self._topic_prefix}cyberwave/cloud-node/{self.slug}/command"
        self._cyberwave.mqtt.subscribe(slug_topic, on_command)
        logger.info(f"Subscribed to command topic: {slug_topic}")

    def _publish_response(
        self,
        request_id: Optional[str],
        success: bool,
        output: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        """Publish a response message back via MQTT."""
        if not self._cyberwave:
            return

        response = {
            "status": "ok" if success else "error",
            "request_id": request_id,
            "slug": self.slug,
            "instance_uuid": self.instance_uuid,
        }

        if output is not None:
            response["output"] = output
        if error is not None:
            response["error"] = error

        # Publish to response topic
        topic = f"{self._topic_prefix}cyberwave/cloud-node/{self.instance_uuid}/response"
        try:
            self._cyberwave.mqtt._client.publish(topic, response)
            logger.debug(f"Published response to {topic}")
        except Exception as e:
            logger.error(f"Failed to publish response: {e}")

    async def _handle_inference(self, params: dict, request_id: Optional[str]) -> None:
        """Handle an inference command."""
        if not self.config.inference:
            self._publish_response(
                request_id,
                success=False,
                error="Inference command not configured in cyberwave.yml",
            )
            return

        result = await self.execute_workload("inference", params)
        self._publish_response(
            request_id,
            success=result.success,
            output=result.output,
            error=result.error,
        )

    async def _handle_training(self, params: dict, request_id: Optional[str]) -> None:
        """Handle a training command."""
        if not self.config.training:
            self._publish_response(
                request_id,
                success=False,
                error="Training command not configured in cyberwave.yml",
            )
            return

        result = await self.execute_workload("training", params)
        self._publish_response(
            request_id,
            success=result.success,
            output=result.output,
            error=result.error,
        )

    async def _handle_status(self, request_id: Optional[str]) -> None:
        """Handle a status query."""
        self._publish_response(
            request_id,
            success=True,
            output=json.dumps(
                {
                    "slug": self.slug,
                    "instance_uuid": self.instance_uuid,
                    "profile_slug": self.config.profile_slug,
                    "provider": self.config.provider,
                    "has_inference": bool(self.config.inference),
                    "has_training": bool(self.config.training),
                }
            ),
        )

    async def _run_install_script(self) -> None:
        """Run the install script from cyberwave.yml."""
        if not self.config.install_script:
            return

        logger.info(f"Running install script: {self.config.install_script}")

        try:
            result = await self._run_command(self.config.install_script)
            if not result.success:
                raise CloudNodeError(
                    f"Install script failed with code {result.return_code}: {result.error}"
                )
            logger.info("Install script completed successfully")

        except Exception as e:
            logger.error(f"Install script failed: {e}")
            raise CloudNodeError(f"Install script failed: {e}") from e

    async def _register(self) -> None:
        """Register this node with the Cyberwave backend.

        Retries registration every 10 seconds if it fails (e.g., if the instance
        is in a transient state like 'terminating').

        The backend owns the UUID and slug - after successful registration,
        this method updates self.instance_uuid and self.slug with the values
        returned by the backend.
        """
        logger.info(f"Registering Cloud Node '{self.slug}'")

        while self._running:
            try:
                response = self.client.register(
                    endpoint=f"mqtt://{self.slug}",  # MQTT-based endpoint
                    profile_slug=self.config.profile_slug,
                    slug=self.slug,  # Optional hint, backend may generate different one
                    provider=self.config.provider,
                )

                # Update instance identity with values from backend
                # The backend is the owner of UUID and slug
                self.instance_uuid = response.uuid
                self.slug = response.slug

                logger.info(f"Registration successful: {response.message}")
                logger.info(f"Instance UUID: {self.instance_uuid}, Slug: {self.slug}")
                return

            except CloudNodeClientError as e:
                logger.warning(f"Registration failed: {e}. Retrying in 10 seconds...")
                await asyncio.sleep(10)

        # If we exit the loop because _running became False, raise an error
        raise CloudNodeError("Registration aborted due to shutdown")

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to the backend."""
        logger.info(f"Starting heartbeat loop (interval: {self.config.heartbeat_interval}s)")

        while self._running:
            try:
                response = self.client.heartbeat(slug=self.slug)
                logger.debug(f"Heartbeat sent: {response.message}")

            except CloudNodeClientError as e:
                logger.warning(f"Heartbeat failed: {e}")
                # Continue trying - the backend might be temporarily unavailable

            await asyncio.sleep(self.config.heartbeat_interval)

    async def execute_workload(
        self,
        workload_type: str,
        params: dict,
    ) -> WorkloadResult:
        """Execute an inference or training workload.

        Args:
            workload_type: Either "inference" or "training"
            params: Parameters to pass to the command

        Returns:
            WorkloadResult with output or error
        """
        command_template = (
            self.config.inference if workload_type == "inference" else self.config.training
        )

        if not command_template:
            return WorkloadResult(
                success=False,
                error=f"{workload_type} command not configured",
            )

        # Replace {body} placeholder with JSON-encoded params
        params_json = json.dumps(params)
        command = command_template.replace("{body}", shlex.quote(params_json))

        logger.info(f"Executing {workload_type} workload: {command}")
        return await self._run_command(command)

    async def _run_command(self, command: str) -> WorkloadResult:
        """Run a shell command and return the result.

        Args:
            command: Shell command to execute

        Returns:
            WorkloadResult with output and status
        """
        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.working_dir,
            )

            stdout, stderr = await process.communicate()
            stdout_str = stdout.decode("utf-8") if stdout else ""
            stderr_str = stderr.decode("utf-8") if stderr else ""

            if process.returncode == 0:
                return WorkloadResult(
                    success=True,
                    output=stdout_str,
                    return_code=0,
                )
            else:
                return WorkloadResult(
                    success=False,
                    output=stdout_str,
                    error=stderr_str or f"Command failed with code {process.returncode}",
                    return_code=process.returncode,
                )

        except Exception as e:
            return WorkloadResult(
                success=False,
                error=str(e),
            )

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self._running = False
            # Set the event in a thread-safe way
            asyncio.get_event_loop().call_soon_threadsafe(self._shutdown_event.set)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def _notify_failed(self, error: str) -> None:
        """Notify the backend that this node has failed."""
        try:
            self.client.failed(error=error, slug=self.slug)
            logger.info("Backend notified of failure")
        except CloudNodeClientError as e:
            logger.error(f"Failed to notify backend of failure: {e}")

    async def _shutdown(self) -> None:
        """Perform graceful shutdown."""
        logger.info("Shutting down Cloud Node...")
        self._running = False

        # Cancel background tasks
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Notify backend of termination via REST API
        try:
            self.client.terminated(slug=self.slug)
            logger.info("Backend notified of termination")
        except CloudNodeClientError as e:
            logger.error(f"Failed to notify backend of termination: {e}")

        # Disconnect MQTT
        if self._cyberwave:
            try:
                if self._cyberwave.mqtt:
                    self._cyberwave.mqtt.disconnect()
                self._cyberwave.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting from MQTT: {e}")

        # Close REST client
        self.client.close()

        logger.info("Cloud Node shutdown complete")
