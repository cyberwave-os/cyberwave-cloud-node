"""Main Cloud Node service implementation.

This module provides the CloudNode class that orchestrates:
- Running the install script on startup
- Registering with the Cyberwave backend
- Connecting to MQTT to receive workload commands
- Sending periodic heartbeats
- Processing inference and training workloads as independent OS processes
- Handling graceful shutdown without interrupting running workloads

Workloads (inference/training) run as detached subprocesses that survive
Cloud Node restarts. The node tracks process IDs to determine capacity
and collect results when workloads complete.
"""

import asyncio
import json
import logging
import os
import shlex
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
import psutil

from .client import CloudNodeClient, CloudNodeClientError
from .config import CloudNodeConfig, get_api_token, get_instance_uuid, get_instance_slug
from .mqtt import CloudNodeMQTTClient, MQTTError

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


@dataclass
class ActiveWorkload:
    """Represents an active workload running as an OS process."""

    pid: int
    request_id: Optional[str]
    workload_type: str  # "inference" or "training"
    started_at: float
    command: str
    stdout_file: Path
    stderr_file: Path
    params: Dict[str, Any] = field(default_factory=dict)
    workload_uuid: Optional[str] = None  # UUID of the workload from backend


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
            config=CloudNodeConfig(
                inference="python inference.py --params {body}",
            ),
            slug="my-gpu-node",  # Optional hint, backend assigns actual slug
        )
        node.run()
    """

    def __init__(
        self,
        config: CloudNodeConfig,
        slug: Optional[str] = None,
        client: Optional[CloudNodeClient] = None,
        working_dir: Optional[Path] = None,
    ):
        """Initialize a Cloud Node.

        Args:
            config: Node configuration (from cyberwave.yml or programmatic)
            slug: Optional slug hint for this node (backend assigns the actual slug)
            client: Optional pre-configured CloudNodeClient (for REST API calls)
            working_dir: Working directory for running commands. Defaults to current directory.
        """
        # Slug can come from environment, parameter, or will be assigned by backend during registration
        self.slug: str = slug or get_instance_slug() or ""
        self.config = config
        self.client = client or CloudNodeClient()
        self.working_dir = working_dir or Path.cwd()

        # Instance UUID can come from environment (e.g., set by Terraform) or will be
        # assigned by the backend during registration. Check environment first.
        self.instance_uuid: str = get_instance_uuid() or ""

        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._log_flush_task: Optional[asyncio.Task] = None
        self._mqtt_reconnect_task: Optional[asyncio.Task] = None
        self._workload_monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # Track active workloads by PID (process-based, survives restarts)
        self._active_workloads: Dict[int, ActiveWorkload] = {}
        self._workload_lock = asyncio.Lock()

        # Directory for workload output files
        self._workload_output_dir = Path.home() / ".cyberwave" / "workload_logs"
        self._workload_output_dir.mkdir(parents=True, exist_ok=True)

        # Log buffers for cloud node itself (not workload output)
        self._stdout_buffer: list[str] = []
        self._stderr_buffer: list[str] = []
        self._log_buffer_lock = asyncio.Lock()
        self._log_flush_interval = 30  # seconds

        # MQTT v5 client
        self._mqtt_client: Optional[CloudNodeMQTTClient] = None
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
        config_path: Optional[Path] = None,
        slug: Optional[str] = None,
        client: Optional[CloudNodeClient] = None,
    ) -> "CloudNode":
        """Create a CloudNode from a cyberwave.yml config file.

        Args:
            config_path: Path to config file. Defaults to cyberwave.yml in current directory.
            slug: Optional slug hint for this node (backend assigns the actual slug)
            client: Optional pre-configured CloudNodeClient

        Returns:
            CloudNode instance
        """
        config = CloudNodeConfig.from_file(config_path)
        working_dir = config_path.parent if config_path else Path.cwd()
        return cls(config=config, slug=slug, client=client, working_dir=working_dir)

    @classmethod
    def from_env(
        cls,
        slug: Optional[str] = None,
        client: Optional[CloudNodeClient] = None,
    ) -> "CloudNode":
        """Create a CloudNode from environment variables.

        Args:
            slug: Optional slug hint for this node (backend assigns the actual slug)
            client: Optional pre-configured CloudNodeClient

        Returns:
            CloudNode instance
        """
        config = CloudNodeConfig.from_env()
        return cls(config=config, slug=slug, client=client)

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

            await self._recover_local_workloads()

            # Step 3: Register with backend via MQTT
            await self._register_via_mqtt()

            # Ensure instance_uuid is set after registration
            if not self.instance_uuid:
                raise CloudNodeError(
                    "Registration completed but instance_uuid is not set. "
                    "Cannot start heartbeat loop without instance UUID."
                )

            # Step 4: Subscribe to command topics
            await self._subscribe_to_commands()

            # Step 5: Start background monitoring loops
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._log_flush_task = asyncio.create_task(self._log_flush_loop())
            self._mqtt_reconnect_task = asyncio.create_task(self._mqtt_reconnect_loop())
            self._workload_monitor_task = asyncio.create_task(self._workload_monitor_loop())

            logger.info(
                f"Cloud Node '{self.slug}' (uuid: {self.instance_uuid}) is running. "
                "Waiting for commands via MQTT..."
            )

            # Wait for shutdown signal
            await self._shutdown_event.wait()

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down gracefully")
            # Don't mark as failed for keyboard interrupt
        except Exception as e:
            logger.error(f"Cloud Node error: {e}", exc_info=True)
            try:
                await self._notify_failed(str(e))
            except Exception as notify_error:
                logger.error(f"Failed to notify backend of failure: {notify_error}")
            # Don't re-raise - let shutdown happen gracefully

        finally:
            try:
                await self._shutdown()
            except Exception as shutdown_error:
                logger.error(f"Error during shutdown: {shutdown_error}", exc_info=True)

    async def _connect_mqtt(self) -> None:
        """Connect to the MQTT broker using MQTT v5."""
        logger.info(f"Connecting to MQTT broker at {self.config.mqtt_host}:{self.config.mqtt_port}")

        token = get_api_token()
        if not token:
            raise CloudNodeError(
                "API token is required. Set CYBERWAVE_API_TOKEN environment variable."
            )

        self._mqtt_client = CloudNodeMQTTClient(
            host=self.config.mqtt_host,
            port=self.config.mqtt_port,
            username=self.config.mqtt_username,
            password=self.config.mqtt_password,
            keepalive=60,
            topic_prefix=self._topic_prefix,
        )

        try:
            await self._mqtt_client.connect()
            logger.info("Connected to MQTT broker")
        except MQTTError as e:
            logger.error(f"Failed to connect to MQTT broker: {e}", exc_info=True)
            raise CloudNodeError(f"MQTT connection failed: {e}") from e

    async def _subscribe_to_commands(self) -> None:
        """Subscribe to MQTT topics for receiving workload commands."""
        if not self._mqtt_client:
            raise CloudNodeError("MQTT client not connected")

        def on_command(payload: dict[str, Any], properties) -> None:
            """Handle incoming command messages."""
            try:
                # Ignore status messages (responses)
                if "status" in payload:
                    return

                command = payload.get("command")
                request_id = payload.get("request_id")
                params = payload.get("params", {})

                if not command:
                    logger.warning("Command message missing 'command' field")
                    return

                # Reject new commands during shutdown
                if not self._running:
                    logger.warning(
                        f"Rejecting command {command} (request_id: {request_id}) - "
                        "node is shutting down"
                    )
                    self._publish_response(
                        request_id,
                        success=False,
                        error="Cloud node is shutting down and not accepting new commands",
                    )
                    return

                logger.info(f"Received command: {command} (request_id: {request_id})")

                if self._event_loop is None:
                    logger.error("Event loop not available")
                    return

                match command:
                    case "inference":
                        if self._event_loop:
                            asyncio.run_coroutine_threadsafe(
                                self._handle_inference(params, request_id), self._event_loop
                            )
                    case "training":
                        if self._event_loop:
                            asyncio.run_coroutine_threadsafe(
                                self._handle_training(params, request_id), self._event_loop
                            )
                    case "status":
                        if self._event_loop:
                            asyncio.run_coroutine_threadsafe(
                                self._handle_status(request_id), self._event_loop
                            )
                    case "workload_received":
                        if self._event_loop:
                            asyncio.run_coroutine_threadsafe(
                                self._handle_workload_received(params, request_id), self._event_loop
                            )
                    case "cancel":
                        if self._event_loop:
                            asyncio.run_coroutine_threadsafe(
                                self._handle_cancel(params, request_id), self._event_loop
                            )
                    case _:
                        logger.warning(f"Unknown command: {command}")
                        self._publish_response(
                            request_id, success=False, error=f"Unknown command: {command}"
                        )
            except Exception as e:
                logger.error(f"Error processing command: {e}", exc_info=True)

        # Subscribe to cloud-node command topic
        # Topic pattern: cyberwave/cloud-node/{instance_uuid}/command
        topic = f"{self._topic_prefix}cyberwave/cloud-node/{self.instance_uuid}/command"
        try:
            self._mqtt_client.subscribe_command(topic, on_command)
            logger.info(f"Subscribed to command topic: {topic}")
        except MQTTError as e:
            logger.error(f"Failed to subscribe to MQTT topic {topic}: {e}", exc_info=True)
            raise CloudNodeError(f"Failed to subscribe to command topic: {e}") from e

    def _publish_message(self, topic_type: str, message: dict) -> None:
        """Publish a message to an MQTT topic."""
        if not self._mqtt_client:
            return
        topic = f"{self._topic_prefix}cyberwave/cloud-node/{self.instance_uuid}/{topic_type}"
        try:
            # Use asyncio to run the async publish in a non-blocking way
            if self._event_loop:
                asyncio.run_coroutine_threadsafe(
                    self._mqtt_client.publish_message(topic, message), self._event_loop
                )
            logger.debug(f"Published message to topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to publish message to {topic}: {e}")
            raise CloudNodeError(f"Failed to publish message to {topic}: {e}") from e

    def _publish_response(
        self,
        request_id: Optional[str],
        success: bool,
        output: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        """Publish a response message back via MQTT."""

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

        self._publish_message("response", response)

    def _is_node_busy(self) -> bool:
        """Check if the node is currently running any workloads."""
        return len(self._active_workloads) > 0

    def _get_active_workload_count(self) -> int:
        """Get count of active workloads."""
        return len(self._active_workloads)

    async def _handle_inference(self, params: dict, request_id: Optional[str]) -> None:
        """Handle an inference command by spawning a detached process."""
        try:
            if not self.config.inference:
                self._publish_response(
                    request_id,
                    success=False,
                    error="Inference command not configured in cyberwave.yml",
                )
                return

            # Check if node is busy (optional: could support multiple concurrent workloads)
            if self._is_node_busy():
                logger.warning(
                    f"Node is busy with {self._get_active_workload_count()} active workload(s). "
                    f"Rejecting inference request {request_id}"
                )
                self._publish_response(
                    request_id,
                    success=False,
                    error=f"Node is busy with {self._get_active_workload_count()} active workload(s)",
                )
                return

            logger.info(f"Starting inference workload (request_id: {request_id})")
            await self._spawn_workload_process("inference", params, request_id)

        except Exception as e:
            logger.error(
                f"Error starting inference command (request_id: {request_id}): {e}", exc_info=True
            )
            self._publish_response(
                request_id,
                success=False,
                error=f"Failed to start inference: {str(e)}",
            )

    async def _handle_training(self, params: dict, request_id: Optional[str]) -> None:
        """Handle a training command by spawning a detached process."""
        try:
            if not self.config.training:
                self._publish_response(
                    request_id,
                    success=False,
                    error="Training command not configured in cyberwave.yml",
                )
                return

            # Check if node is busy (optional: could support multiple concurrent workloads)
            if self._is_node_busy():
                logger.warning(
                    f"Node is busy with {self._get_active_workload_count()} active workload(s). "
                    f"Rejecting training request {request_id}"
                )
                self._publish_response(
                    request_id,
                    success=False,
                    error=f"Node is busy with {self._get_active_workload_count()} active workload(s)",  # noqa: E501
                )
                return

            logger.info(f"Starting training workload (request_id: {request_id})")
            await self._spawn_workload_process("training", params, request_id)

        except Exception as e:
            logger.error(
                f"Error starting training command (request_id: {request_id}): {e}", exc_info=True
            )
            self._publish_response(
                request_id,
                success=False,
                error=f"Failed to start training: {str(e)}",
            )

    async def _handle_status(self, request_id: Optional[str]) -> None:
        """Handle a status query."""
        async with self._workload_lock:
            active_workloads_info = [
                {
                    "pid": w.pid,
                    "type": w.workload_type,
                    "request_id": w.request_id,
                    "running_for_seconds": int(time.time() - w.started_at),
                }
                for w in self._active_workloads.values()
            ]

        self._publish_response(
            request_id,
            success=True,
            output=json.dumps(
                {
                    "slug": self.slug,
                    "instance_uuid": self.instance_uuid,
                    "profile_slug": self.config.profile_slug,
                    "has_inference": bool(self.config.inference),
                    "has_training": bool(self.config.training),
                    "is_busy": self._is_node_busy(),
                    "active_workloads": active_workloads_info,
                }
            ),
        )

    async def _handle_workload_received(self, params: dict, request_id: Optional[str]) -> None:
        """Handle a workload_received notification.

        This is called when a workload is assigned to this instance.
        The params contain the full workload details including workload_uuid,
        profile_slug, status, command_type, and command_params.
        """
        workload_uuid = params.get("workload_uuid")
        command_type = params.get("command_type")
        command_params = params.get("command_params") or {}

        logger.info(
            f"Workload {workload_uuid} assigned to this instance "
            f"(command_type={command_type}, status={params.get('status')})"
        )

        # If there's a command_type, execute it
        # Pass workload_uuid through to command_params so it's available when spawning the process
        if workload_uuid:
            command_params["workload_uuid"] = workload_uuid

        match command_type:
            case "inference":
                await self._handle_inference(command_params, request_id)
            case "training":
                await self._handle_training(command_params, request_id)
            case _:
                # No specific command type - just acknowledge receipt
                logger.info(f"Workload {workload_uuid} received (no command_type specified)")
                self._publish_response(
                    request_id,
                    success=True,
                    output=f"Workload {workload_uuid} received and acknowledged",
                )

    async def _handle_cancel(self, params: dict, request_id: Optional[str]) -> None:
        """Handle a cancel command to terminate a running workload.

        Params can include:
        - pid: Process ID to cancel (direct)
        - workload_request_id: Request ID of the workload to cancel
        - signal: Signal to send (default: SIGTERM, options: SIGTERM, SIGKILL)

        Examples:
            {"pid": 12345}
            {"workload_request_id": "abc-123"}
            {"pid": 12345, "signal": "SIGKILL"}
        """
        try:
            target_pid = params.get("pid")
            target_request_id = params.get("workload_request_id")
            signal_name = params.get("signal", "SIGTERM")

            # Validate signal
            if signal_name not in ["SIGTERM", "SIGKILL", "SIGINT"]:
                self._publish_response(
                    request_id,
                    success=False,
                    error=f"Invalid signal: {signal_name}. Must be SIGTERM, SIGKILL, or SIGINT",
                )
                return

            # Find the workload to cancel
            workload = None
            async with self._workload_lock:
                if target_pid:
                    workload = self._active_workloads.get(target_pid)
                elif target_request_id:
                    # Find by request_id
                    for w in self._active_workloads.values():
                        if w.request_id == target_request_id:
                            workload = w
                            break

            if not workload:
                error_msg = "No active workload found"
                if target_pid:
                    error_msg += f" with PID {target_pid}"
                if target_request_id:
                    error_msg += f" with request_id {target_request_id}"

                logger.warning(error_msg)
                self._publish_response(request_id, success=False, error=error_msg)
                return

            # Cancel the workload
            success, message = await self._cancel_workload(workload, signal_name)

            if success:
                logger.info(
                    f"Successfully cancelled workload PID {workload.pid} "
                    f"({workload.workload_type}, request_id: {workload.request_id})"
                )
                self._publish_response(
                    request_id,
                    success=True,
                    output=json.dumps(
                        {
                            "message": message,
                            "pid": workload.pid,
                            "workload_type": workload.workload_type,
                            "workload_request_id": workload.request_id,
                            "signal": signal_name,
                        }
                    ),
                )
            else:
                logger.error(f"Failed to cancel workload PID {workload.pid}: {message}")
                self._publish_response(request_id, success=False, error=message)

        except Exception as e:
            logger.error(f"Error handling cancel command: {e}", exc_info=True)
            self._publish_response(
                request_id,
                success=False,
                error=f"Failed to cancel workload: {str(e)}",
            )

    async def _cancel_workload(
        self, workload: ActiveWorkload, signal_name: str = "SIGTERM"
    ) -> tuple[bool, str]:
        """Cancel a running workload by sending a signal to its process.

        Args:
            workload: The ActiveWorkload to cancel
            signal_name: Signal to send (SIGTERM, SIGKILL, SIGINT)

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Check if process is still alive
            if not self._is_process_alive(workload.pid):
                # Already dead, clean up
                async with self._workload_lock:
                    self._active_workloads.pop(workload.pid, None)
                return True, f"Process {workload.pid} was already terminated"

            # Get the process
            process = psutil.Process(workload.pid)

            # Send the signal
            signal_map = {
                "SIGTERM": signal.SIGTERM,
                "SIGKILL": signal.SIGKILL,
                "SIGINT": signal.SIGINT,
            }
            sig = signal_map[signal_name]

            logger.info(
                f"Sending {signal_name} to workload PID {workload.pid} ({workload.workload_type})"
            )
            process.send_signal(sig)

            # For SIGKILL, wait briefly to confirm termination
            if signal_name == "SIGKILL":
                try:
                    process.wait(timeout=2)
                except psutil.TimeoutExpired:
                    return False, f"Process {workload.pid} did not terminate after SIGKILL"

            # Remove from active workloads
            async with self._workload_lock:
                self._active_workloads.pop(workload.pid, None)

            # Publish cancellation notification to the workload's original request
            self._publish_response(
                workload.request_id,
                success=False,
                error=f"Workload cancelled by {signal_name}",
                output=json.dumps(
                    {
                        "status": "cancelled",
                        "signal": signal_name,
                        "pid": workload.pid,
                    }
                ),
            )

            return True, f"Workload cancelled with {signal_name}"

        except psutil.NoSuchProcess:
            # Process already gone
            async with self._workload_lock:
                self._active_workloads.pop(workload.pid, None)
            return True, f"Process {workload.pid} no longer exists"

        except psutil.AccessDenied:
            return False, f"Permission denied to send signal to process {workload.pid}"

        except Exception as e:
            return False, f"Failed to cancel workload: {str(e)}"

    async def _run_install_script(self) -> None:
        """Run the install script from cyberwave.yml."""
        if not self.config.install_script:
            return

        logger.info(f"Running install script: {self.config.install_script}")

        try:
            # Install scripts run directly with login shell to ensure conda is available
            result = await self._run_command(self.config.install_script, workload_type="install")
            if not result.success:
                error_msg = f"Install script failed with code {result.return_code}"
                if result.error:
                    error_msg += f": {result.error}"
                if result.output:
                    logger.error(f"Install script stdout: {result.output}")
                raise CloudNodeError(error_msg)
            logger.info("Install script completed successfully")

        except Exception as e:
            logger.error(f"Install script failed: {e}")
            raise CloudNodeError(f"Install script failed: {e}") from e

    async def _spawn_workload_process(
        self, workload_type: str, params: dict, request_id: Optional[str]
    ) -> None:
        """Spawn a detached workload process that survives Cloud Node restarts.

        The process runs independently and is tracked by PID. Output is streamed
        to log files and will be collected when the process completes.
        """
        # Build command
        command_template = (
            self.config.inference if workload_type == "inference" else self.config.training
        )

        # Create output files for this workload
        timestamp = int(time.time())
        workload_id = f"{workload_type}_{request_id or timestamp}"
        stdout_file = self._workload_output_dir / f"{workload_id}.stdout.log"
        stderr_file = self._workload_output_dir / f"{workload_id}.stderr.log"

        # For large payloads, write params to a temp file and pass the filename
        # This avoids "Argument list too long" errors
        params_json = json.dumps(params)
        params_file = self._workload_output_dir / f"{workload_id}.params.json"
        params_file.write_text(params_json)

        # Replace {body} with the path to the params file (wrapped in $(cat ...))
        # This allows the script to read the params from the file
        command = command_template.replace("{body}", f"$(cat {shlex.quote(str(params_file))})")

        logger.info(f"Spawning {workload_type} process with params file: {params_file}")
        logger.info(f"Command: {command}")
        logger.info(f"Output: {stdout_file}")
        logger.info(f"Errors: {stderr_file}")

        try:
            # Spawn detached process
            # Use bash -c to ensure proper shell expansion
            shell_command = f"bash -c {shlex.quote(command)}"

            with stdout_file.open("w") as stdout_f, stderr_file.open("w") as stderr_f:
                process = subprocess.Popen(
                    shell_command,
                    shell=True,
                    stdout=stdout_f,
                    stderr=stderr_f,
                    cwd=self.working_dir,
                    start_new_session=True,  # Detach from parent process (cross-platform)
                )

            pid = process.pid
            logger.info(
                f"Started {workload_type} workload in background process PID {pid} "
                f"(request_id: {request_id})"
            )

            # Extract workload_uuid from params if present (passed from backend)
            workload_uuid = params.get("workload_uuid")

            # Track the workload
            workload = ActiveWorkload(
                pid=pid,
                request_id=request_id,
                workload_type=workload_type,
                started_at=time.time(),
                command=command,
                stdout_file=stdout_file,
                stderr_file=stderr_file,
                params=params,
                workload_uuid=workload_uuid,
            )

            async with self._workload_lock:
                self._active_workloads[pid] = workload

            await self._save_workload_state()

            # Publish initial acknowledgment
            if not self._mqtt_client:
                logger.warning("MQTT client not available, skipping workload status update")
                return
            if not workload_uuid:
                logger.warning("Workload UUID not available, skipping workload status update")
                return
            await self._mqtt_client.update_workload_status(
                workload_uuid=workload_uuid,
                status="running",
                additional_data={
                    "message": f"{workload_type} started in background",
                    "pid": pid,
                },
            )
            return

        except Exception as e:
            logger.error(f"Failed to spawn {workload_type} process: {e}", exc_info=True)
            raise

    async def _save_workload_state(self):
        """Persist active workloads to local file."""
        state_file = Path.home() / ".cyberwave" / "active_workloads.json"

        async with self._workload_lock:
            state = {
                str(pid): {
                    "pid": pid,
                    "request_id": w.request_id,
                    "workload_type": w.workload_type,
                    "started_at": w.started_at,
                    "command": w.command,
                    "stdout_file": str(w.stdout_file),
                    "stderr_file": str(w.stderr_file),
                    "params": w.params,
                    "workload_uuid": w.workload_uuid,
                }
                for pid, w in self._active_workloads.items()
            }

        try:
            state_file.write_text(json.dumps(state, indent=2))
            logger.debug(f"Saved workload state ({len(state)} workload(s))")
        except Exception as e:
            logger.error(f"Failed to save workload state: {e}")
            raise

    async def _recover_local_workloads(self):
        """Load workload state from local file and reattach to running processes."""
        state_file = Path.home() / ".cyberwave" / "active_workloads.json"

        if not state_file.exists():
            logger.info("No workload state file found")
            return

        try:
            state = json.loads(state_file.read_text())
            logger.info(f"Found {len(state)} tracked workload(s) from previous session")

            for pid_str, data in state.items():
                pid = int(pid_str)

                # Check if process still exists
                if self._is_process_alive(pid):
                    # Reattach tracking
                    workload = ActiveWorkload(
                        pid=pid,
                        request_id=data["request_id"],
                        workload_type=data["workload_type"],
                        started_at=data["started_at"],
                        command=data["command"],
                        stdout_file=Path(data["stdout_file"]),
                        stderr_file=Path(data["stderr_file"]),
                        params=data.get("params", {}),
                        workload_uuid=data.get("workload_uuid"),
                    )

                    self._active_workloads[pid] = workload
                    logger.info(
                        f"Recovered workload PID {pid} ({workload.workload_type}, "
                        f"running for {int(time.time() - workload.started_at)}s)"
                    )
                else:
                    # Process completed while we were down - handle orphaned results
                    logger.info(f"Workload PID {pid} completed while service was down")
                    # await self._handle_orphaned_completion(data)

            logger.info(f"Recovered {len(self._active_workloads)} active workload(s)")

        except Exception as e:
            logger.error(f"Failed to recover workload state: {e}", exc_info=True)
            raise

    async def _workload_monitor_loop(self) -> None:
        """Monitor active workload processes and collect results when they complete.

        This loop runs in the background and:
        1. Checks if workload processes are still alive
        2. Collects results when processes complete
        3. Publishes results back via MQTT
        4. Cleans up completed workloads
        """
        logger.info("Starting workload monitor loop")
        check_interval = 5  # Check every 5 seconds

        while self._running:
            await asyncio.sleep(check_interval)

            async with self._workload_lock:
                pids_to_check = list(self._active_workloads.keys())

            for pid in pids_to_check:
                try:
                    # Check if process is still alive
                    if self._is_process_alive(pid):
                        continue

                    # Process completed - collect results
                    async with self._workload_lock:
                        workload = self._active_workloads.get(pid)

                    if not workload:
                        continue

                    logger.info(
                        f"Workload process PID {pid} ({workload.workload_type}) completed. "
                        f"Collecting results..."
                    )

                    await self._handle_workload_completion(workload)

                    # Remove from active workloads
                    async with self._workload_lock:
                        self._active_workloads.pop(pid, None)

                except Exception as e:
                    logger.error(f"Error monitoring workload PID {pid}: {e}", exc_info=True)

    def _is_process_alive(self, pid: int) -> bool:
        """Check if a process is still running."""
        try:
            process = psutil.Process(pid)
            return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    async def _handle_workload_completion(self, workload: ActiveWorkload) -> None:
        """Handle completion of a workload process.

        Collects output, determines exit code, and publishes results.
        """
        try:
            # Get process exit code
            try:
                process = psutil.Process(workload.pid)
                exit_code = process.wait(timeout=1)
            except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                # Process already gone, read from status if available
                exit_code = None

            # Read output files
            stdout_content = ""
            stderr_content = ""

            try:
                if workload.stdout_file.exists():
                    stdout_content = workload.stdout_file.read_text()
            except Exception as e:
                logger.error(f"Failed to read stdout file: {e}")

            try:
                if workload.stderr_file.exists():
                    stderr_content = workload.stderr_file.read_text()
            except Exception as e:
                logger.error(f"Failed to read stderr file: {e}")

            # Determine success
            success = exit_code == 0 if exit_code is not None else False

            # Calculate duration
            duration = time.time() - workload.started_at

            logger.info(
                f"Workload {workload.workload_type} (PID {workload.pid}) completed: "
                f"exit_code={exit_code}, duration={duration:.1f}s, success={success}"
            )
            # Publish results
            result_data = {
                "message": f"{workload.workload_type} completed",
                "pid": workload.pid,
                "status": "completed",
                "success": success,
                "exit_code": exit_code,
                "duration_seconds": duration,
            }

            if stdout_content:
                # Send stdout via log streaming
                await self._buffer_log(stdout_content, "stdout")

            if stderr_content:
                # Send stderr via log streaming
                await self._buffer_log(stderr_content, "stderr")

            if self.config.upload_results and workload.workload_uuid and success:
                await self._upload_result_files(workload)

            # Notify the backend that the workload has completed via MQTT
            if workload.workload_uuid and self._mqtt_client:
                try:
                    await self._mqtt_client.complete_workload(
                        workload_uuid=str(workload.workload_uuid),
                        timeout=30.0,
                    )
                    logger.info(f"Workload {workload.workload_uuid} completion notified via MQTT")
                except (MQTTError, asyncio.TimeoutError) as e:
                    logger.error(f"Failed to notify workload completion via MQTT: {e}")

            # Clean up workload files (log files and params file)
            workload.stdout_file.unlink(missing_ok=True)
            workload.stderr_file.unlink(missing_ok=True)

            # Clean up params file (has same base name as stdout file but with .params.json)
            params_file = (
                workload.stdout_file.parent
                / f"{workload.stdout_file.stem.replace('.stdout', '')}.params.json"
            )
            params_file.unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"Error handling workload completion: {e}", exc_info=True)

    async def _upload_result_files(self, workload: ActiveWorkload) -> None:
        """Upload result files from the results folder to the backend.

        Recursively scans the results folder and uploads all files to signed URLs.

        Args:
            workload: The completed workload with workload_uuid
        """
        if not workload.workload_uuid:
            logger.warning("Cannot upload results: workload_uuid not available")
            return

        # Resolve results_folder relative to working_dir (handles both relative and absolute paths)
        results_path = (self.working_dir / self.config.results_folder).resolve()

        if not results_path.is_dir():
            logger.debug(f"Results folder not found or not a directory: {results_path}")
            return

        # Recursively find all files (checkpoints are in subdirectories like ./runs/{run_id}/)
        try:
            result_files = [f for f in results_path.rglob("*") if f.is_file()]
        except Exception as e:
            logger.error(f"Error scanning results folder {results_path}: {e}")
            return

        if not result_files:
            logger.debug(f"No result files found in {results_path}")
            return

        logger.info(f"Found {len(result_files)} file(s) to upload from {results_path}")

        uploaded_count = 0
        failed_count = 0

        # Notify the backend of finalizing the results via MQTT
        if workload.workload_uuid and self._mqtt_client:
            try:
                await self._mqtt_client.update_workload_status(
                    workload_uuid=str(workload.workload_uuid),
                    status="finalizing",
                    additional_data={
                        "message": "Starting result file upload",
                    },
                )
                logger.info(
                    f"Notified backend of finalizing status for workload {workload.workload_uuid} via MQTT"
                )
            except (MQTTError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"Failed to notify backend of finalizing status via MQTT: {e}. "
                    "Continuing with file uploads."
                )
        for file_path in result_files:
            # Preserve directory structure by using relative path as filename
            filename = str(file_path.relative_to(results_path)).replace("\\", "/")

            try:
                # Get signed URL from backend
                signed_url_response = self.client.get_signed_url_for_attachment(
                    workload_uuid=workload.workload_uuid,
                    filename=filename,
                )
                signed_url = signed_url_response.get("signed_url")

                if not signed_url:
                    logger.error(f"No signed URL returned for {filename}")
                    failed_count += 1
                    continue

                # Upload file
                async with httpx.AsyncClient(timeout=300.0) as client:
                    with open(file_path, "rb") as f:
                        response = await client.put(
                            signed_url,
                            content=f.read(),
                            headers={"Content-Type": "application/octet-stream"},
                        )

                if response.status_code in (200, 204):
                    logger.info(f"Uploaded: {filename}")
                    uploaded_count += 1
                else:
                    logger.error(f"Upload failed ({response.status_code}): {filename}")
                    failed_count += 1

            except CloudNodeClientError as e:
                logger.error(f"Failed to get signed URL for {filename}: {e}")
                failed_count += 1
            except Exception as e:
                logger.error(f"Error uploading {filename}: {e}", exc_info=True)
                failed_count += 1

        logger.info(f"Upload complete: {uploaded_count} succeeded, {failed_count} failed")

    async def _register_via_mqtt(self) -> None:
        """Register this node with the Cyberwave backend via MQTT.

        Uses MQTT v5 request/response pattern with a conditional two-step flow:
        1. Request instance creation (only if instance_uuid not set from environment)
        2. Register the instance with profile (handles restart scenarios)

        If CYBERWAVE_CLOUD_NODE_INSTANCE_UUID is set in environment (e.g., by Terraform),
        skips step 1 and goes directly to registration.

        Retries registration every 10 seconds if it fails.

        The backend owns the UUID and slug - after successful registration,
        this method updates self.instance_uuid and self.slug with the values
        returned by the backend.
        """
        if self.slug:
            logger.info(f"Registering Cloud Node via MQTT with slug hint '{self.slug}'")
        else:
            logger.info("Registering Cloud Node via MQTT (backend will assign slug)")

        if not self._mqtt_client:
            raise CloudNodeError("MQTT client not connected")

        while self._running:
            try:
                instance_uuid = self.instance_uuid
                instance_slug = self.slug

                # Step 1: Request instance creation (only if UUID not already set)
                if not instance_uuid:
                    logger.info("Step 1: Requesting instance creation (no UUID in environment)...")
                    create_response = await self._mqtt_client.request_instance(
                        profile_slug=self.config.profile_slug,
                        slug=self.slug or None,  # Optional hint, backend assigns actual slug
                        provider="self-hosted",
                        visibility="private",
                        timeout=30.0,
                    )

                    # Extract instance details from response
                    instance_uuid = create_response.payload.get("uuid", "")
                    instance_slug = create_response.payload.get("slug", "")

                    if not instance_uuid or not instance_slug:
                        raise CloudNodeError(
                            f"Invalid instance creation response: uuid={instance_uuid}, slug={instance_slug}"
                        )

                    logger.info(
                        f"Instance created: UUID={instance_uuid}, Slug={instance_slug}, "
                        f"Status={create_response.payload.get('status')}"
                    )
                else:
                    logger.info(
                        f"Step 1: Skipping instance creation (UUID already set from environment: {instance_uuid})"
                    )

                # Step 2: Register the instance (updates status to READY, handles restart scenarios)
                logger.info(f"Step 2: Registering instance {instance_uuid}...")
                register_response = await self._mqtt_client.register_instance(
                    instance_uuid=instance_uuid,
                    profile_slug=self.config.profile_slug,
                    timeout=30.0,
                )

                # Update instance details
                self.instance_uuid = register_response.payload.get("uuid", instance_uuid)
                self.slug = register_response.payload.get("slug", instance_slug) or ""

                if not self.instance_uuid:
                    raise CloudNodeError(f"Invalid registration response: missing uuid")

                # If slug is empty, generate a fallback slug from uuid (shouldn't happen with backend fix, but safety measure)
                if not self.slug:
                    logger.warning(
                        f"Registration response missing slug, using uuid-based fallback slug"
                    )
                    self.slug = f"cn-{self.instance_uuid[:8]}"

                logger.info("Registration successful via MQTT")
                logger.info(f"Instance UUID: {self.instance_uuid}, Slug: {self.slug}")
                return

            except (MQTTError, asyncio.TimeoutError) as e:
                logger.warning(f"Registration failed: {e}. Retrying in 10 seconds...")
                await asyncio.sleep(10)

        # If we exit the loop because _running became False, raise an error
        raise CloudNodeError("Registration aborted due to shutdown")

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to the backend via MQTT."""
        logger.info(f"Starting heartbeat loop (interval: {self.config.heartbeat_interval}s)")

        while self._running:
            try:
                if self._mqtt_client:
                    response = await self._mqtt_client.send_heartbeat(
                        instance_uuid=self.instance_uuid,
                        timeout=10.0,
                    )
                    logger.debug(f"Heartbeat sent via MQTT: {response.payload.get('message')}")
                else:
                    logger.warning("MQTT client not available for heartbeat")

            except (MQTTError, asyncio.TimeoutError) as e:
                logger.warning(f"Heartbeat failed: {e}")
                # Continue trying - the backend might be temporarily unavailable

            await asyncio.sleep(self.config.heartbeat_interval)

    async def _log_flush_loop(self) -> None:
        """Periodically flush buffered logs to the backend."""
        logger.info(f"Starting log flush loop (interval: {self._log_flush_interval}s)")

        while self._running:
            await asyncio.sleep(self._log_flush_interval)
            await self._flush_logs()

    async def _mqtt_reconnect_loop(self) -> None:
        """Monitor MQTT connection and reconnect if needed."""
        logger.info("Starting MQTT reconnection monitoring loop")
        check_interval = 30  # Check every 30 seconds

        while self._running:
            await asyncio.sleep(check_interval)

            try:
                # Check if MQTT is connected
                if self._mqtt_client and not self._mqtt_client.connected:
                    logger.warning("MQTT connection lost, attempting to reconnect...")
                    try:
                        await self._mqtt_client.connect()
                        logger.info("MQTT reconnected successfully")
                        # Re-subscribe to command topics after reconnection
                        if self.instance_uuid:
                            await self._subscribe_to_commands()
                    except MQTTError as reconnect_error:
                        logger.error(
                            f"Failed to reconnect to MQTT: {reconnect_error}", exc_info=True
                        )
                        # Continue monitoring - will retry on next check
            except Exception as e:
                logger.error(f"Error checking MQTT connection status: {e}", exc_info=True)
                # Don't crash - continue monitoring

    async def _flush_logs(self) -> None:
        """Flush buffered logs to the backend via MQTT."""
        async with self._log_buffer_lock:
            # Flush stdout buffer
            if self._stdout_buffer:
                stdout_content = "\n".join(self._stdout_buffer)
                self._stdout_buffer.clear()
                try:
                    if self._mqtt_client:
                        await self._mqtt_client.send_log(
                            instance_uuid=self.instance_uuid,
                            log_content=stdout_content,
                            log_type="stdout",
                            timeout=10.0,
                        )
                        logger.debug(f"Flushed {len(stdout_content)} bytes of stdout logs via MQTT")
                    else:
                        logger.warning("MQTT client not available for log flush")
                        # Re-add to buffer
                        self._stdout_buffer.insert(0, stdout_content)
                        self._truncate_buffer(self._stdout_buffer)
                except (MQTTError, asyncio.TimeoutError) as e:
                    logger.warning(f"Failed to flush stdout logs: {e}")
                    # Re-add to buffer for next attempt (but limit size)
                    self._stdout_buffer.insert(0, stdout_content)
                    self._truncate_buffer(self._stdout_buffer)

            # Flush stderr buffer
            if self._stderr_buffer:
                stderr_content = "\n".join(self._stderr_buffer)
                self._stderr_buffer.clear()
                try:
                    if self._mqtt_client:
                        await self._mqtt_client.send_log(
                            instance_uuid=self.instance_uuid,
                            log_content=stderr_content,
                            log_type="stderr",
                            timeout=10.0,
                        )
                        logger.debug(f"Flushed {len(stderr_content)} bytes of stderr logs via MQTT")
                    else:
                        logger.warning("MQTT client not available for log flush")
                        # Re-add to buffer
                        self._stderr_buffer.insert(0, stderr_content)
                        self._truncate_buffer(self._stderr_buffer)
                except (MQTTError, asyncio.TimeoutError) as e:
                    logger.warning(f"Failed to flush stderr logs: {e}")
                    # Re-add to buffer for next attempt (but limit size)
                    self._stderr_buffer.insert(0, stderr_content)
                    self._truncate_buffer(self._stderr_buffer)

    def _truncate_buffer(self, buffer: list[str], max_lines: int = 10000) -> None:
        """Truncate buffer to prevent unbounded growth."""
        if len(buffer) > max_lines:
            # Keep the most recent lines
            del buffer[:-max_lines]

    async def _buffer_log(self, content: str, log_type: str = "stdout") -> None:
        """Add log content to the appropriate buffer."""
        if not content:
            return

        async with self._log_buffer_lock:
            lines = content.splitlines()
            if log_type == "stderr":
                self._stderr_buffer.extend(lines)
                self._truncate_buffer(self._stderr_buffer)
            else:
                self._stdout_buffer.extend(lines)
                self._truncate_buffer(self._stdout_buffer)

    async def _run_command(
        self,
        command: str,
        workload_type: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> WorkloadResult:
        """Run a shell command directly and capture output in real-time.

        Args:
            command: Shell command to execute
            workload_type: Optional workload type (inference/training) for logging
            request_id: Optional request ID for logging

        Returns:
            WorkloadResult with output and status
        """
        logger.info(
            f"Starting command execution (workload_type: {workload_type}, request_id: {request_id})"
        )
        logger.info(f"Command: {command}")

        try:
            # Create subprocess with separate stdout/stderr pipes
            # Use bash to ensure proper shell expansion and environment variable handling
            shell_command = f"bash -c {shlex.quote(command)}"
            process = await asyncio.create_subprocess_shell(
                shell_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.working_dir,
            )

            # Collect stdout and stderr in real-time
            stdout_chunks: list[str] = []
            stderr_chunks: list[str] = []

            async def read_stdout() -> None:
                """Read stdout and buffer logs in real-time."""
                if process.stdout:
                    try:
                        # Read data in chunks to handle both line-buffered and unbuffered output
                        while True:
                            # Read up to 8KB at a time
                            chunk = await process.stdout.read(8192)
                            if not chunk:
                                break
                            chunk_str = chunk.decode("utf-8", errors="replace")
                            stdout_chunks.append(chunk_str)
                            # Buffer logs in real-time for backend visualization
                            await self._buffer_log(chunk_str, "stdout")
                    except Exception as e:
                        logger.error(f"Error reading stdout: {e}", exc_info=True)

            async def read_stderr() -> None:
                """Read stderr and buffer logs in real-time."""
                if process.stderr:
                    try:
                        # Read data in chunks to handle both line-buffered and unbuffered output
                        while True:
                            # Read up to 8KB at a time
                            chunk = await process.stderr.read(8192)
                            if not chunk:
                                break
                            chunk_str = chunk.decode("utf-8", errors="replace")
                            stderr_chunks.append(chunk_str)
                            # Buffer logs in real-time for backend visualization
                            await self._buffer_log(chunk_str, "stderr")
                    except Exception as e:
                        logger.error(f"Error reading stderr: {e}", exc_info=True)

            # Read stdout and stderr concurrently while process runs
            # This ensures logs are captured in real-time
            _, _, return_code = await asyncio.gather(
                read_stdout(),
                read_stderr(),
                process.wait(),
            )

            # Combine all chunks
            stdout_str = "".join(stdout_chunks)
            stderr_str = "".join(stderr_chunks)

            if return_code == 0:
                logger.info(f"Command completed successfully (return_code: {return_code})")
                return WorkloadResult(
                    success=True,
                    output=stdout_str,
                    return_code=return_code,
                )
            else:
                logger.warning(f"Command failed with code {return_code}")
                if stderr_str:
                    logger.error(f"Command stderr: {stderr_str}")
                if stdout_str:
                    logger.info(f"Command stdout: {stdout_str}")
                return WorkloadResult(
                    success=False,
                    output=stdout_str,
                    error=stderr_str or f"Command failed with code {return_code}",
                    return_code=return_code,
                )

        except Exception as e:
            logger.error(f"Error running command: {e}", exc_info=True)
            # Return error result instead of raising
            return WorkloadResult(
                success=False,
                error=f"Command execution failed: {str(e)}",
            )

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self._running = False
            # Set the event in a thread-safe way
            # Use the stored event loop if available, otherwise get the current one
            loop = self._event_loop if self._event_loop else asyncio.get_event_loop()
            if loop and not loop.is_closed():
                loop.call_soon_threadsafe(self._shutdown_event.set)
            else:
                # Fallback: set the event directly if loop is not available
                self._shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def _notify_failed(self, error: str) -> None:
        """Notify the backend that this node has failed via MQTT."""
        try:
            if self._mqtt_client:
                await self._mqtt_client.notify_failed(
                    instance_uuid=self.instance_uuid,
                    error=error,
                    timeout=10.0,
                )
                logger.info("Backend notified of failure via MQTT")
            else:
                logger.error("MQTT client not available to notify backend of failure")
        except (MQTTError, asyncio.TimeoutError) as e:
            logger.error(f"Failed to notify backend of failure via MQTT: {e}")

    async def _shutdown(self) -> None:
        """Perform graceful shutdown.

        Workloads run as independent OS processes that survive Cloud Node shutdown.
        This method:
        1. Stops accepting new commands
        2. Cancels background monitoring tasks
        3. Logs any running workloads (they continue independently)
        4. Flushes logs and notifies backend
        """
        logger.info("Shutting down Cloud Node...")
        self._running = False

        # Step 1: Cancel background tasks (heartbeat, log flush, mqtt monitor, workload monitor)
        background_tasks = [
            ("heartbeat", self._heartbeat_task),
            ("log_flush", self._log_flush_task),
            ("mqtt_reconnect", self._mqtt_reconnect_task),
            ("workload_monitor", self._workload_monitor_task),
        ]

        for task_name, task in background_tasks:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.debug(f"{task_name} task cancelled successfully")

        # Step 2: Check for running workloads (they will continue in background)
        async with self._workload_lock:
            active_workloads = list(self._active_workloads.values())

        if active_workloads:
            logger.info(
                f"{len(active_workloads)} workload(s) still running in background processes:"
            )
            for workload in active_workloads:
                logger.info(
                    f"  - PID {workload.pid}: {workload.workload_type} "
                    f"(running for {int(time.time() - workload.started_at)}s)"
                )
            logger.info(
                "These workloads will continue running independently. "
                "Output will be available in log files:"
            )
            for workload in active_workloads:
                logger.info(f"  - {workload.stdout_file}")

        # Step 3: Flush any remaining logs before shutdown
        try:
            await self._flush_logs()
            logger.info("Final log flush completed")
        except Exception as e:
            logger.error(f"Failed to flush logs during shutdown: {e}")

        # Step 4: Notify backend of termination via MQTT
        try:
            if self._mqtt_client and self.instance_uuid:
                await self._mqtt_client.notify_terminated(
                    instance_uuid=self.instance_uuid,
                    timeout=10.0,
                )
                logger.info("Backend notified of termination via MQTT")
            else:
                logger.error("MQTT client not available to notify backend of termination")
        except (MQTTError, asyncio.TimeoutError) as e:
            logger.error(f"Failed to notify backend of termination via MQTT: {e}")

        # Step 5: Disconnect MQTT
        if self._mqtt_client:
            try:
                await self._mqtt_client.disconnect()
                logger.info("MQTT disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting from MQTT: {e}")

        # Step 6: Close REST client
        self.client.close()

        logger.info("Cloud Node shutdown complete")
