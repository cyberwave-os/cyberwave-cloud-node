"""Main Cloud Node service implementation.

This module provides the CloudNode class that orchestrates:
- Running the install script on startup
- Registering with the Cyberwave backend
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
import socket
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

from .client import CloudNodeClient, CloudNodeClientError
from .config import CloudNodeConfig

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
    3. Starts HTTP server to receive workload requests
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
            client: Optional pre-configured CloudNodeClient
            working_dir: Working directory for running commands. Defaults to current directory.
        """
        self.slug = slug
        self.config = config
        self.client = client or CloudNodeClient()
        self.working_dir = working_dir or Path.cwd()

        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._server_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

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
        3. Start the HTTP server and heartbeat loop
        4. Block until shutdown signal is received
        """
        asyncio.run(self.run_async())

    async def run_async(self) -> None:
        """Run the Cloud Node service asynchronously."""
        self._running = True
        self._setup_signal_handlers()

        try:
            # Step 1: Run install script
            if self.config.install_script:
                await self._run_install_script()

            # Step 2: Get our endpoint URL
            endpoint = self._get_endpoint_url()
            logger.info(f"Cloud Node endpoint: {endpoint}")

            # Step 3: Register with backend
            await self._register(endpoint)

            # Step 4: Start heartbeat loop and HTTP server
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._server_task = asyncio.create_task(self._run_server())

            # Wait for shutdown signal
            await self._shutdown_event.wait()

        except Exception as e:
            logger.error(f"Cloud Node error: {e}")
            await self._notify_failed(str(e))
            raise

        finally:
            await self._shutdown()

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

    async def _register(self, endpoint: str) -> None:
        """Register this node with the Cyberwave backend.

        Retries registration every 10 seconds if it fails (e.g., if the instance
        is in a transient state like 'terminating').
        """
        logger.info(f"Registering Cloud Node '{self.slug}' at {endpoint}")

        while self._running:
            try:
                response = self.client.register(
                    slug=self.slug,
                    endpoint=endpoint,
                    profile_slug=self.config.profile_slug,
                    provider=self.config.provider,
                )
                logger.info(f"Registration successful: {response.message}")
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

    async def _run_server(self) -> None:
        """Run the HTTP server to receive workload requests."""

        class WorkloadRequest(BaseModel):
            """Request body for workload execution."""

            params: dict = {}

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            logger.info(f"Cloud Node server starting on port {self.config.server_port}")
            yield
            logger.info("Cloud Node server shutting down")

        app = FastAPI(
            title=f"Cyberwave Cloud Node - {self.slug}",
            lifespan=lifespan,
        )

        @app.get("/health")
        async def health():
            """Health check endpoint."""
            return {"status": "healthy", "slug": self.slug}

        @app.post("/inference")
        async def run_inference(request: WorkloadRequest):
            """Execute an inference workload."""
            if not self.config.inference:
                raise HTTPException(
                    status_code=400,
                    detail="Inference command not configured in cyberwave.yml",
                )

            result = await self.execute_workload("inference", request.params)
            if not result.success:
                raise HTTPException(status_code=500, detail=result.error)

            return JSONResponse(
                content={
                    "success": True,
                    "output": result.output,
                }
            )

        @app.post("/training")
        async def run_training(request: WorkloadRequest):
            """Execute a training workload."""
            if not self.config.training:
                raise HTTPException(
                    status_code=400,
                    detail="Training command not configured in cyberwave.yml",
                )

            result = await self.execute_workload("training", request.params)
            if not result.success:
                raise HTTPException(status_code=500, detail=result.error)

            return JSONResponse(
                content={
                    "success": True,
                    "output": result.output,
                }
            )

        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=self.config.server_port,
            log_level="info",
        )
        server = uvicorn.Server(config)
        await server.serve()

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

    def _get_endpoint_url(self) -> str:
        """Get the endpoint URL for this node.

        Tries to determine the public IP/hostname of this machine.
        Can be overridden via CYBERWAVE_ENDPOINT env var.
        """
        # Allow explicit override
        endpoint_override = os.getenv("CYBERWAVE_ENDPOINT")
        if endpoint_override:
            return endpoint_override

        # Try to get a routable IP
        try:
            # Connect to a public DNS to get our routable IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
        except Exception:
            ip = "127.0.0.1"

        return f"http://{ip}:{self.config.server_port}"

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

        # Notify backend of termination
        try:
            self.client.terminated(slug=self.slug)
            logger.info("Backend notified of termination")
        except CloudNodeClientError as e:
            logger.error(f"Failed to notify backend of termination: {e}")

        # Close client
        self.client.close()

        logger.info("Cloud Node shutdown complete")
