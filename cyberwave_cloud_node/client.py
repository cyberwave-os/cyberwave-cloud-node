"""HTTP client for Cyberwave Cloud Node API calls.

This module handles communication with the Cyberwave backend API for:
- Registering the cloud node instance
- Sending heartbeats
- Reporting termination or failure
"""

import logging
import uuid as uuid_lib
from dataclasses import dataclass
from typing import Any, Optional

import httpx

from .config import (
    CLOUD_NODE_CREATE_INSTANCE_ENDPOINT,
    CLOUD_NODE_FAILED_ENDPOINT,
    CLOUD_NODE_HEARTBEAT_ENDPOINT,
    CLOUD_NODE_LOG_ENDPOINT,
    CLOUD_NODE_REGISTER_ENDPOINT,
    CLOUD_NODE_TERMINATED_ENDPOINT,
    CLOUD_NODE_WORKLOAD_COMPLETE_ENDPOINT,
    CLOUD_NODE_WORKLOAD_FAIL_ENDPOINT,
    CLOUD_NODE_WORKLOAD_UPDATE_ENDPOINT,
    get_api_token,
    get_api_url,
    get_instance_uuid,
    get_workspace_slug,
)
from .credentials import (
    InstanceIdentity,
    clear_instance_identity,
    load_instance_identity,
    save_instance_identity,
)

logger = logging.getLogger(__name__)


class CloudNodeClientError(Exception):
    """Raised when a Cloud Node API call fails."""

    def __init__(
        self, message: str, status_code: Optional[int] = None, details: Optional[dict] = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.details = details


@dataclass
class RegisterResponse:
    """Response from the register endpoint."""

    success: bool
    message: str
    uuid: str
    slug: str

    @classmethod
    def from_dict(cls, data: dict) -> "RegisterResponse":
        return cls(
            success=data.get("success", False),
            message=data.get("message", ""),
            uuid=data.get("uuid", ""),
            slug=data.get("slug", ""),
        )


@dataclass
class HeartbeatResponse:
    """Response from the heartbeat endpoint."""

    success: bool
    message: str

    @classmethod
    def from_dict(cls, data: dict) -> "HeartbeatResponse":
        return cls(
            success=data.get("success", False),
            message=data.get("message", ""),
        )


@dataclass
class TerminatedResponse:
    """Response from the terminated endpoint."""

    success: bool
    message: str

    @classmethod
    def from_dict(cls, data: dict) -> "TerminatedResponse":
        return cls(
            success=data.get("success", False),
            message=data.get("message", ""),
        )


@dataclass
class FailedResponse:
    """Response from the failed endpoint."""

    success: bool
    message: str

    @classmethod
    def from_dict(cls, data: dict) -> "FailedResponse":
        return cls(
            success=data.get("success", False),
            message=data.get("message", ""),
        )


@dataclass
class LogResponse:
    """Response from the log endpoint."""

    success: bool
    message: str

    @classmethod
    def from_dict(cls, data: dict) -> "LogResponse":
        return cls(
            success=data.get("success", False),
            message=data.get("message", ""),
        )


class CloudNodeClient:
    """Client for Cyberwave Cloud Node API calls.

    Handles authentication and communication with the backend API.
    Similar to AuthClient in cyberwave-cli but focused on cloud node operations.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        workspace_slug: Optional[str] = None,
    ):
        """Initialize the Cloud Node client.

        Args:
            base_url: API base URL. Defaults to CYBERWAVE_API_URL env var or https://api.cyberwave.com
            token: API token. Defaults to CYBERWAVE_API_TOKEN env var
            workspace_slug: Workspace slug. Defaults to CYBERWAVE_WORKSPACE_SLUG env var
        """
        self.base_url = base_url or get_api_url()
        self.token = token or get_api_token()
        self.workspace_slug = workspace_slug or get_workspace_slug()

        if not self.token:
            raise CloudNodeClientError(
                "API token is required. Set CYBERWAVE_API_TOKEN environment variable "
                "or pass token to CloudNodeClient."
            )

        self._client = httpx.Client(
            base_url=self.base_url,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Token {self.token}",
            },
            timeout=30.0,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the HTTP client."""
        self._client.close()

    def create_instance(
        self,
        profile_slug: str,
        slug: Optional[str] = None,
        workspace_slug: Optional[str] = None,
    ) -> RegisterResponse:
        """Create a new Cloud Node instance with the backend.

        This creates the instance record in the backend but does not register it.
        After creation, call register() with the returned UUID to complete registration.

        Note: The backend create_instance endpoint requires workspace_uuid, but we only
        have workspace_slug. The workspace will be resolved by the backend from the
        authenticated user's context or query parameters.

        Args:
            profile_slug: The node profile slug (defines capabilities)
            slug: Optional slug for this instance (backend generates if not provided)
            workspace_slug: Optional workspace slug override (passed as query param)

        Returns:
            RegisterResponse with success status, message, uuid and slug

        Raises:
            CloudNodeClientError: If creation fails
        """
        payload = {
            "profile_slug": profile_slug,
        }
        if slug:
            payload["slug"] = slug

        # Note: create_instance requires workspace_uuid in payload, but we have workspace_slug
        # The backend's require_workspace decorator can resolve workspace from query params
        # However, the schema requires workspace_uuid. We'll pass workspace_slug as query param
        # and let the backend handle it. If this doesn't work, we may need backend changes.
        params = {}
        ws_slug = workspace_slug or self.workspace_slug
        if ws_slug:
            # Pass as query param - backend's require_workspace might not support slug,
            # but we'll try. If it fails, we'll need backend changes.
            params["workspace_slug"] = ws_slug

        try:
            response = self._client.post(
                CLOUD_NODE_CREATE_INSTANCE_ENDPOINT, json=payload, params=params
            )

            if response.status_code == 200:
                # The response is a CloudNodeInstanceSchema, not RegisterResponse
                # We need to convert it
                data = response.json()
                result = RegisterResponse(
                    success=True,
                    message=f"Instance {data.get('uuid')} created",
                    uuid=data.get("uuid", ""),
                    slug=data.get("slug", ""),
                )
                logger.info(f"Created Cloud Node instance: uuid={result.uuid}, slug={result.slug}")
                return result

            self._handle_error_response(response, "create_instance")

        except httpx.RequestError as e:
            raise CloudNodeClientError(f"Connection error during instance creation: {e}") from e

        raise CloudNodeClientError("Instance creation failed")

    def register(
        self,
        profile_slug: str,
        slug: Optional[str] = None,
        workspace_slug: Optional[str] = None,
        save_identity: bool = True,
    ) -> RegisterResponse:
        """Register this Cloud Node instance with the backend.

        Called after the node has booted and is ready to serve workloads.

        Registration flow:
        1. If instance UUID is available (from env or stored identity):
           - Try to register directly using the UUID endpoint
           - If 404 (instance doesn't exist), create it first, then register
        2. If instance UUID is NOT available:
           - First create the instance via create_instance()
           - Then register it using the returned UUID

        Args:
            profile_slug: The node profile slug (defines capabilities)
            slug: Optional slug for this instance (backend generates if not provided)
            workspace_slug: Optional workspace slug override
            save_identity: Whether to save the returned UUID/slug locally (default: True)

        Returns:
            RegisterResponse with success status, message, uuid and slug

        Raises:
            CloudNodeClientError: If registration fails
        """
        # Try to load instance UUID from environment or stored identity
        instance_uuid = None
        # First check environment variable for instance_uuid
        instance_uuid = get_instance_uuid()
        if instance_uuid:
            logger.info(f"Using instance UUID from environment: {instance_uuid}")

        # Load stored identity if we still need slug or instance_uuid
        if not slug or not instance_uuid:
            stored_identity = load_instance_identity()
            if stored_identity:
                if not instance_uuid:
                    instance_uuid = stored_identity.uuid
                if not slug:
                    slug = stored_identity.slug
                logger.info(f"Using stored identity: uuid={instance_uuid}, slug={slug}")

        ws_slug = workspace_slug or self.workspace_slug

        # If UUID exists, try to register directly first
        if instance_uuid:
            logger.info(f"Attempting to register existing instance with UUID: {instance_uuid}")
            payload = {
                "profile_slug": profile_slug,
            }

            try:
                # Use UUID-based endpoint: /api/v1/cloud-node/{uuid}/register
                endpoint = CLOUD_NODE_REGISTER_ENDPOINT.format(uuid=instance_uuid)
                response = self._client.post(endpoint, json=payload)

                if response.status_code < 400:
                    result = RegisterResponse.from_dict(response.json())

                    # Save the identity locally for future use
                    if save_identity and result.success:
                        identity = InstanceIdentity(
                            uuid=result.uuid,
                            slug=result.slug,
                            workspace_slug=ws_slug,
                        )
                        clear_instance_identity()
                        save_instance_identity(identity)
                        logger.info(
                            f"Saved instance identity: uuid={result.uuid}, slug={result.slug}"
                        )

                    return result

                # If 404, instance doesn't exist - need to create it first
                if response.status_code == 404:
                    logger.warning(
                        f"Instance {instance_uuid} not found. Creating new instance first."
                    )
                    # Fall through to create + register flow below
                else:
                    self._handle_error_response(response, "register")

            except httpx.RequestError as e:
                raise CloudNodeClientError(f"Connection error during registration: {e}") from e

        # Create instance first (if UUID doesn't exist or registration returned 404)
        logger.info("Creating new instance before registration")
        try:
            # Step 1: Create the instance using the create endpoint
            create_result = self.create_instance(
                profile_slug=profile_slug,
                slug=slug,
                workspace_slug=ws_slug,
            )
            instance_uuid = create_result.uuid
            logger.info(f"Created instance with UUID: {instance_uuid}")

            # Step 2: Register the instance using the UUID
            payload = {
                "profile_slug": profile_slug,
            }

            endpoint = CLOUD_NODE_REGISTER_ENDPOINT.format(uuid=instance_uuid)
            response = self._client.post(endpoint, json=payload)

            if response.status_code == 200:
                result = RegisterResponse.from_dict(response.json())

                # Save the identity locally for future use
                if save_identity and result.success:
                    identity = InstanceIdentity(
                        uuid=result.uuid,
                        slug=result.slug,
                        workspace_slug=ws_slug,
                    )
                    clear_instance_identity()
                    save_instance_identity(identity)
                    logger.info(f"Saved instance identity: uuid={result.uuid}, slug={result.slug}")

                return result

            self._handle_error_response(response, "register")

        except httpx.RequestError as e:
            raise CloudNodeClientError(f"Connection error during registration: {e}") from e

        raise CloudNodeClientError("Registration failed")

    def heartbeat(
        self,
        slug: Optional[str] = None,
        instance_uuid: Optional[str] = None,
        workspace_slug: Optional[str] = None,
    ) -> HeartbeatResponse:
        """Send a heartbeat to signal the node is alive.

        Called periodically to prevent the backend from marking this instance as stale.
        If no slug or instance_uuid is provided, uses the stored identity from registration.

        Args:
            slug: Instance slug (uses stored identity if not provided)
            instance_uuid: Instance UUID (uses stored identity if not provided)
            workspace_slug: Optional workspace slug override

        Returns:
            HeartbeatResponse with success status

        Raises:
            CloudNodeClientError: If heartbeat fails or no identity is available
        """
        # Auto-load instance UUID/slug from environment or stored identity if not provided
        # First check environment variable for instance_uuid
        if not instance_uuid:
            instance_uuid = get_instance_uuid()

        # Load stored identity if we still need either value
        if not instance_uuid or not slug:
            stored_identity = load_instance_identity()
            if stored_identity:
                if not instance_uuid:
                    instance_uuid = stored_identity.uuid
                if not slug:
                    slug = stored_identity.slug

        # instance_uuid is REQUIRED for heartbeat (backend requires UUID in path)
        if not instance_uuid:
            raise CloudNodeClientError(
                "instance_uuid is required for heartbeat. "
                "Call register() first or provide instance_uuid."
            )

        # Use UUID-based endpoint: /api/v1/cloud-node/{uuid}/heartbeat
        # No payload needed - UUID is in the path
        try:
            endpoint = CLOUD_NODE_HEARTBEAT_ENDPOINT.format(uuid=instance_uuid)
            response = self._client.post(endpoint, json={})

            if response.status_code == 200:
                return HeartbeatResponse.from_dict(response.json())

            self._handle_error_response(response, "heartbeat")

        except httpx.RequestError as e:
            raise CloudNodeClientError(f"Connection error during heartbeat: {e}") from e

        raise CloudNodeClientError("Heartbeat failed")

    def terminated(
        self,
        slug: Optional[str] = None,
        instance_uuid: Optional[str] = None,
        workspace_slug: Optional[str] = None,
    ) -> TerminatedResponse:
        """Notify the backend that this instance is terminating.

        Called during graceful shutdown.
        If no slug or instance_uuid is provided, uses the stored identity from registration.

        Args:
            slug: Instance slug (uses stored identity if not provided)
            instance_uuid: Instance UUID (uses stored identity if not provided)
            workspace_slug: Optional workspace slug override

        Returns:
            TerminatedResponse with success status

        Raises:
            CloudNodeClientError: If notification fails or no identity is available
        """
        # Auto-load instance UUID/slug from environment or stored identity if not provided
        # First check environment variable for instance_uuid
        if not instance_uuid:
            instance_uuid = get_instance_uuid()

        # Load stored identity if we still need either value
        if not instance_uuid or not slug:
            stored_identity = load_instance_identity()
            if stored_identity:
                if not instance_uuid:
                    instance_uuid = stored_identity.uuid
                if not slug:
                    slug = stored_identity.slug

        if not instance_uuid and not slug:
            raise CloudNodeClientError(
                "No instance identity found. Call register() first or provide slug/instance_uuid."
            )

        payload = {}
        if slug:
            payload["slug"] = slug
        if instance_uuid:
            payload["instance_uuid"] = instance_uuid

        ws_slug = workspace_slug or self.workspace_slug
        if ws_slug:
            payload["workspace_slug"] = ws_slug

        try:
            response = self._client.post(CLOUD_NODE_TERMINATED_ENDPOINT, json=payload)

            if response.status_code == 200:
                return TerminatedResponse.from_dict(response.json())

            self._handle_error_response(response, "terminated")

        except httpx.RequestError as e:
            raise CloudNodeClientError(
                f"Connection error during termination notification: {e}"
            ) from e

        raise CloudNodeClientError("Termination notification failed")

    def failed(
        self,
        error: str,
        slug: Optional[str] = None,
        instance_uuid: Optional[str] = None,
        workspace_slug: Optional[str] = None,
    ) -> FailedResponse:
        """Notify the backend that this instance has failed.

        Called when a critical error occurs that prevents the node from functioning.
        If no slug or instance_uuid is provided, uses the stored identity from registration.

        Args:
            error: Description of the failure
            slug: Instance slug (uses stored identity if not provided)
            instance_uuid: Instance UUID (uses stored identity if not provided)
            workspace_slug: Optional workspace slug override

        Returns:
            FailedResponse with success status

        Raises:
            CloudNodeClientError: If notification fails or no identity is available
        """
        # Auto-load instance UUID/slug from environment or stored identity if not provided
        # First check environment variable for instance_uuid
        if not instance_uuid:
            instance_uuid = get_instance_uuid()

        # Load stored identity if we still need either value
        if not instance_uuid or not slug:
            stored_identity = load_instance_identity()
            if stored_identity:
                if not instance_uuid:
                    instance_uuid = stored_identity.uuid
                if not slug:
                    slug = stored_identity.slug

        if not instance_uuid and not slug:
            raise CloudNodeClientError(
                "No instance identity found. Call register() first or provide slug/instance_uuid."
            )

        payload = {"error": error}
        if slug:
            payload["slug"] = slug
        if instance_uuid:
            payload["instance_uuid"] = instance_uuid

        ws_slug = workspace_slug or self.workspace_slug
        if ws_slug:
            payload["workspace_slug"] = ws_slug

        try:
            response = self._client.post(CLOUD_NODE_FAILED_ENDPOINT, json=payload)

            if response.status_code == 200:
                return FailedResponse.from_dict(response.json())

            self._handle_error_response(response, "failed")

        except httpx.RequestError as e:
            raise CloudNodeClientError(f"Connection error during failure notification: {e}") from e

        raise CloudNodeClientError("Failure notification failed")

    def send_log(
        self,
        log_content: str,
        log_type: str = "stdout",
        slug: Optional[str] = None,
        instance_uuid: Optional[str] = None,
        workspace_slug: Optional[str] = None,
    ) -> LogResponse:
        """Send log content to the backend for storage.

        Logs are stored asynchronously in GCS by the backend.
        If no slug or instance_uuid is provided, uses the stored identity from registration.

        Args:
            log_content: The log content to send
            log_type: Type of log ("stdout", "stderr", "system", "app")
            slug: Instance slug (uses stored identity if not provided)
            instance_uuid: Instance UUID (uses stored identity if not provided)
            workspace_slug: Optional workspace slug override

        Returns:
            LogResponse with success status

        Raises:
            CloudNodeClientError: If sending fails or no identity is available
        """
        if not log_content:
            return LogResponse(success=True, message="No log content to send")

        # Auto-load instance UUID/slug from environment or stored identity if not provided
        # First check environment variable for instance_uuid
        if not instance_uuid:
            instance_uuid = get_instance_uuid()

        # Load stored identity if we still need either value
        if not instance_uuid or not slug:
            stored_identity = load_instance_identity()
            if stored_identity:
                if not instance_uuid:
                    instance_uuid = stored_identity.uuid
                if not slug:
                    slug = stored_identity.slug

        if not instance_uuid and not slug:
            raise CloudNodeClientError(
                "No instance identity found. Call register() first or provide slug/instance_uuid."
            )

        # Backend log endpoint is instance UUID scoped: /api/v1/cloud-node/{uuid}/log
        if not instance_uuid:
            raise CloudNodeClientError(
                "instance_uuid is required to send logs (backend requires UUID in URL path). "
                "Call register() first or set CYBERWAVE_CLOUD_NODE_INSTANCE_UUID."
            )

        payload = {
            "log_type": log_type,
            "log_content": log_content,
        }
        if slug:
            payload["slug"] = slug
        if instance_uuid:
            payload["instance_uuid"] = instance_uuid

        ws_slug = workspace_slug or self.workspace_slug
        if ws_slug:
            payload["workspace_slug"] = ws_slug

        try:
            endpoint = CLOUD_NODE_LOG_ENDPOINT.format(uuid=instance_uuid)
            response = self._client.post(endpoint, json=payload)

            if response.status_code == 200:
                return LogResponse.from_dict(response.json())

            self._handle_error_response(response, "send_log")

        except httpx.RequestError as e:
            raise CloudNodeClientError(f"Connection error during log send: {e}") from e

        raise CloudNodeClientError("Log send failed")

    def get_signed_url_for_attachment(
        self, workload_uuid: str, filename: str, workspace_slug: Optional[str] = None
    ) -> dict:
        """Get a signed URL for uploading an attachment to a workload.

        Args:
            workload_uuid: UUID of the workload
            filename: Name of the file to upload
            workspace_slug: Optional workspace slug override

        Returns:
            Dictionary with signed_url, filename, and expiration_hours

        Raises:
            CloudNodeClientError: If the request fails
        """
        endpoint = f"/api/v1/cloud-node-workloads/{workload_uuid}/attachments"
        payload = {"filename": filename}

        ws_slug = workspace_slug or self.workspace_slug
        params = {}
        if ws_slug:
            params["workspace_slug"] = ws_slug

        try:
            response = self._client.post(endpoint, json=payload, params=params)

            if response.status_code == 200:
                return response.json()

            self._handle_error_response(response, "get_signed_url_for_attachment")

        except httpx.RequestError as e:
            raise CloudNodeClientError(f"Connection error during signed URL request: {e}") from e

        raise CloudNodeClientError("Failed to get signed URL")

    def upload_workload_results(
        self,
        workload_uuid: str,
        filenames: list[str],
        signed_urls: list[str],
        attachment_uuids: list[str],
        workspace_slug: Optional[str] = None,
    ) -> dict:
        """Notify backend about uploaded workload result files.

        This endpoint is called after files have been uploaded to signed URLs.

        Args:
            workload_uuid: UUID of the workload
            filenames: List of filenames that were uploaded
            signed_urls: List of signed URLs that were used for upload
            attachment_uuids: List of attachment UUIDs
            workspace_slug: Optional workspace slug override

        Returns:
            Dictionary with success status and message

        Raises:
            CloudNodeClientError: If the request fails
        """
        endpoint = f"/api/v1/cloud-node-workloads/{workload_uuid}/upload-results"

        ws_slug = workspace_slug or self.workspace_slug
        params = {}
        if ws_slug:
            params["workspace_slug"] = ws_slug

        payload = {
            "filenames": filenames,
            "signed_urls": signed_urls,
            "attachment_uuids": attachment_uuids,
        }

        try:
            response = self._client.post(endpoint, json=payload, params=params)

            if response.status_code == 200:
                return response.json()

            self._handle_error_response(response, "upload_workload_results")

        except httpx.RequestError as e:
            raise CloudNodeClientError(f"Connection error during upload results: {e}") from e

        raise CloudNodeClientError("Failed to upload workload results")

    def complete_workload(
        self, workload_uuid: str, workspace_slug: Optional[str] = None
    ) -> dict:
        """Mark a workload as completed.

        Args:
            workload_uuid: UUID of the workload to complete
            workspace_slug: Optional workspace slug override

        Returns:
            Dictionary with success status and message

        Raises:
            CloudNodeClientError: If the request fails
        """
        endpoint = f"/api/v1/cloud-node-workloads/{workload_uuid}/complete"

        ws_slug = workspace_slug or self.workspace_slug
        params = {}
        if ws_slug:
            params["workspace_slug"] = ws_slug

        try:
            response = self._client.post(endpoint, params=params)

            if response.status_code == 200:
                return response.json()

            self._handle_error_response(response, "complete_workload")

        except httpx.RequestError as e:
            raise CloudNodeClientError(f"Connection error during workload completion: {e}") from e

        raise CloudNodeClientError("Failed to complete workload")

    def _handle_error_response(self, response: httpx.Response, operation: str) -> None:
        """Handle error responses from the API."""
        try:
            data = response.json()
        except Exception:
            data = None

        if response.status_code == 400:
            message = data.get("message", str(data)) if data else "Bad request"
            raise CloudNodeClientError(
                f"{operation.title()} failed: {message}",
                status_code=400,
                details=data,
            )

        if response.status_code == 401:
            raise CloudNodeClientError(
                "Authentication failed. Check your API token.",
                status_code=401,
            )

        if response.status_code == 403:
            raise CloudNodeClientError(
                "Permission denied. Check your workspace access.",
                status_code=403,
            )

        if response.status_code == 404:
            raise CloudNodeClientError(
                f"Resource not found during {operation}.",
                status_code=404,
            )

        raise CloudNodeClientError(
            f"{operation.title()} failed with status {response.status_code}",
            status_code=response.status_code,
            details=data,
        )
