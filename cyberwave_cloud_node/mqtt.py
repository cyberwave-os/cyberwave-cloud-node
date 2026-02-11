"""MQTT v5 client for Cyberwave Cloud Node.

This module handles MQTT v5 communication with request/response patterns:
- Publishing messages with correlation data
- Subscribing to topics and handling responses
- Request/response pattern for instance creation
- Command subscription and response publishing
"""

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Optional

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .config import get_api_token

logger = logging.getLogger(__name__)


class MQTTError(Exception):
    """Raised when an MQTT operation fails."""

    pass


@dataclass
class MQTTResponse:
    """Response from an MQTT request/response operation."""

    success: bool
    payload: dict[str, Any]
    correlation_data: Optional[bytes] = None


class CloudNodeMQTTClient:
    """MQTT v5 client for Cloud Node with request/response support.

    Features:
    - MQTT v5 protocol with properties support
    - Request/response pattern with correlation data
    - Automatic reconnection
    - Topic management
    - Command subscription with callbacks
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        keepalive: int = 60,
        client_id: Optional[str] = None,
        topic_prefix: str = "",
    ):
        """Initialize MQTT client.

        Args:
            host: MQTT broker hostname
            port: MQTT broker port
            username: MQTT username (optional)
            password: MQTT password (optional)
            keepalive: Keepalive interval in seconds
            client_id: Client ID (auto-generated if not provided)
            topic_prefix: Prefix for all topics (for environment separation)
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.keepalive = keepalive
        self.topic_prefix = topic_prefix

        # Generate client ID if not provided
        if not client_id:
            client_id = f"cloud_node_{uuid.uuid4().hex[:8]}"

        # Create MQTT v5 client
        self._client = mqtt.Client(
            client_id=client_id,
            protocol=mqtt.MQTTv5,
            transport="tcp",
        )

        # Set authentication
        if username and password:
            self._client.username_pw_set(username, password)

        # Connection state
        self._connected = False
        self._connect_event = asyncio.Event()
        self._disconnect_event = asyncio.Event()

        # Pending requests (for request/response pattern)
        self._pending_requests: dict[bytes, asyncio.Future[MQTTResponse]] = {}

        # Response topic for this client
        self._response_topic = f"{self.topic_prefix}cyberwave/response/{client_id}"

        # Setup callbacks (type: ignore for MQTT v5 callback signature differences)
        self._client.on_connect = self._on_connect  # type: ignore[assignment]
        self._client.on_disconnect = self._on_disconnect  # type: ignore[assignment]
        self._client.on_message = self._on_message  # type: ignore[assignment]

        # Event loop reference
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _on_connect(
        self, client: mqtt.Client, userdata: Any, flags: dict, rc: int, properties: Properties
    ) -> None:
        """Callback when connected to broker (MQTT v5 always provides properties)."""
        if rc == 0:
            logger.info(f"Connected to MQTT broker at {self.host}:{self.port}")
            self._connected = True

            # Subscribe to our response topic for request/response pattern
            client.subscribe(self._response_topic, qos=1)
            logger.info(f"Subscribed to response topic: {self._response_topic}")

            if self._loop:
                self._loop.call_soon_threadsafe(self._connect_event.set)
        else:
            logger.error(f"Failed to connect to MQTT broker, return code: {rc}")
            self._connected = False

    def _on_disconnect(
        self, client: mqtt.Client, userdata: Any, rc: int, properties: Properties
    ) -> None:
        """Callback when disconnected from broker (MQTT v5 always provides properties)."""
        logger.warning(f"Disconnected from MQTT broker, return code: {rc}")
        self._connected = False
        if self._loop:
            self._loop.call_soon_threadsafe(self._disconnect_event.set)
            self._loop.call_soon_threadsafe(self._connect_event.clear)

    def _on_message(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
        """Callback when message received."""
        try:
            topic = message.topic
            payload = message.payload
            properties = message.properties

            logger.debug(f"Received message on topic: {topic}")

            # Check if this is a response to a pending request
            if topic == self._response_topic and properties:
                correlation_data = getattr(properties, "CorrelationData", None)
                if correlation_data and correlation_data in self._pending_requests:
                    # This is a response to one of our requests
                    future = self._pending_requests.pop(correlation_data)

                    # Parse payload
                    try:
                        payload_dict = json.loads(payload.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.error(f"Failed to decode response payload: {e}")
                        payload_dict = {}

                    response = MQTTResponse(
                        success=payload_dict.get("success", False),
                        payload=payload_dict,
                        correlation_data=correlation_data,
                    )

                    if self._loop:
                        self._loop.call_soon_threadsafe(future.set_result, response)
                    return

            # For other messages, log them (command handlers are set up separately)
            logger.debug(f"Unhandled message on topic {topic}")

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    async def connect(self) -> None:
        """Connect to MQTT broker asynchronously."""
        self._loop = asyncio.get_running_loop()

        try:
            # Start the network loop in a separate thread
            self._client.loop_start()

            # Connect to broker
            self._client.connect(
                self.host,
                self.port,
                self.keepalive,
            )

            # Wait for connection with timeout
            try:
                await asyncio.wait_for(self._connect_event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                raise MQTTError("Connection timeout after 10 seconds")

            if not self._connected:
                raise MQTTError("Failed to connect to MQTT broker")

        except Exception as e:
            self._client.loop_stop()
            raise MQTTError(f"Failed to connect to MQTT broker: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from MQTT broker."""
        if self._connected:
            self._client.disconnect()
            # Wait for disconnect confirmation
            try:
                await asyncio.wait_for(self._disconnect_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Disconnect timeout")

        self._client.loop_stop()
        self._connected = False

    async def publish_request(
        self,
        topic: str,
        payload: dict[str, Any],
        timeout: float = 30.0,
    ) -> MQTTResponse:
        """Publish a request and wait for response using MQTT v5 request/response pattern.

        Args:
            topic: Topic to publish to
            payload: Payload dictionary (will be JSON-encoded)
            timeout: Timeout in seconds to wait for response

        Returns:
            MQTTResponse with the response payload

        Raises:
            MQTTError: If not connected or request fails
            asyncio.TimeoutError: If no response received within timeout
        """
        if not self._connected:
            raise MQTTError("Not connected to MQTT broker")

        # Generate correlation data
        correlation_data = uuid.uuid4().bytes

        # Create properties with response topic and correlation data
        properties = Properties(PacketTypes.PUBLISH)
        properties.ResponseTopic = self._response_topic  # type: ignore[attr-defined]
        properties.CorrelationData = correlation_data  # type: ignore[attr-defined]

        # Create future for response
        future: asyncio.Future[MQTTResponse] = asyncio.Future()
        self._pending_requests[correlation_data] = future

        try:
            # Publish request
            payload_json = json.dumps(payload)
            result = self._client.publish(
                topic=topic,
                payload=payload_json,
                qos=1,
                properties=properties,
            )

            # Check if publish succeeded
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise MQTTError(f"Failed to publish message: {result.rc}")

            logger.debug(f"Published request to {topic}, waiting for response...")

            # Wait for response with timeout
            response = await asyncio.wait_for(future, timeout=timeout)
            logger.debug(f"Received response for request on {topic}")

            return response

        except asyncio.TimeoutError:
            # Clean up pending request
            self._pending_requests.pop(correlation_data, None)
            raise asyncio.TimeoutError(f"No response received within {timeout} seconds")

        except Exception as e:
            # Clean up pending request
            self._pending_requests.pop(correlation_data, None)
            raise MQTTError(f"Request failed: {e}") from e

    async def publish_message(
        self,
        topic: str,
        payload: dict[str, Any],
        qos: int = 0,
    ) -> None:
        """Publish a one-way message (no response expected).

        Args:
            topic: Topic to publish to
            payload: Payload dictionary (will be JSON-encoded)
            qos: QoS level (0, 1, or 2)

        Raises:
            MQTTError: If not connected or publish fails
        """
        if not self._connected:
            raise MQTTError("Not connected to MQTT broker")

        try:
            payload_json = json.dumps(payload)
            result = self._client.publish(
                topic=topic,
                payload=payload_json,
                qos=qos,
            )

            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise MQTTError(f"Failed to publish message: {result.rc}")

            logger.debug(f"Published message to {topic}")

        except Exception as e:
            raise MQTTError(f"Failed to publish message: {e}") from e

    def subscribe_command(
        self,
        topic: str,
        callback: Callable[[dict[str, Any], Optional[Properties]], None],
    ) -> None:
        """Subscribe to a command topic with a callback.

        Args:
            topic: Topic to subscribe to
            callback: Callback function(payload_dict, properties)

        Raises:
            MQTTError: If not connected or subscribe fails
        """
        if not self._connected:
            raise MQTTError("Not connected to MQTT broker")

        def message_callback(client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
            """Wrapper callback for command messages."""
            try:
                # Parse payload
                try:
                    payload_dict = json.loads(message.payload.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(f"Failed to decode command payload: {e}")
                    payload_dict = {}

                # Call user callback
                callback(payload_dict, message.properties)

            except Exception as e:
                logger.error(f"Error in command callback: {e}", exc_info=True)

        # Subscribe to topic
        self._client.message_callback_add(topic, message_callback)
        result = self._client.subscribe(topic, qos=1)

        if result[0] != mqtt.MQTT_ERR_SUCCESS:
            raise MQTTError(f"Failed to subscribe to {topic}")

        logger.info(f"Subscribed to command topic: {topic}")

    async def request_instance(
        self,
        profile_slug: str,
        slug: Optional[str] = None,
        workspace_uuid: Optional[str] = None,
        provider: str = "self-hosted",
        visibility: str = "private",
        timeout: float = 30.0,
    ) -> MQTTResponse:
        """Request a cloud node instance creation via MQTT.

        This uses the MQTT v5 request/response pattern to create an instance.

        Args:
            profile_slug: Profile slug for the instance
            slug: Optional slug for the instance
            workspace_uuid: Optional workspace UUID
            provider: Provider type (default: self-hosted)
            visibility: Visibility setting (default: private)
            timeout: Timeout in seconds

        Returns:
            MQTTResponse with instance details (uuid, slug, status, profile_slug)

        Raises:
            MQTTError: If request fails
            asyncio.TimeoutError: If no response within timeout
        """
        # Get API token to use as resource UUID
        token = get_api_token()
        if not token:
            raise MQTTError("API token is required for instance creation")

        # Build request payload
        payload = {
            "profile_slug": profile_slug,
            "provider": provider,
            "visibility": visibility,
        }

        if slug:
            payload["slug"] = slug
        if workspace_uuid:
            payload["workspace_uuid"] = workspace_uuid

        # Publish request to token topic
        topic = f"{self.topic_prefix}cyberwave/cloud-node-token/{token}/request-instance"

        logger.info(f"Requesting instance creation via MQTT (profile: {profile_slug})")

        response = await self.publish_request(topic, payload, timeout=timeout)

        if not response.success:
            error_msg = response.payload.get("message", "Unknown error")
            raise MQTTError(f"Instance creation failed: {error_msg}")

        logger.info(
            f"Instance created successfully: "
            f"uuid={response.payload.get('uuid')}, slug={response.payload.get('slug')}"
        )

        return response

    async def register_instance(
        self,
        instance_uuid: str,
        profile_slug: str,
        timeout: float = 30.0,
    ) -> MQTTResponse:
        """Register a cloud node instance after creation via MQTT.

        This uses the MQTT v5 request/response pattern to register an instance.
        It checks the instance state and only registers if needed (PROVISIONING, FAILED, TERMINATED).
        If the instance is already registered (READY, BUSY), it returns success with existing info.

        Args:
            instance_uuid: UUID of the instance to register (from request_instance)
            profile_slug: Profile slug for the instance
            timeout: Timeout in seconds

        Returns:
            MQTTResponse with instance details (uuid, slug, status)

        Raises:
            MQTTError: If request fails
            asyncio.TimeoutError: If no response within timeout
        """
        # Build request payload
        payload = {
            "profile_slug": profile_slug,
        }

        # Publish request to cloud-node register topic
        topic = f"{self.topic_prefix}cyberwave/cloud-node/{instance_uuid}/register"

        logger.info(f"Registering instance {instance_uuid} via MQTT (profile: {profile_slug})")

        response = await self.publish_request(topic, payload, timeout=timeout)

        if not response.success:
            error_msg = response.payload.get("message", "Unknown error")
            raise MQTTError(f"Instance registration failed: {error_msg}")

        logger.info(
            f"Instance registered successfully: "
            f"uuid={response.payload.get('uuid')}, slug={response.payload.get('slug')}"
        )

        return response

    async def send_heartbeat(
        self,
        instance_uuid: str,
        timeout: float = 10.0,
    ) -> MQTTResponse:
        """Send heartbeat via MQTT.

        Args:
            instance_uuid: UUID of the cloud node instance
            timeout: Timeout in seconds

        Returns:
            MQTTResponse with heartbeat confirmation

        Raises:
            MQTTError: If request fails
            asyncio.TimeoutError: If no response within timeout
        """
        topic = f"{self.topic_prefix}cyberwave/cloud-node/{instance_uuid}/heartbeat"

        logger.debug(f"Sending heartbeat for instance {instance_uuid}")

        response = await self.publish_request(topic, {}, timeout=timeout)

        if not response.success:
            error_msg = response.payload.get("message", "Unknown error")
            raise MQTTError(f"Heartbeat failed: {error_msg}")

        return response

    async def send_log(
        self,
        instance_uuid: str,
        log_content: str,
        log_type: str = "stdout",
        timeout: float = 10.0,
    ) -> MQTTResponse:
        """Send log content via MQTT.

        Args:
            instance_uuid: UUID of the cloud node instance
            log_content: Log content to send
            log_type: Type of log ("stdout", "stderr", "system", "app")
            timeout: Timeout in seconds

        Returns:
            MQTTResponse with log save confirmation

        Raises:
            MQTTError: If request fails
            asyncio.TimeoutError: If no response within timeout
        """
        topic = f"{self.topic_prefix}cyberwave/cloud-node/{instance_uuid}/logs"

        payload = {
            "instance_uuid": instance_uuid,
            "log_type": log_type,
            "log_content": log_content,
        }

        logger.debug(
            f"Sending log for instance {instance_uuid} "
            f"(type: {log_type}, size: {len(log_content)} bytes)"
        )

        response = await self.publish_request(topic, payload, timeout=timeout)

        if not response.success:
            error_msg = response.payload.get("message", "Unknown error")
            raise MQTTError(f"Log send failed: {error_msg}")

        return response

    async def notify_terminated(
        self,
        instance_uuid: str,
        timeout: float = 10.0,
    ) -> MQTTResponse:
        """Notify backend that instance is terminating via MQTT.

        Args:
            instance_uuid: UUID of the cloud node instance
            timeout: Timeout in seconds

        Returns:
            MQTTResponse with termination confirmation

        Raises:
            MQTTError: If request fails
            asyncio.TimeoutError: If no response within timeout
        """
        topic = f"{self.topic_prefix}cyberwave/cloud-node/{instance_uuid}/terminate"

        logger.info(f"Notifying backend of termination for instance {instance_uuid}")

        response = await self.publish_request(topic, {}, timeout=timeout)

        if not response.success:
            error_msg = response.payload.get("message", "Unknown error")
            raise MQTTError(f"Termination notification failed: {error_msg}")

        return response

    async def notify_failed(
        self,
        instance_uuid: str,
        error: str,
        timeout: float = 10.0,
    ) -> MQTTResponse:
        """Notify backend that instance has failed via MQTT.

        Args:
            instance_uuid: UUID of the cloud node instance
            error: Error message describing the failure
            timeout: Timeout in seconds

        Returns:
            MQTTResponse with failure notification confirmation

        Raises:
            MQTTError: If request fails
            asyncio.TimeoutError: If no response within timeout
        """
        topic = f"{self.topic_prefix}cyberwave/cloud-node/{instance_uuid}/fail"

        payload = {
            "instance_uuid": instance_uuid,
            "error": error,
        }

        logger.error(f"Notifying backend of failure for instance {instance_uuid}: {error}")

        response = await self.publish_request(topic, payload, timeout=timeout)

        if not response.success:
            error_msg = response.payload.get("message", "Unknown error")
            raise MQTTError(f"Failure notification failed: {error_msg}")

        return response

    @property
    def connected(self) -> bool:
        """Check if connected to broker."""
        return self._connected
