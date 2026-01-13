"""Configuration and constants for Cyberwave Cloud Node."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv

from . import credentials as creds_module  # noqa: E402

# API Configuration
DEFAULT_API_URL = "https://api.cyberwave.com"
CLOUD_NODE_REGISTER_ENDPOINT = "/api/v1/cloud-node/register"
CLOUD_NODE_HEARTBEAT_ENDPOINT = "/api/v1/cloud-node/heartbeat"
CLOUD_NODE_TERMINATED_ENDPOINT = "/api/v1/cloud-node/terminated"
CLOUD_NODE_FAILED_ENDPOINT = "/api/v1/cloud-node/failed"

# MQTT Configuration
DEFAULT_MQTT_HOST = "mqtt.cyberwave.com"
DEFAULT_MQTT_PORT = 1883

# Default config file name
CONFIG_FILE_NAME = "cyberwave.yml"
ENV_FILE_NAME = ".env"

# Heartbeat interval in seconds
DEFAULT_HEARTBEAT_INTERVAL = 30


def load_dotenv_files(working_dir: Optional[Path] = None) -> None:
    """Load .env files from working directory and home directory.

    Priority (highest to lowest):
    1. Environment variables (already set)
    2. .env in working directory
    3. .env in ~/.cyberwave/
    """
    # Load from ~/.cyberwave/.env first (lowest priority)
    cyberwave_env = creds_module.CONFIG_DIR / ENV_FILE_NAME
    if cyberwave_env.exists():
        load_dotenv(cyberwave_env, override=False)

    # Load from working directory (higher priority, but doesn't override existing)
    if working_dir:
        local_env = working_dir / ENV_FILE_NAME
    else:
        local_env = Path.cwd() / ENV_FILE_NAME

    if local_env.exists():
        load_dotenv(local_env, override=False)


def get_api_url() -> str:
    """Get the API URL from environment or default."""
    return os.getenv("CYBERWAVE_API_URL", DEFAULT_API_URL)


def get_api_token() -> Optional[str]:
    """Get the API token from environment or stored credentials.

    Priority:
    1. CYBERWAVE_API_TOKEN environment variable
    2. Stored credentials in ~/.cyberwave/credentials.json
    """
    token = os.getenv("CYBERWAVE_API_TOKEN")
    if token:
        return token

    # Fall back to stored credentials
    return creds_module.get_token()


def get_workspace_slug() -> Optional[str]:
    """Get the workspace slug from environment or stored credentials.

    Priority:
    1. CYBERWAVE_WORKSPACE_SLUG environment variable
    2. Stored credentials in ~/.cyberwave/credentials.json
    """
    slug = os.getenv("CYBERWAVE_WORKSPACE_SLUG")
    if slug:
        return slug

    # Fall back to stored credentials
    return creds_module.get_workspace_slug()


def get_mqtt_host() -> str:
    """Get MQTT broker host from environment or default."""
    return os.getenv("CYBERWAVE_MQTT_HOST", DEFAULT_MQTT_HOST)


def get_mqtt_port() -> int:
    """Get MQTT broker port from environment or default."""
    return int(os.getenv("CYBERWAVE_MQTT_PORT", str(DEFAULT_MQTT_PORT)))


def get_mqtt_username() -> Optional[str]:
    """Get MQTT username from environment."""
    return os.getenv("CYBERWAVE_MQTT_USERNAME")


def get_mqtt_password() -> Optional[str]:
    """Get MQTT password from environment."""
    return os.getenv("CYBERWAVE_MQTT_PASSWORD")


@dataclass
class CloudNodeConfig:
    """Configuration for a Cyberwave Cloud Node.

    Loaded from cyberwave.yml in the project root.
    """

    install_script: Optional[str] = None
    inference: Optional[str] = None
    training: Optional[str] = None
    profile_slug: str = "default"
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL
    mqtt_host: str = DEFAULT_MQTT_HOST
    mqtt_port: int = DEFAULT_MQTT_PORT
    mqtt_username: Optional[str] = None
    mqtt_password: Optional[str] = None
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "CloudNodeConfig":
        """Create config from a dictionary."""
        cloud_node_data = data.get("cyberwave-cloud-node", data)

        return cls(
            install_script=cloud_node_data.get("install_script"),
            inference=cloud_node_data.get("inference"),
            training=cloud_node_data.get("training"),
            profile_slug=cloud_node_data.get("profile_slug", "default"),
            heartbeat_interval=cloud_node_data.get(
                "heartbeat_interval", DEFAULT_HEARTBEAT_INTERVAL
            ),
            mqtt_host=cloud_node_data.get("mqtt_host", get_mqtt_host()),
            mqtt_port=cloud_node_data.get("mqtt_port", get_mqtt_port()),
            mqtt_username=cloud_node_data.get("mqtt_username", get_mqtt_username()),
            mqtt_password=cloud_node_data.get("mqtt_password", get_mqtt_password()),
            extra={
                k: v
                for k, v in cloud_node_data.items()
                if k
                not in {
                    "install_script",
                    "inference",
                    "training",
                    "profile_slug",
                    "heartbeat_interval",
                    "mqtt_host",
                    "mqtt_port",
                    "mqtt_username",
                    "mqtt_password",
                }
            },
        )

    @classmethod
    def from_file(cls, path: Optional[Path] = None) -> "CloudNodeConfig":
        """Load configuration from a YAML file.

        Args:
            path: Path to the config file. Defaults to cyberwave.yml in current directory.

        Returns:
            CloudNodeConfig instance

        Raises:
            FileNotFoundError: If the config file doesn't exist
            yaml.YAMLError: If the config file is invalid YAML
        """
        if path is None:
            path = Path.cwd() / CONFIG_FILE_NAME

        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}

        return cls.from_dict(data)

    @classmethod
    def from_env(cls) -> "CloudNodeConfig":
        """Create config from environment variables.

        Useful for containerized deployments where config is passed via env vars.
        """
        return cls(
            install_script=os.getenv("CYBERWAVE_INSTALL_SCRIPT"),
            inference=os.getenv("CYBERWAVE_INFERENCE_CMD"),
            training=os.getenv("CYBERWAVE_TRAINING_CMD"),
            profile_slug=os.getenv("CYBERWAVE_PROFILE_SLUG", "default"),
            heartbeat_interval=int(
                os.getenv("CYBERWAVE_HEARTBEAT_INTERVAL", str(DEFAULT_HEARTBEAT_INTERVAL))
            ),
            mqtt_host=get_mqtt_host(),
            mqtt_port=get_mqtt_port(),
            mqtt_username=get_mqtt_username(),
            mqtt_password=get_mqtt_password(),
        )
