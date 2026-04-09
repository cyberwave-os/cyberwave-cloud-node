"""Credentials management for Cyberwave Cloud Node.

Stores and retrieves credentials from the Cyberwave config directory,
compatible with cyberwave-cli and cyberwave-edge-core credentials.

Config directory resolution (same logic as cyberwave-cli and edge-core):
  1. CYBERWAVE_EDGE_CONFIG_DIR env var (explicit override)
  2. On macOS: ~/.cyberwave (Docker bind-mount compatibility)
  3. On other platforms: /etc/cyberwave if writable/creatable
  4. ~/.cyberwave as a fallback for non-root users
"""

import json
import os
import platform
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

_SYSTEM_CONFIG_DIR = Path("/etc/cyberwave")
_USER_CONFIG_DIR = Path.home() / ".cyberwave"


def _resolve_sudo_user_home() -> Path | None:
    """Return the invoking user's home directory when running via sudo."""
    sudo_user = os.getenv("SUDO_USER", "").strip()
    if not sudo_user:
        return None
    try:
        import pwd

        home = pwd.getpwnam(sudo_user).pw_dir
    except Exception:
        return None
    if not home:
        return None
    return Path(home)


def _resolve_config_dir() -> Path:
    """Pick the best writable config directory.

    Priority:
      1. CYBERWAVE_EDGE_CONFIG_DIR env var (explicit override)
      2. On macOS: ~/.cyberwave for Docker bind-mount compatibility
      3. On other platforms: /etc/cyberwave if writable/creatable
      4. ~/.cyberwave as a fallback for non-root users
    """
    env_override = os.getenv("CYBERWAVE_EDGE_CONFIG_DIR")
    if env_override:
        return Path(env_override)

    if platform.system() == "Darwin":
        sudo_home = _resolve_sudo_user_home()
        base_home = sudo_home or Path.home()
        return base_home / ".cyberwave"

    if _SYSTEM_CONFIG_DIR.exists():
        if os.access(_SYSTEM_CONFIG_DIR, os.W_OK):
            return _SYSTEM_CONFIG_DIR
        return _USER_CONFIG_DIR

    try:
        _SYSTEM_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        return _SYSTEM_CONFIG_DIR
    except PermissionError:
        pass

    return _USER_CONFIG_DIR


# Config directory shared with cyberwave-cli and cyberwave-edge-core.
CONFIG_DIR = _resolve_config_dir()
CREDENTIALS_FILE = CONFIG_DIR / "credentials.json"
INSTANCE_IDENTITY_FILE = CONFIG_DIR / "instance_identity.json"


@dataclass
class Credentials:
    """User credentials for the Cyberwave API."""

    token: str
    email: Optional[str] = None
    created_at: Optional[str] = None
    workspace_uuid: Optional[str] = None
    workspace_name: Optional[str] = None
    workspace_slug: Optional[str] = None
    cyberwave_environment: Optional[str] = None
    cyberwave_base_url: Optional[str] = None
    cyberwave_mqtt_host: Optional[str] = None
    cyberwave_mqtt_port: Optional[str] = None

    def runtime_envs(self) -> dict[str, str]:
        """Return persisted runtime env vars shared with cloud node launches."""
        envs: dict[str, str] = {}
        if self.cyberwave_environment:
            envs["CYBERWAVE_ENVIRONMENT"] = self.cyberwave_environment
        if self.cyberwave_base_url:
            envs["CYBERWAVE_BASE_URL"] = self.cyberwave_base_url
        if self.cyberwave_mqtt_host:
            envs["CYBERWAVE_MQTT_HOST"] = self.cyberwave_mqtt_host
        if self.cyberwave_mqtt_port:
            envs["CYBERWAVE_MQTT_PORT"] = self.cyberwave_mqtt_port
        return envs

    def to_dict(self) -> dict:
        """Convert credentials to dictionary."""
        payload = {
            "token": self.token,
            "email": self.email,
            "created_at": self.created_at,
            "workspace_uuid": self.workspace_uuid,
            "workspace_name": self.workspace_name,
            "workspace_slug": self.workspace_slug,
        }
        envs = self.runtime_envs()
        if envs:
            payload["envs"] = envs
        return {key: value for key, value in payload.items() if value is not None}

    @classmethod
    def from_dict(cls, data: dict) -> "Credentials":
        """Create credentials from dictionary."""
        raw_envs = data.get("envs")
        envs: dict[str, object] = raw_envs if isinstance(raw_envs, dict) else {}

        def _env_value(key: str) -> Optional[str]:
            value = envs.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            flat_value = data.get(key)
            if isinstance(flat_value, str) and flat_value.strip():
                return flat_value.strip()
            return None

        return cls(
            token=data.get("token", ""),
            email=data.get("email"),
            created_at=data.get("created_at"),
            workspace_uuid=data.get("workspace_uuid"),
            workspace_name=data.get("workspace_name"),
            workspace_slug=data.get("workspace_slug"),
            cyberwave_environment=_env_value("CYBERWAVE_ENVIRONMENT"),
            cyberwave_base_url=_env_value("CYBERWAVE_BASE_URL"),
            cyberwave_mqtt_host=_env_value("CYBERWAVE_MQTT_HOST"),
            cyberwave_mqtt_port=_env_value("CYBERWAVE_MQTT_PORT"),
        )


def ensure_config_dir() -> None:
    """Ensure the config directory exists with proper permissions."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    # Apply user-only permissions only for home-directory configs; system
    # directories like /etc/cyberwave use their own permission model.
    if os.name != "nt" and CONFIG_DIR != _SYSTEM_CONFIG_DIR:
        os.chmod(CONFIG_DIR, 0o700)


def save_credentials(credentials: Credentials) -> None:
    """Save credentials to the config file."""
    ensure_config_dir()

    # Add timestamp if not present
    if not credentials.created_at:
        credentials.created_at = datetime.utcnow().isoformat()

    with open(CREDENTIALS_FILE, "w") as f:
        json.dump(credentials.to_dict(), f, indent=2)

    # Set file permissions to user-only on Unix systems
    if os.name != "nt":
        os.chmod(CREDENTIALS_FILE, 0o600)


def load_credentials() -> Optional[Credentials]:
    """Load credentials from the config file."""
    if not CREDENTIALS_FILE.exists():
        return None

    try:
        with open(CREDENTIALS_FILE, "r") as f:
            data = json.load(f)
            return Credentials.from_dict(data)
    except (json.JSONDecodeError, KeyError):
        return None


def clear_credentials() -> None:
    """Remove stored credentials."""
    if CREDENTIALS_FILE.exists():
        CREDENTIALS_FILE.unlink()


def get_token() -> Optional[str]:
    """Get the stored token, if any."""
    creds = load_credentials()
    return creds.token if creds else None


def get_workspace_slug() -> Optional[str]:
    """Get the stored workspace slug, if any."""
    creds = load_credentials()
    return creds.workspace_slug if creds else None


# =============================================================================
# Instance Identity (UUID and slug assigned by the backend)
# =============================================================================


@dataclass
class InstanceIdentity:
    """Instance identity assigned by the backend during registration."""

    uuid: str
    slug: str
    workspace_slug: Optional[str] = None
    registered_at: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert identity to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "InstanceIdentity":
        """Create identity from dictionary."""
        return cls(
            uuid=data.get("uuid", ""),
            slug=data.get("slug", ""),
            workspace_slug=data.get("workspace_slug"),
            registered_at=data.get("registered_at"),
        )


def save_instance_identity(identity: InstanceIdentity) -> None:
    """Save instance identity to the config file."""
    ensure_config_dir()

    # Add timestamp if not present
    if not identity.registered_at:
        identity.registered_at = datetime.utcnow().isoformat()

    with open(INSTANCE_IDENTITY_FILE, "w") as f:
        json.dump(identity.to_dict(), f, indent=2)

    # Set file permissions to user-only on Unix systems
    if os.name != "nt":
        os.chmod(INSTANCE_IDENTITY_FILE, 0o600)


def load_instance_identity() -> Optional[InstanceIdentity]:
    """Load instance identity from the config file."""
    if not INSTANCE_IDENTITY_FILE.exists():
        return None

    try:
        with open(INSTANCE_IDENTITY_FILE, "r") as f:
            data = json.load(f)
            return InstanceIdentity.from_dict(data)
    except (json.JSONDecodeError, KeyError):
        return None


def clear_instance_identity() -> None:
    """Remove stored instance identity."""
    if INSTANCE_IDENTITY_FILE.exists():
        INSTANCE_IDENTITY_FILE.unlink()


def get_instance_uuid() -> Optional[str]:
    """Get the stored instance UUID, if any."""
    identity = load_instance_identity()
    return identity.uuid if identity else None


def get_instance_slug() -> Optional[str]:
    """Get the stored instance slug, if any."""
    identity = load_instance_identity()
    return identity.slug if identity else None
