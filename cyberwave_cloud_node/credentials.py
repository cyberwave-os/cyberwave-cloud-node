"""Credentials management for Cyberwave Cloud Node.

Stores and retrieves credentials from ~/.cyberwave/credentials.json,
compatible with cyberwave-cli credentials.
"""

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

# Config directory (shared with cyberwave-cli)
CONFIG_DIR = Path.home() / ".cyberwave"
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

    def to_dict(self) -> dict:
        """Convert credentials to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Credentials":
        """Create credentials from dictionary."""
        return cls(
            token=data.get("token", ""),
            email=data.get("email"),
            created_at=data.get("created_at"),
            workspace_uuid=data.get("workspace_uuid"),
            workspace_name=data.get("workspace_name"),
            workspace_slug=data.get("workspace_slug"),
        )


def ensure_config_dir() -> None:
    """Ensure the config directory exists with proper permissions."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    # Set directory permissions to user-only on Unix systems
    if os.name != "nt":
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
