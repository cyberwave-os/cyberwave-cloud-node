"""Cyberwave Cloud Node - Turn any computer into a GPU node for inference and training.

This package provides tools to register any computer as a Cyberwave Cloud Node,
enabling it to receive and execute inference and training workloads.

Basic Usage:
    # Run from a directory with cyberwave.yml
    from cyberwave_cloud_node import CloudNode

    node = CloudNode.from_config_file(slug="my-gpu-node")
    node.run()

Authentication (in order of priority):
    1. CYBERWAVE_TOKEN environment variable
    2. .env file in current directory
    3. .env file in ~/.cyberwave/
    4. Stored credentials from cyberwave-cli (~/.cyberwave/credentials.json)

Environment Variables:
    CYBERWAVE_BASE_URL: API base URL (default: https://api.cyberwave.com)
    CYBERWAVE_TOKEN: Your Cyberwave API token
    CYBERWAVE_WORKSPACE_SLUG: Your workspace slug (optional)
    CYBERWAVE_INSTANCE_SLUG: Instance slug hint (optional, for automated deployments)
"""

from .client import (
    CloudNodeClient,
    CloudNodeClientError,
    FailedResponse,
    HeartbeatResponse,
    RegisterResponse,
    TerminatedResponse,
)
from .cloud_node import CloudNode, CloudNodeError, WorkloadResult
from .config import CloudNodeConfig, load_dotenv_files
from .credentials import (
    Credentials,
    clear_credentials,
    get_token,
    get_workspace_slug,
    load_credentials,
    save_credentials,
)

__all__ = [
    # Main classes
    "CloudNode",
    "CloudNodeClient",
    "CloudNodeConfig",
    # Results
    "WorkloadResult",
    "RegisterResponse",
    "HeartbeatResponse",
    "TerminatedResponse",
    "FailedResponse",
    # Credentials
    "Credentials",
    "load_credentials",
    "save_credentials",
    "clear_credentials",
    "get_token",
    "get_workspace_slug",
    # Utilities
    "load_dotenv_files",
    # Exceptions
    "CloudNodeError",
    "CloudNodeClientError",
]
