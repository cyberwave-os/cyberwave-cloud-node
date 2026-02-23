"""Command-line interface for Cyberwave Cloud Node.

Usage:
    cyberwave-cloud-node start
    cyberwave-cloud-node start --config ./cyberwave.yml
    cyberwave-cloud-node start --slug my-node  # Optional: provide a slug hint
"""

import argparse
import logging
import os
import sys
from importlib.metadata import version
from pathlib import Path

from .cloud_node import CloudNode, CloudNodeError
from .config import CloudNodeConfig, get_api_token, get_instance_slug, load_dotenv_files


def init_sentry() -> None:
    """Initialize Sentry SDK if DSN is provided."""
    sentry_dsn = os.environ.get("SENTRY_DSN")
    if sentry_dsn:
        import sentry_sdk

        logger = logging.getLogger("cyberwave-cloud-node")
        logger.info("Initializing Sentry SDK...")

        sentry_sdk.init(
            dsn=sentry_dsn,
            enable_logs=True,
            # Set traces_sample_rate to 1.0 to capture 100%
            # of the transactions for performance monitoring.
            # We recommend adjusting this value in production.
            traces_sample_rate=float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "1.0")),
            # Set profiles_sample_rate to profile 100% of sampled transactions.
            # We recommend adjusting this value in production.
            profiles_sample_rate=float(
                os.environ.get("SENTRY_PROFILES_SAMPLE_RATE", "1.0")
            ),
            environment=os.environ.get("ENVIRONMENT", "local"),
            profile_session_sample_rate=float(
                os.environ.get("SENTRY_PROFILE_SESSION_SAMPLE_RATE", "1.0")
            ),
            release=os.environ.get("SENTRY_RELEASE"),
            profile_lifecycle="trace",
        )
        logger.info("Sentry SDK initialized successfully")
    else:
        logger = logging.getLogger("cyberwave-cloud-node")
        logger.debug("Sentry DSN not set, skipping Sentry initialization")


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the CLI."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main() -> int:
    """Main entry point for the CLI."""

    parser = argparse.ArgumentParser(
        prog="cyberwave-cloud-node",
        description="Turn any computer into a Cyberwave Cloud Node for inference and training",
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {version('cyberwave-cloud-node')}",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Start command
    start_parser = subparsers.add_parser(
        "start",
        help="Start the Cloud Node service",
    )
    start_parser.add_argument(
        "--slug",
        default=None,
        help="Optional slug hint for this node (backend assigns the actual slug)",
    )
    start_parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to cyberwave.yml config file (default: ./cyberwave.yml)",
    )
    start_parser.add_argument(
        "--profile",
        default=None,
        help="Node profile slug (overrides config file)",
    )
    start_parser.add_argument(
        "--mqtt-host",
        default=None,
        dest="mqtt_host",
        help="MQTT broker host (overrides config file)",
    )
    start_parser.add_argument(
        "--mqtt-port",
        type=int,
        default=None,
        dest="mqtt_port",
        help="MQTT broker port (overrides config file)",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)
    
    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "start":
        return start_node(args)

    return 0


def start_node(args: argparse.Namespace) -> int:
    """Start the Cloud Node service."""
    logger = logging.getLogger("cyberwave-cloud-node")

    # Determine working directory first for .env loading
    config_path = args.config
    if config_path:
        working_dir = config_path.parent
    else:
        working_dir = Path.cwd()

    # Load .env files (from working dir and ~/.cyberwave/)
    load_dotenv_files(working_dir)
    # Initialize Sentry after dotenv loading so SENTRY_DSN from .env is honored.
    init_sentry()

    # Check for API token (now checks env vars, .env files, and stored credentials)
    if not get_api_token():
        logger.error(
            "API token is required. You can provide it via:\n"
            "  1. CYBERWAVE_API_KEY environment variable\n"
            "  2. .env file in current directory or ~/.cyberwave/.env\n"
            "  3. Login with cyberwave-cli (stores in ~/.cyberwave/credentials.json)\n\n"
            "Get your token from https://cyberwave.com/profile"
        )
        return 1

    try:
        # Load config
        if config_path:
            config = CloudNodeConfig.from_file(config_path)
        else:
            try:
                config = CloudNodeConfig.from_file()
            except FileNotFoundError:
                logger.info("No cyberwave.yml found, using environment configuration")
                config = CloudNodeConfig.from_env()

        # Apply CLI overrides
        if args.profile:
            config.profile_slug = args.profile
        if args.mqtt_host:
            config.mqtt_host = args.mqtt_host
        if args.mqtt_port:
            config.mqtt_port = args.mqtt_port

        # Get instance slug: CLI arg > env var > stored identity
        slug = args.slug or get_instance_slug()

        if slug:
            logger.info(f"Starting Cloud Node with slug hint '{slug}'")
        logger.info(f"Profile: {config.profile_slug}")
        logger.info(f"MQTT Broker: {config.mqtt_host}:{config.mqtt_port}")
        if config.inference:
            logger.info(f"Inference command: {config.inference}")
        if config.training:
            logger.info(f"Training command: {config.training}")

        # Create and run the node
        node = CloudNode(
            config=config,
            slug=slug,  # Optional hint, backend assigns actual slug
            working_dir=working_dir,
        )
        node.run()

        return 0

    except CloudNodeError as e:
        logger.error(f"Cloud Node error: {e}")
        return 1

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
