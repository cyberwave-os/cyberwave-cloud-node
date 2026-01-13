"""Command-line interface for Cyberwave Cloud Node.

Usage:
    cyberwave-cloud-node start --slug my-node
    cyberwave-cloud-node start --slug my-node --config ./cyberwave.yml
"""

import argparse
import logging
import sys
from pathlib import Path

from .cloud_node import CloudNode, CloudNodeError
from .config import CloudNodeConfig, get_api_token, load_dotenv_files


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
        required=True,
        help="Unique identifier for this node within your workspace",
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

    # Check for API token (now checks env vars, .env files, and stored credentials)
    if not get_api_token():
        logger.error(
            "API token is required. You can provide it via:\n"
            "  1. CYBERWAVE_API_TOKEN environment variable\n"
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

        logger.info(f"Starting Cloud Node '{args.slug}'")
        logger.info(f"Profile: {config.profile_slug}")
        logger.info(f"MQTT Broker: {config.mqtt_host}:{config.mqtt_port}")
        if config.inference:
            logger.info(f"Inference command: {config.inference}")
        if config.training:
            logger.info(f"Training command: {config.training}")

        # Create and run the node
        node = CloudNode(
            slug=args.slug,
            config=config,
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
