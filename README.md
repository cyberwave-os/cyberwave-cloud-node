# Cyberwave Cloud Node

Turn any computer into a Cloud Node instance you can use to run inference or training with Cyberwave.
This subfolder contains the documentation of the package that helps developers create Cloud Node (GPU) instances that are cyberwave-ready in a few lines of code.

## How it Works

The Cloud Node connects to the Cyberwave MQTT broker to receive commands. This means:

- **No public URL required** - The node connects outbound to MQTT, no need to expose ports
- **Firewall friendly** - Works behind NAT and firewalls
- **Real-time communication** - Commands are received instantly via MQTT subscription

## Quickstart

Let's say you have a codebase that runs training and inference. You want that to be part of a Cyberwave pipeline.

First, you install the package

```bash
pip install cyberwave-cloud-node

# or if you want the compiled version

sudo apt-get install cyberwave-cloud-node
```

Then to the root of your repository you add a yaml file like this

```yaml
# cyberwave.yml
cyberwave-cloud-node:
  install_script: ./install.sh          # install what you need in the cloud GPU
  inference: python ./inference.py --params {body}  # {body} renders to JSON params from MQTT
  training: python ./training.py --params {body}
  profile_slug: gpu-a100                # optional: node profile (default: "default")
  heartbeat_interval: 30                # optional: heartbeat interval in seconds
  mqtt_host: mqtt.cyberwave.com         # optional: custom MQTT broker
  mqtt_port: 1883                       # optional: custom MQTT port
```

Behind the scenes, Cyberwave Cloud Node will take care of:

- Running the install script on startup and notifying you if it fails
- Registering with the Cyberwave backend to get a UUID and slug
- Storing identity locally for re-registration support
- Connecting to MQTT to receive commands
- Sending periodic heartbeats
- Processing inference and training requests, publishing results back via MQTT
- Graceful shutdown with termination notification

## Authentication

The Cloud Node needs an API token to communicate with Cyberwave. You can provide it in several ways (in order of priority):

1. **Environment variable**: `export CYBERWAVE_API_TOKEN=your-token`
2. **`.env` file** in current directory:
   ```bash
   # .env
   CYBERWAVE_API_TOKEN=your-token
   CYBERWAVE_WORKSPACE_SLUG=my-workspace
   ```
3. **`.env` file** in `~/.cyberwave/.env` (shared config)
4. **Stored credentials** from `cyberwave-cli` login (`~/.cyberwave/credentials.json`)

If you've already logged in with `cyberwave-cli`, the Cloud Node will automatically use those credentials.

## Environment Variables

### Required

- `CYBERWAVE_API_TOKEN`: Your Cyberwave API token

### Optional - API & Workspace

- `CYBERWAVE_WORKSPACE_SLUG`: Your workspace slug
- `CYBERWAVE_INSTANCE_SLUG`: Instance slug hint (useful for automated deployments)
- `CYBERWAVE_API_URL`: API URL (default: https://api.cyberwave.com)

### Optional - MQTT

- `CYBERWAVE_MQTT_HOST`: MQTT broker host (default: mqtt.cyberwave.com)
- `CYBERWAVE_MQTT_PORT`: MQTT broker port (default: 1883)
- `CYBERWAVE_MQTT_USERNAME`: MQTT username if required
- `CYBERWAVE_MQTT_PASSWORD`: MQTT password if required
- `CYBERWAVE_ENVIRONMENT`: Environment prefix for MQTT topics (empty for production)

### Optional - Commands (alternative to cyberwave.yml)

- `CYBERWAVE_INSTALL_SCRIPT`: Install script command
- `CYBERWAVE_INFERENCE_CMD`: Inference command template
- `CYBERWAVE_TRAINING_CMD`: Training command template
- `CYBERWAVE_PROFILE_SLUG`: Node profile slug (default: "default")
- `CYBERWAVE_HEARTBEAT_INTERVAL`: Heartbeat interval in seconds (default: 30)

## CLI Usage

```bash
# Start the cloud node (backend assigns UUID and slug)
export CYBERWAVE_API_TOKEN=your-token-here
cyberwave-cloud-node start

# With a slug hint (backend may use this or assign a different one)
cyberwave-cloud-node start --slug my-gpu-node

# With custom config file
cyberwave-cloud-node start --config ./path/to/cyberwave.yml

# With profile override
cyberwave-cloud-node start --profile gpu-a100

# With custom MQTT broker
cyberwave-cloud-node start --mqtt-host localhost --mqtt-port 1883

# Verbose logging
cyberwave-cloud-node start -v
```

Note: The `--slug` parameter is a hint. The backend is the owner of UUIDs and slugs - it may use your hint or assign a different one.

## Programmatic Usage

```python
from cyberwave_cloud_node import CloudNode, CloudNodeConfig

# From config file (recommended)
node = CloudNode.from_config_file()
node.run()

# With a slug hint
node = CloudNode.from_config_file(slug="my-gpu-node")
node.run()

# From environment variables
node = CloudNode.from_env()
node.run()

# Or fully programmatic
config = CloudNodeConfig(
    install_script="./install.sh",
    inference="python inference.py --params {body}",
    training="python train.py --params {body}",
    profile_slug="gpu-a100",
    heartbeat_interval=30,
    mqtt_host="mqtt.cyberwave.com",
    mqtt_port=1883,
)
node = CloudNode(config=config, slug="my-gpu-node")
node.run()
```

## Instance Identity

After successful registration, the backend assigns a UUID and slug to your node. This identity is stored locally in `~/.cyberwave/instance_identity.json` for:

- **Re-registration**: If the node restarts, it will re-register with the same identity
- **Debugging**: You can inspect the file to see your node's assigned UUID and slug

## MQTT Topics

The cloud node subscribes to command topics and publishes responses:

**Command Topics** (subscribes to):

- `cyberwave/cloud-node/{instance_uuid}/command`
- `cyberwave/cloud-node/{slug}/command`

**Response Topic** (publishes to):

- `cyberwave/cloud-node/{instance_uuid}/response`

### Command Message Format

```json
{
  "command": "inference",
  "request_id": "unique-request-id",
  "params": {
    "model": "gpt-4",
    "input": "Hello world"
  }
}
```

Supported commands:

- `inference` - Run the inference command
- `training` - Run the training command
- `status` - Get node status

### Response Message Format

```json
{
  "status": "ok",
  "request_id": "unique-request-id",
  "slug": "my-gpu-node",
  "instance_uuid": "abc-123",
  "output": "Command output here"
}
```

On error:

```json
{
  "status": "error",
  "request_id": "unique-request-id",
  "slug": "my-gpu-node",
  "instance_uuid": "abc-123",
  "error": "Error message here"
}
```

## TODO

- Try it on a real repo - I cloned https://github.com/cyberwave-os/openvla-oft into this workspace
- Add a Nuitka github action to build it as a binary
