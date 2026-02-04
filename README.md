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
- Processing inference and training requests as independent OS processes
- Monitoring workload processes and collecting results when they complete
- Graceful shutdown without interrupting running workloads:
  - Rejects new commands during shutdown
  - Workloads continue running as independent processes
  - Flushes logs and notifies backend before terminating

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
- `status` - Get node status (includes active workloads)
- `cancel` - Cancel a running workload by PID or request_id

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

### Cancelling Workloads

To cancel a running workload, send a `cancel` command:

**Cancel by PID:**
```json
{
  "command": "cancel",
  "request_id": "unique-request-id",
  "params": {
    "pid": 12345
  }
}
```

**Cancel by workload request_id:**
```json
{
  "command": "cancel",
  "request_id": "unique-request-id",
  "params": {
    "workload_request_id": "original-training-request-id"
  }
}
```

**Cancel with specific signal:**
```json
{
  "command": "cancel",
  "request_id": "unique-request-id",
  "params": {
    "pid": 12345,
    "signal": "SIGKILL"
  }
}
```

Supported signals:
- `SIGTERM` (default) - Graceful termination, allows cleanup
- `SIGINT` - Interrupt signal (like Ctrl+C)
- `SIGKILL` - Force kill, immediate termination

**Cancel Response:**
```json
{
  "status": "ok",
  "request_id": "unique-request-id",
  "slug": "my-gpu-node",
  "instance_uuid": "abc-123",
  "output": {
    "message": "Workload cancelled with SIGTERM",
    "pid": 12345,
    "workload_type": "training",
    "workload_request_id": "original-training-request-id",
    "signal": "SIGTERM"
  }
}
```

## Process-Based Workload Management

Training and inference jobs run as **independent OS processes** that survive Cloud Node restarts. This design ensures:

### How It Works

1. **Command Received**: When a training/inference command arrives via MQTT
   - Cloud Node spawns a detached subprocess (using `start_new_session=True`)
   - Process runs independently with its own PID
   - Output streams to log files in `~/.cyberwave/workload_logs/`

2. **Background Monitoring**: The workload monitor loop:
   - Checks every 5 seconds if workload processes are still alive
   - Collects results when processes complete
   - Publishes completion status and output back via MQTT

3. **Node Capacity & Status**: 
   - Cloud Node tracks active workloads by PID
   - Rejects new workloads when busy (configurable for concurrent workloads)
   - Status command returns:
     ```json
     {
       "slug": "my-gpu-node",
       "instance_uuid": "abc-123",
       "is_busy": true,
       "active_workloads": [
         {
           "pid": 12345,
           "type": "training",
           "request_id": "abc-123",
           "running_for_seconds": 3600
         }
       ]
     }
     ```

4. **Workload Cancellation**:
   - Backend can cancel workloads by PID or request_id
   - Supports graceful (SIGTERM) or force (SIGKILL) termination
   - Sends cancellation notification to original workload request

5. **Graceful Shutdown**: When Cloud Node shuts down (Ctrl+C, SIGTERM, SIGINT):
   - ✅ Stops accepting new commands
   - ✅ Cancels background monitoring tasks
   - ✅ Logs running workload PIDs and output file locations
   - ✅ Workloads **continue running** in background
   - ✅ Results will be available in log files

### Benefits

- **Resilience**: Cloud Node can restart/upgrade without killing training jobs
- **Fast Shutdown**: No waiting - shutdown completes immediately
- **Process Independence**: Training jobs aren't tied to Cloud Node lifecycle
- **Crash Recovery**: Workloads survive Cloud Node crashes
- **Simple Monitoring**: Just check PID to see if workload is alive

### Output Files

Workload output is streamed to:
```
~/.cyberwave/workload_logs/
├── training_<request_id>.stdout.log
├── training_<request_id>.stderr.log
├── inference_<request_id>.stdout.log
└── inference_<request_id>.stderr.log
```

These files are also streamed to the backend in real-time for live monitoring.

## TODO

- Try it on a real repo - I cloned https://github.com/cyberwave-os/openvla-oft into this workspace
- Add a Nuitka github action to build it as a binary
