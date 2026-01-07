# Cyberwave Cloud Node

Turn any computer into a Cloude Node instance you can use to run inference or training with Cyberwave.
This subfolder contains the documentation of the package that helps developers create Cloud Node (GPU) instances that are cyberwave-ready in a few lines of code.

## Quickstart

Let's say you have a codebase that runs training and inference. You want that to be part of a Cyberwave pipeline.

First, you install the package

```bash
pip install cyberwave-cloud-node

# or if you want the compiled version

sudo apt get install cyberwave-cloud-node
```

Then to the root of your repository you add a yaml file like this

```yaml
# cyberwave.yml
cyberwave-cloud-node: # all of these should be bash commands
  install_script: ./install.sh # install what you need in the cloud GPU
  inference: python ./inference.py --params {body} # put what you need. {body} will render to the stringified JSON params from the HTTP request. see later for examples
  training: python ./training.py --params {body}
```

Behind the scenes, Cyberwave Cloude Node will take care of:

- Installing on startup and notifying you if it fails
- Signaling to Cyberwave when the installation is complete, so you will see your GPU available in your workspace
- Sending an heartbeat signal
- Processing inference and training requests, updating your Cyberwave GPU requests

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

- `CYBERWAVE_API_TOKEN`: Your Cyberwave API token
- `CYBERWAVE_WORKSPACE_SLUG` (optional): Your workspace slug
- `CYBERWAVE_API_URL` (optional): API URL (default: https://api.cyberwave.com)
- `CYBERWAVE_ENDPOINT` (optional): Override the auto-detected endpoint URL

## CLI Usage

```bash
# Start the cloud node
export CYBERWAVE_API_TOKEN=your-token-here
cyberwave-cloud-node start --slug my-gpu-node

# With custom config file
cyberwave-cloud-node start --slug my-gpu-node --config ./path/to/cyberwave.yml
```

## Programmatic Usage

```python
from cyberwave_cloud_node import CloudNode, CloudNodeConfig

# From config file
node = CloudNode.from_config_file(slug="my-gpu-node")
node.run()

# Or programmatically
config = CloudNodeConfig(
    install_script="./install.sh",
    inference="python inference.py --params {body}",
    training="python train.py --params {body}",
    profile_slug="gpu-a100",
)
node = CloudNode(slug="my-gpu-node", config=config)
node.run()
```

## TODO

- Try it on a real repo - I cloned https://github.com/cyberwave-os/openvla-oft into this workspace
- Add a Nuitka github action to build it as a binary
