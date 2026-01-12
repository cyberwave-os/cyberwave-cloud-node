#!/bin/bash
# Build script for creating standalone Cyberwave Cloud Node binary

set -e

echo "=== Building Cyberwave Cloud Node ==="

# Ensure we're in the project directory
cd "$(dirname "$0")"

# Check if pyinstaller is installed
if ! command -v pyinstaller &> /dev/null; then
    echo "Installing build dependencies..."
    pip install -e ".[build]"
fi

# Clean previous builds
rm -rf build dist *.spec __pyinstaller_entry.py

# Create a wrapper entry point that uses absolute imports
cat > __pyinstaller_entry.py << 'EOF'
"""PyInstaller entry point for Cyberwave Cloud Node."""
from cyberwave_cloud_node.cli import main

if __name__ == "__main__":
    main()
EOF

# Build standalone binary
echo "Building standalone binary..."
pyinstaller \
    --onefile \
    --name cyberwave-cloud-node \
    --hidden-import cyberwave_cloud_node \
    --hidden-import cyberwave_cloud_node.cli \
    --hidden-import cyberwave_cloud_node.client \
    --hidden-import cyberwave_cloud_node.cloud_node \
    --hidden-import cyberwave_cloud_node.config \
    --hidden-import cyberwave_cloud_node.credentials \
    --hidden-import httpx \
    --hidden-import yaml \
    --hidden-import dotenv \
    --hidden-import cyberwave \
    __pyinstaller_entry.py

# Clean up the temporary entry point
rm -f __pyinstaller_entry.py

echo ""
echo "=== Build complete ==="
echo "Binary: dist/cyberwave-cloud-node"
echo ""
echo "Test with: ./dist/cyberwave-cloud-node --help"
