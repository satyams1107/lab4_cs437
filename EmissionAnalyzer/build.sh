#!/bin/bash
set -euo pipefail

# Determine component version from gdk-config.json
version=$(python3 - <<'PY'
import json
with open('gdk-config.json') as f:
    cfg = json.load(f)
v = cfg['component']['com.iotea.EmissionAnalyzer']['version']
print(v)
PY
)

out_dir="greengrass-build/artifacts/com.iotea.EmissionAnalyzer/${version}"
mkdir -p "$out_dir"

# Create the src.zip archive using Python (keeps directory structure)
python3 - <<'PY'
import zipfile, os, json
with open('gdk-config.json') as f:
    cfg = json.load(f)
version = cfg['component']['com.iotea.EmissionAnalyzer']['version']
out_dir = f"greengrass-build/artifacts/com.iotea.EmissionAnalyzer/{version}"
output_path = out_dir + "/src.zip"

with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    # Add src directory
    for root, dirs, files in os.walk('src'):
        for file in files:
            file_path = os.path.join(root, file)
            arcname = file_path
            zipf.write(file_path, arcname)

    # Add pubsub_message_handlers directory (if it exists)
    if os.path.isdir('pubsub_message_handlers'):
        for root, dirs, files in os.walk('pubsub_message_handlers'):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = file_path
                zipf.write(file_path, arcname)

print('Build completed:', output_path)
PY
