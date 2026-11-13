#!/usr/bin/env bash

# Generates the protobuf and gRPC classes that ship in the Python SDK
# (sdk/python). The generator version is pinned so that the output is
# reproducible: CI regenerates the classes and fails on any diff.
#
# Install the generator with: pip install -e 'sdk/python[dev]'

set -eo pipefail

# Paths below are relative to the repository root.
cd "$(dirname "$0")/.."

OUT=sdk/python/src
PYTHON=${PYTHON:-python}
GRPCIO_TOOLS_VERSION=1.71.0

installed=$("$PYTHON" -c 'from importlib.metadata import version; print(version("grpcio-tools"))' 2>/dev/null || true)
if [ "$installed" != "$GRPCIO_TOOLS_VERSION" ]; then
    echo "protogen-py: grpcio-tools==$GRPCIO_TOOLS_VERSION is required, found '${installed:-none}'." >&2
    echo "protogen-py: run: pip install -e 'sdk/python[dev]'" >&2
    exit 1
fi

# Remove the previous output first, so a deleted .proto file leaves no class behind.
rm -rf "$OUT/coinbase"

"$PYTHON" -m grpc_tools.protoc \
    --python_out="$OUT" \
    --grpc_python_out="$OUT" \
    --proto_path=protos \
    protos/coinbase/chainstorage/*.proto \
    protos/coinbase/c3/common/*.proto \
    protos/coinbase/crypto/rosetta/types/*.proto
