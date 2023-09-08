#!/usr/bin/env bash

# Should provide the directory where this script lives in most cases.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# The name of our image.
# In most cases this is the path to the image in the container registry.
IMAGE_NAME="raritan"

# Build the image.
docker image build -t $IMAGE_NAME .

# Run the container in a disposable manner.
# Add a volume to the current working dir.
docker run --rm -it -v $SCRIPT_DIR:/workspace $IMAGE_NAME bash
