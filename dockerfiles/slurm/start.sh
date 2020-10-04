#!/bin/bash

echo ""
echo "For go drmaa:"
echo "cd /root/go/src/github.com/dgruber/drmaa/examples/simplesubmit"
echo "./build.sh --slurm"
echo "export LD_LIBRARY_PATH=/usr/local/lib"
echo "./simplesubmit"
echo ""
echo ""
echo "Starting container"

docker run --rm -it -h ernie slurm-drmaa-dev
