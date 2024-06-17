#!/bin/bash

# Start or install Open Cluster Scheduler
cd /opt/helpers
./installer.sh

source /opt/cs-install/default/common/settings.sh

exec "$@"
