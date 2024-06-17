# Open Cluster Scheduler Go DRMAA Development Container

The scripts in this directory sets up a 1 node Open Cluster Scheduler
cluster, which allows you to quickly build and test Go DRMAA applications
without the hassle of manually creating a build environment.

## Issues

Warning: Currently there is a build issue with hwloc which is not yet fully solved.

## Building and Starting

Building an Ubuntu 22.04 based container with all dependencies:

    make build

Running a single node cluster based on the container.

    make run

After successfully executing _make run_ you should see command line output
similar than that:

```go
Install log can be found in: /opt/cs-install/default/common/install_logs/qmaster_install_master_2024-06-17_12:09:35.log
Install log can be found in: /opt/cs-install/default/common/install_logs/execd_install_master_2024-06-17_12:09:50.log
root@master modified "global" in configuration list
root@master modified "all.q" in cluster queue list
```
