#!/bin/sh

# You need to source the settings.sh file (source /path/to/GE/default/common/settings.sh)
# of your Grid Engine installation before building. "default" is the CELL name, which can
# be different in your installation.

if [ "$1" = "--torque" ]; then
    export CGO_CFLAGS='-DTORQUE -I/usr/include/torque'
elif [ "$1" = "--slurm" ]; then
    # expect drmaa is installed in /usr/local - also set export LD_LIBRARY_PATH=/usr/local/lib when
    # running the example
    export CGO_CFLAGS='-DSLURM -I/usr/local/include'
    export CGO_LDFLAGS='-L/usr/local/lib'
else
    export ARCH=`$SGE_ROOT/util/arch`
    export CGO_LDFLAGS="-L$SGE_ROOT/lib/$ARCH/"
    export CGO_CFLAGS="-I$SGE_ROOT/include"
fi

go build -a 
