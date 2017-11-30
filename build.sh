#!/bin/sh

# You need to source the settings.sh file (source /path/to/GE/default/common/settings.sh)
# of your Grid Engine installation before building. "default" is the CELL name, which can
# be different in your installation.

if [ "$1" = "--torque" ]; then
    export CGO_CFLAGS='-DTORQUE -I/usr/include/torque'
elif [ "$1" = "--slurm" ]; then
    export CGO_CFLAGS='-DSLURM'
    if [ -n "$2" ]; then
	SLURM_RDMAA_ROOT=$2
    fi
    if [ -n "$SLURM_DRMAA_ROOT" ]; then
	export CGO_LDFLAGS="-L$SLURM_DRMAA_ROOT/lib"
	export CGO_CFLAGS="-DSLURM -I$SLURM_DRMAA_ROOT/include"
    fi
else
    if [ "$SGE_ROOT" = "" ]; then
        echo "source your Grid Engine settings.(c)sh file"
        exit 1
    fi

    ARCH=`$SGE_ROOT/util/arch`

    export CGO_LDFLAGS="-L$SGE_ROOT/lib/$ARCH/"
    export CGO_CFLAGS="-I$SGE_ROOT/include"
fi

go build -a
go install
