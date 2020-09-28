#!/bin/sh

# You need to source the settings.sh file (source /path/to/GE/default/common/settings.sh)
# of your Grid Engine installation before building. "default" is the CELL name, which can
# be different in your installation.

if [ "$1" = "--drmaa" ]; then
    if [ -z "$2" ]
    then
	echo "usage: $0 --drmaa <drmaa root>"
	exit 1
    else
	if [ -d $2 ]
	then 
	    export CGO_LDFLAGS="-L$2/lib"
	    export CGO_CFLAGS="-I$2/include"
	else
	    echo "$2 does not exist or is not a directory."
	    exit 1
	fi
    fi
elif [ "$1" = "--torque" ]; then
    export CGO_CFLAGS='-DTORQUE -I/usr/include/torque'
elif [ "$1" = "--slurm" ]; then
    export CGO_CFLAGS='-DSLURM'
    if [ -n "$2" ]; then
	SLURM_DRMAA_ROOT=$2
    fi
    if [ -n "$SLURM_DRMAA_ROOT" ]; then
	export CGO_LDFLAGS="-L$SLURM_DRMAA_ROOT/lib"
	export CGO_CFLAGS="-DSLURM -I$SLURM_DRMAA_ROOT/include"
    fi
    export LD_LIBRARY_PATH="$SLURM_DRMAA_ROOT/lib"
else
    if [ "$SGE_ROOT" = "" ]; then
        echo "source your Grid Engine settings.(c)sh file"
        exit 1
    fi

    ARCH=`$SGE_ROOT/util/arch`

    export CGO_LDFLAGS="-L$SGE_ROOT/lib/$ARCH/"
    export CGO_CFLAGS="-I$SGE_ROOT/include"

    # adapt to Son of Grid Engine's header file changes
    if [ "$1" = "--sog" ]; then
        echo "SOG"
        export CGO_CFLAGS="-DSOG -I$SGE_ROOT/include"
    fi
fi

go test -v
go build -a
go install
