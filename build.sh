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
