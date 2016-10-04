#!/bin/sh

# You need to source the settings.sh file (source /path/to/GE/default/common/settings.sh)
# of your Grid Engine installation before building. "default" is the CELL name, which can
# be different in your installation.

if [ "$SGE_ROOT" = "" ]; then
    echo "source your Grid Engine settings.(c)sh file"
    exit 1
fi

ARCH=`$SGE_ROOT/util/arch`

#if [ "$1" = "--torque" ]; then
#    export CGO_CFLAGS='-DTORQUE -I/usr/include/torque'
#else
#    export CGO_LDFLAGS="-L$SGE_ROOT/lib/lx-amd64/"
#    export CGO_CFLAGS="-I$SGE_ROOT/include"
#fi

if [ "$1" = "--torque" ]; then
    export CGO_CFLAGS='-DTORQUE -I/usr/include/torque'
else
    export CGO_LDFLAGS="-L$SGE_ROOT/lib/$ARCH/"
    export CGO_CFLAGS="-I$SGE_ROOT/include"
fi

go build -a
go install
