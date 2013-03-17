#!/bin/sh

# You need to source the settings.sh file (source /path/to/GE/default/common/settings.sh)
# of your Grid Engine installation before building. "default" is the CELL name, which can
# be different in the 

export CGO_LDFLAGS="-L$SGE_ROOT/lib/lx-amd64/"
export CGO_CFLAGS="-I$SGE_ROOT/include"

go install 

