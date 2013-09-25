# go-drmaa #
========

DRMAA job submission library for Go (#golang). Tested with Univa Grid Engine
and Torque, but it should work also other resource manager / cluster scheduler. 

The "gestatus" library only works with Grid Engine (some values only available
on Univa Grid Engine).

## Example ##

First download the package:

~~~
   export GOPATH=${GOPATH:-~/src/go}
   mkdir -p $GOPATH
   go get -d github.com/dgruber/drmaa
   cd $GOPATH/github.com/dgruber/drmaa
~~~

Next, we need to compile the code.

For Univa Grid Engine:

~~~
   source /path/to/grid/engine/installation/default/settings.sh
   ./build.sh
   cd example
   go build
   ./example
~~~

For Torque:

~~~
   ./build.sh --torque
   cd example
   go build
   ./example
~~~

If your Torque drmaa.h header file is not located under /usr/include/torque,
you will have to modify the build.sh script before running it.

The example program submits a sleep job into the system and prints out detailed
job information as soon as the job is started.

More examples can be found at http://www.gridengine.eu.
