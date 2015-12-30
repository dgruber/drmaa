go-drmaa
========
[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/dgruber/drmaa)
[![Apache V2 License](http://img.shields.io/badge/license-BSD-blue.svg)](https://github.com/dgruber/drmaa/blob/master/COPYING)

This is a job submission library for Go (#golang) which is compatible to 
the [DRMAA](http://drmaa.org) standard. The Go library is a wrapper around
the [DRMAA C library implementation](https://www.ogf.org/documents/GFD.22.pdf) provided
by many distributed resource managers (cluster schedulers).

It is developed by using [Univa Grid Engine](http://www.univa.com). The library
was tested with Grid Engine and Torque, but it should work also other resource manager / cluster scheduler.

The "gestatus" subpackage only works with Grid Engine (some values are only available
on Univa Grid Engine).

The DRMAA (Distributed Resource Management Application API) standard is meanwhile
available in version 2. DRMAA2 provides more functionalities around cluster monitoring
and job session management. DRMAA and DRMAA2 are not compatible hence it is expected
that both libraries are co-existing for a while. The Go DRMAA2 can be found [here](https://github.com/dgruber/drmaa2).

Note: Univa Grid Engine 8.3.0 and later added new functions which allows you 
to submit a job on behalf of another user. This helps creating a DRMAA service
(like a web portal) which submits jobs. This functionality is available in the
*UGE83\_sudo* branch: https://github.com/dgruber/drmaa/tree/UGE83_sudo
The functions are: RunJobsAs(), RunBulkJobsAs(), and ControlAs()

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
   cd examples/simplesubmit
   go build
   ./simplesubmit
~~~

For Torque:

~~~
   ./build.sh --torque
   cd examples/simplesubmit
   go build
   ./simplesubmit
~~~

If your Torque drmaa.h header file is not located under /usr/include/torque,
you will have to modify the build.sh script before running it.

The example program submits a sleep job into the system and prints out detailed
job information as soon as the job is started.

More examples can be found at http://www.gridengine.eu.
