go-drmaa
========
[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/dgruber/drmaa)
[![Apache V2 License](http://img.shields.io/badge/license-BSD-blue.svg)](https://github.com/dgruber/drmaa/blob/master/COPYING)
[![Go Report Card](http://goreportcard.com/badge/dgruber/drmaa)](http://goreportcard.com/report/dgruber/drmaa)

This is a job submission library for Go (#golang) which is compatible to 
the [DRMAA](http://drmaa.org) standard. The Go library is a wrapper around
the [DRMAA C library implementation](https://www.ogf.org/documents/GFD.22.pdf) provided
by many distributed resource managers (cluster schedulers).

The library was developed using [Univa Grid Engine's](http://www.univa.com) libdrmaa.so. It
was tested with Grid Engine, Torque, and [SLURM](https://apps.man.poznan.pl/trac/slurm-drmaa), but it should work also other resource managers / cluster schedulers which provide _libdrmaa.so_.

The "gestatus" subpackage only works with Grid Engine (some values are only available
on Univa Grid Engine).

The DRMAA (Distributed Resource Management Application API) standard is meanwhile
available in version 2. DRMAA2 provides more functionality around cluster monitoring
and job session management. DRMAA and DRMAA2 are not compatible hence it is expected
that both libraries are co-existing for a while. The Go DRMAA2 can be found [here](https://github.com/dgruber/drmaa2).

Note: Univa Grid Engine 8.3.0 and later added new functions that allows you 
to submit a job on behalf of another user. This helps creating a DRMAA service
(like a web portal) that submits jobs. This functionality is available in the
*UGE83\_sudo* branch: https://github.com/dgruber/drmaa/tree/UGE83_sudo
The functions are: RunJobsAs(), RunBulkJobsAs(), and ControlAs()

## Compilation ##

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
   export LD_LIBRARY_PATH=$SGE_ROOT/lib/lx-amd64
   ./simplesubmit
~~~

For [Torque](https://github.com/adaptivecomputing/torque/tree/master/src/drmaa):

If your Torque drmaa.h header file is not located under /usr/include/torque,
you will have to modify the build.sh script before running it.

~~~
   ./build.sh --torque
   cd examples/simplesubmit
   go build
   ./simplesubmit
~~~

For [SLURM](https://apps.man.poznan.pl/trac/slurm-drmaa):

~~~
   ./build.sh --slurm
   cd examples/simplesubmit
   go build
   ./simplesubmit
~~~



The example program submits a sleep job into the system and prints out detailed
job information as soon as the job is started.

## Short Introduction in Go DRMAA ##

Go DRMAA applications need to open a DRMAA session before the DRMAA calls
can be executed. Opening a DRMAA session usually establishes a connection
to the cluster scheduler (distributed resource manager). Hence if no more
DRMAA calls are made the Exit() method of the session must be executed.
This tears down the connection. When an application does not call the Exit()
method this can leave a communication handle open on the cluster scheduler
side (which can take a while to be removed automatically). It should
be always avoided not to call Exit(). In Go the **defer** statement can be
used but remember that the function is not executed when an *os.Exit()* call 
is made.

Creating a DRMAA session:

    s, err := drmaa.MakeSession()

Usually jobs and job workflows are submitted within DRMAA applications.
In order to submit a job first a job template needs to be allocated:

    jt, errJT := s.AllocateJobTemplate()
    if errJT != nil {
       fmt.Printf("Error during allocating a new job template: %s\n", errJT)
       return
    }

Underneath a C job template is allocated which is out-of-scope of the
Go system. Hence it must be ensured that the job template is deleted
when it is not used anymore. Also here the Go **defer** statement is useful.

    // prevent memory leaks by freeing the allocated C job template at the end
    defer s.DeleteJobTemplate(&jt)

The job template contains the specification of the job, like the command
to be executed and its parameters. Those can be set by the setter methods
of the job.

    // set the application to submit
    jt.SetRemoteCommand("sleep")
    // set the parameter (use SetArgs() when having more parameters)
    jt.SetArg("1")

A job can be executed with the session **RunJob()** method. If the same
command should be executed many times, running it as a job array 
would make sense. In Grid Engine each instance gets a task ID assigned
which the job can see in the SGE_TASK_ID environment variable (which 
is set to **unknown** for normal jobs). This task ID can be used for 
finding the right data set the job (array job task) needs to process.
Submitting an array job is done with the **RunBulkJobs()** method.


    jobID, errSubmit := s.RunJob(&jt)

    // submitting 1000 instances of the same job
    jobIDs, errBulkSubmit := s.RunBulkJobs(&jt, 1, 1000, 1)

A job state can also be changed (suspended / resumed / put in hold / deleted):

    errTerm := s.TerminateJob(jobID)

The JobInfo data structure contains the runtime information of the 
job, like exit status or the amount of used resources (memory / IO / etc.).
The JobInfo data structure can be get with the **Wait()** method.

    jinfo, errWait := s.Wait(jobID, drmaa.TimeoutWaitForever)

For more details please consult the documentation and the DRMAA 
standard [specifications](https://www.ogf.org/ogf/doku.php/standards/standards).

More examples can be found on my blog at http://www.gridengine.eu.
