/*
    Copyright 2013, 2015 Daniel Gruber, info@gridengine.eu

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

      * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
      * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    This is a tiny example how to use DRMAA for job submission and
    gestatus for getting detailed job status.

    In order to use the program you need to usually the LD_LIBRARY_PATH
    before starting (e.g. export LD_LIBRARY_PATH=$SGE_ROOT/lib/lx-amd64)
    so that the DRMAA native library of the resource manager can be found.
*/

package main

import (
	"fmt"
	"github.com/dgruber/drmaa"
	"os"
	"time"
)

func main() {
	/* create a new DRMAA1 session */
	s, errMS := drmaa.MakeSession()
	if errMS != nil {
		fmt.Printf("Error during DRMAA session creation: %s\n", errMS)
		os.Exit(1)
	}
	defer s.Exit()

	/* submit the sleep 3600 command to the cluster by using DRMAA */
	jt, errJT := s.AllocateJobTemplate()
	if errJT != nil {
		fmt.Printf("Error during allocating a new job template: %s\n", errJT)
		return
	}
	// prevent memory leaks by freeing the allocated C job template */
	defer s.DeleteJobTemplate(&jt)

	// set the application to submit
	jt.SetRemoteCommand("sleep")
	jt.SetArg("10")

	jobID, errRun := s.RunJob(&jt)
	if errRun != nil {
		fmt.Printf("Error during job submission: %s\n", errRun)
		return
	}

	/* wait activly until job is running (use blocking call in real apps) */
	ps, errPS := s.JobPs(jobID)
	if errPS != nil {
		fmt.Printf("Error during job status query: %s\n", errPS)
		return
	}

	for ps != drmaa.PsRunning && errPS == nil {
		fmt.Println("status is: ", ps)
		time.Sleep(time.Millisecond * 500)
		ps, errPS = s.JobPs(jobID)
	}

	// wait until the job is finished
	jinfo, errWait := s.Wait(jobID, drmaa.TimeoutWaitForever)
	if errWait != nil {
		fmt.Printf("Error during waiting until job %s is finished: %s\n", jobID, errWait)
		return
	}

	fmt.Printf("Job exited with exit code: %d\n", jinfo.ExitStatus())
	fmt.Printf("Resource consumption by the job: %v\n", jinfo.ResourceUsage())
}
