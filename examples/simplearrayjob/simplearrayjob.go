/*
    Copyright 2015 Daniel Gruber, info@gridengine.eu

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

   In order to use the program you need to usually the LD_LIBRARY_PATH
   before starting (e.g. export LD_LIBRARY_PATH=$SGE_ROOT/lib/lx-amd64)
   so that the DRMAA native library of the resource manager can be found.
*/

package main

import (
	"fmt"
	"github.com/dgruber/drmaa"
	"os"
)

func main() {
	/* create a new DRMAA1 session */
	s, errMS := drmaa.MakeSession()
	if errMS != nil {
		fmt.Printf("Error during DRMAA session creation: %s\n", errMS)
		os.Exit(1)
	}
	defer s.Exit()

	jt, errJT := s.AllocateJobTemplate()
	if errJT != nil {
		fmt.Printf("Error during allocating a new job template: %s\n", errJT)
		return
	}
	// prevent memory leaks by freeing the allocated C job template */
	defer s.DeleteJobTemplate(&jt)

	// set the application to submit
	jt.SetRemoteCommand("sleep")
	jt.SetArg("1")

	// submit 50 instances of the sleep binary (which sleeps
	// always 1 second) as array job tasks: 1, 3, 5, 7, 9, 11, ..., 101
	jobIDs, errRun := s.RunBulkJobs(&jt, 1, 101, 2)
	if errRun != nil {
		fmt.Printf("Error during job submission: %s\n", errRun)
		return
	}

	// wait until all tasks are finished - reap the job information
	fmt.Println("Waiting until all tasks are finished.")
	errSync := s.Synchronize(jobIDs, drmaa.TimeoutWaitForever, true)
	if errSync != nil {
		fmt.Printf("Error while job synchronization: %s", errSync)
	}
}
