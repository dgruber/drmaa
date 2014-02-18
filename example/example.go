/*
    Copyright 2013 Daniel Gruber

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

    This is a tiny example how to use DRMAA for job submission and
    gestatus for getting detailed job status.

    In order to use the program you need to usually the LD_LIBRARY_PATH
    before starting (e.g. export LD_LIBRARY_PATH=$SGE_ROOT/lib/lx-amd64)
    so that the DRMAA native library of the resource manager can be found.
*/

package main

import (
	"github.com/dgruber/drmaa"
	"github.com/dgruber/drmaa/gestatus"
	"fmt"
	"time"
)

func main() {

   /* create a new DRMAA1 session */
	s, _ := drmaa.MakeSession()
	defer s.Exit()

   /* submit the sleep 3600 command to the cluster
      by using DRMAA */
	jt, _ := s.AllocateJobTemplate()

	jt.SetRemoteCommand("sleep")
	jt.SetArg("3600")

	jobId, _ := s.RunJob(&jt)

	d, _ := time.ParseDuration("500ms")

	/* wait activly until job is running (use blocking call in real apps) */
	ps, _ := s.JobPs(jobId)

	for ps != drmaa.PsRunning {
		fmt.Println("status is: ", ps)
		time.Sleep(d)
		ps, _ = s.JobPs(jobId)
	}

   /* get detailed job status (Grid Engine specific) */
	jobStatus, err := gestatus.GetJobStatus(&s, jobId)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Job Name: %s\n", jobStatus.JobName())
	fmt.Printf("Job Number: %d\n", jobStatus.JobId())
	fmt.Printf("Job Script: %s\n", jobStatus.JobScript())
	fmt.Printf("Job Args: %s\n", jobStatus.JobArgs())
	fmt.Printf("Job Owner: %s\n", jobStatus.JobOwner())
	fmt.Printf("Job Group: %s\n", jobStatus.JobGroup())
	fmt.Printf("Job UID: %d\n", jobStatus.JobUID())
	fmt.Printf("Job GID: %d\n", jobStatus.JobGID())
	fmt.Printf("Job accounting string: %s\n", jobStatus.JobAccountName())
	fmt.Printf("Job is now: %t\n", jobStatus.IsImmediateJob())
	fmt.Printf("Job is binary: %t\n", jobStatus.IsBinaryJob())
	fmt.Printf("Job has reservation: %t\n", jobStatus.HasReservation())
	fmt.Printf("Job is array job: %t\n", jobStatus.IsArrayJob())
	fmt.Printf("Job merges stderr %t\n", jobStatus.JobMergesStderr())
	fmt.Printf("Job has 'no shell' requested: %t\n", jobStatus.HasNoShell())
	fmt.Printf("Job has memory binding: %t\n", jobStatus.HasMemoryBinding())
	fmt.Printf("Job memory binding: %s\n", jobStatus.MemoryBinding())
	fmt.Printf("Job submission time: %s\n", jobStatus.SubmissionTime())
	fmt.Printf("Job start time: %s\n", jobStatus.StartTime())
	fmt.Printf("Job deadline: %s\n", jobStatus.JobDeadline())
	fmt.Printf("Job mail options: %s\n", jobStatus.MailOptions())
	fmt.Printf("Job AR: %d\n", jobStatus.AdvanceReservationID())
	fmt.Printf("Job POSIX priority: %d\n", jobStatus.PosixPriority())
	fmt.Printf("Job Class Name: %s\n", jobStatus.JobClassName())
	fmt.Printf("Job Mailing Adresses: %s\n", jobStatus.MailAdresses())
	fmt.Printf("Job Destination Queue Instance List: %s\n", jobStatus.DestinationQueueInstanceList())
	fmt.Printf("Job Destination Host List: %s\n", jobStatus.DestinationHostList())
	fmt.Printf("Job Tasks: %d\n", jobStatus.TasksCount())
}
