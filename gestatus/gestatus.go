/*
   Copyright 2013, 2016 Daniel Gruber, info@gridengine.eu

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


   This module contains Grid Engine specific code for getting much
   more detailed job information than plain DRMAA1 is able to show.
   This is done by parsing the qstat -xml output.
*/

package gestatus

import (
	"github.com/dgruber/drmaa"
	"github.com/dgruber/drmaa/gestatus/private_gestatus"
	"strconv"
	"strings"
	"time"
)

// ClusterJobs contains all jobs found in the cluster.
type ClusterJobs struct {
	jobs []geparser.QstatJob
}

// Job represents the state of a job and its properties.
type Job struct {
	Number    int64
	Priority  float64
	Name      string
	Owner     string
	State     string
	StartTime string
	QueueName string
	JobClass  string
	Slots     int64
}

func convertQstatJobToJob(qsj geparser.QstatJob) (job Job) {
	job.Number = qsj.JBjobnumber
	job.Priority = qsj.JATprio
	job.Name = qsj.JBname
	job.Owner = qsj.JBowner
	job.State = qsj.State
	qsj.JATstarttime = strings.Replace(qsj.JATstarttime, "T", "-", 1)
	job.StartTime = qsj.JATstarttime
	job.QueueName = qsj.QueueName
	job.JobClass = qsj.JClassName
	job.Slots = qsj.Slots
	return job
}

// GetClusterJobs performs internally a job status call (qstat)
// to the cluster to get more detailed information about the job
// status then what plain DRMAA offers.
func GetClusterJobs() (clusterjobs ClusterJobs, err error) {
	if cjs, err := geparser.GetClusterJobsStatus(); err == nil {
		if cjs.JobList != nil {
			clusterjobs.jobs = cjs.JobList
		}
		return clusterjobs, nil
	}
	return clusterjobs, err
}

// AllJobs returns a new slice of jobs found in the cluster by qstat.
func (cjs *ClusterJobs) AllJobs() []Job {
	if cjs.jobs == nil {
		return nil
	}
	jobs := make([]Job, 0, len(cjs.jobs))
	for _, j := range cjs.jobs {
		jobs = append(jobs, convertQstatJobToJob(j))
	}
	return jobs
}

// ----------------------------------------------------------------------------

// JobStatus represents fine detailed job status information. It offers
// more details than the Job struct. The information is collected by calling
// qstat -j when using Grid Engine.
type JobStatus struct {
	/* hide internal job status */
	js geparser.InternalJobStatus
}

// GetJob performs qstat -xml -j <jobid> and returns a JobStatus
// object for this job.
func GetJob(jobid string) (jobstat JobStatus, err error) {
	js, err := geparser.GetJobStatusByID(jobid)
	if err != nil {
		return jobstat, err
	}
	jobstat.js = js
	return jobstat, nil
}

// GetJobStatus returns the job status object, which contains all information
// about a job. In case of any error it is nil and a drmaa error
// is returned.
func GetJobStatus(session *drmaa.Session, jobID string) (jobstat JobStatus, err error) {
	js, err := geparser.GetJobStatus(session, jobID)
	if err != nil {
		return jobstat, err
	}
	jobstat.js = js
	return jobstat, nil
}

/* Exported access methods for the JobInfo struct */

// JobName returns the job name (given by -N submission option).
func (js *JobStatus) JobName() string {
	return geparser.GetJobName(&js.js)
}

// JobID returns the unique Grid Engine job ID.
func (js *JobStatus) JobID() int64 {
	return geparser.GetJobNumber(&js.js)
}

// execFileName is the GE internal script name which is executed on execd side
func (js *JobStatus) execFileName() string {
	return geparser.GetExecFileName(&js.js)
}

// JobScript returns the job script name as string.
func (js *JobStatus) JobScript() string {
	return geparser.GetScriptFile(&js.js)
}

// JobArgs returns the job arguments as string slice.
func (js *JobStatus) JobArgs() []string {
	return geparser.GetJobArgs(&js.js)
}

// JobOwner returns the owner of the job as string.
func (js *JobStatus) JobOwner() string {
	return geparser.GetOwner(&js.js)
}

// JobUID returns the ower of the job as Unix UID.
func (js *JobStatus) JobUID() int {
	return geparser.GetUID(&js.js)
}

// JobGroup returns the primary UNIX group of the job owner as string.
func (js *JobStatus) JobGroup() string {
	return geparser.GetGroup(&js.js)
}

// JobGID returns the primary UNIX group ID of the job owner as int.
func (js *JobStatus) JobGID() int {
	return geparser.GetGID(&js.js)
}

// JobAccountName returns the accounting string assigned to the job.
func (js *JobStatus) JobAccountName() string {
	return geparser.GetAccount(&js.js)
}

// IsImmediateJob returns true in case of an interactive job or a -now y batch job.
func (js *JobStatus) IsImmediateJob() bool {
	return geparser.IsImmediate(&js.js)
}

// HasReservation return true if the job requested a resource reservation.
func (js *JobStatus) HasReservation() bool {
	return geparser.IsReservation(&js.js)
}

// IsBinaryJob returns the group of the job owner.
func (js *JobStatus) IsBinaryJob() bool {
	return geparser.IsBinary(&js.js)
}

// HasNoShell returns true if the job had requested -shell no.
func (js *JobStatus) HasNoShell() bool {
	return geparser.IsNoShell(&js.js)
}

// IsArrayJob returns true in case the job is an array job.
func (js *JobStatus) IsArrayJob() bool {
	return geparser.IsArray(&js.js)
}

// JobMergesStderr returns true if job merges stderr to stdout.
func (js *JobStatus) JobMergesStderr() bool {
	return geparser.IsMergeStderr(&js.js)
}

// HasMemoryBinding returns true in case the job has memory binding requested.
func (js *JobStatus) HasMemoryBinding() bool {
	if geparser.GetMbind(&js.js) == "no_bind" {
		return false
	}
	return true
}

// MemoryBinding returns the status of the actual memory binding done for the processes of the job.
func (js *JobStatus) MemoryBinding() string {
	return geparser.GetMbind(&js.js)
}

// StartTime is when the job was dispatched to the execution host in order to start up the processes.
func (js *JobStatus) StartTime() time.Time {
	return geparser.GetStartTime(&js.js)
}

// RunTime return since how long is the job running. Note that the run-time is dynamically
// calculated assuming that the start time stamp in the cluster is in the same time zone
// then the actual RunTime() call. */
func (js *JobStatus) RunTime() time.Duration {
	if js.StartTime().Unix() != 0 {
		return time.Since(js.StartTime())
	}
	d, _ := time.ParseDuration("0s")
	return d
}

// TaskStartTime is the start time of a specific task of the job (for array jobs).
func (js *JobStatus) TaskStartTime(taskID int) time.Time {
	return geparser.GetTaskStartTime(&js.js, taskID)
}

// executionTime is the end time of the job.
func (js *JobStatus) executionTime() time.Time {
	return geparser.GetExecutionTime(&js.js)
}

// SubmissionTime is the time when the job was submitted and accepted by the cluster.
func (js *JobStatus) SubmissionTime() time.Time {
	return geparser.GetSubmissionTime(&js.js)
}

// JobDeadline returns if the job has set a deadline for starting up.
func (js *JobStatus) JobDeadline() time.Time {
	return geparser.GetDeadline(&js.js)
}

// PosixPriority returns the POSIX priority the job has requested.
// The default priority for the POSIX policy is 0 ranging from -1023
// till 1024. Only administrators can set a positive priority.
func (js *JobStatus) PosixPriority() int {
	// priority is returned as positiv integer 1024 for 0
	return geparser.GetPosixPriority(&js.js) - 1024
}

// MailOptions returns the mail options which determines on which event
// emails about job status change is sent.
func (js *JobStatus) MailOptions() string {
	return geparser.GetMailOptions(&js.js)
}

// AdvanceReservationID returns the ID of the advance reservation the
// job is running in. Note that this ID has no relationship to the
// job IDs.
func (js *JobStatus) AdvanceReservationID() int {
	return geparser.GetAR(&js.js)
}

// JobClassName returns the name of the requested job class.
func (js *JobStatus) JobClassName() string {
	return geparser.GetJobClassName(&js.js)
}

// MailAdresses returns all mail addresses the job is sending
// information about its state.
func (js *JobStatus) MailAdresses() []string {
	return geparser.GetMailingAdresses(&js.js)
}

// HardRequests returns hard resource requests as name and value
// pairs. Names are the first slice the values are encoded in the
// second slice. TODO make a map of it...
func (js *JobStatus) HardRequests() ([]string, []string) {
	return geparser.GetHardRequests(&js.js)
}

func (js *JobStatus) gdilQueueNames(what string, task int) []string {
	qil := make([]string, 16)

	gdil := geparser.GetGDIL(&js.js, task)

	if gdil == nil {
		return nil
	}
	for i := range *gdil {
		if what == "QueueName" {
			qil = append(qil, (*gdil)[i].QueueName)
		} else if what == "HostName" {
			qi := strings.Split((*gdil)[i].QueueName, "@")
			if len(qi) == 2 {
				qil = append(qil, qi[1])
			}
		} else if what == "Slots" {
			qil = append(qil, strconv.Itoa((*gdil)[i].Slots))
		}
	}

	return qil
}

// DestinationQueueInstanceList returns all queue instance names where the job is running.
// A queue instance contains a host and a "@" queue part, where the job is scheduled to.
func (js *JobStatus) DestinationQueueInstanceList() []string {
	return js.gdilQueueNames("QueueName", 0)
}

// DestinationQueueInstanceListOfTask returns the queue instances of a particular
// array job task.
func (js *JobStatus) DestinationQueueInstanceListOfTask(task int) []string {
	return js.gdilQueueNames("QueueName", task)
}

// DestinationSlotsList returns a list of slots used on the queue instances.
func (js *JobStatus) DestinationSlotsList() []string {
	return js.gdilQueueNames("Slots", 0)
}

// DestinationHostList returns all host names where the job (the first task
// in case of array jobs) is running.
func (js *JobStatus) DestinationHostList() []string {
	return js.gdilQueueNames("HostName", 0)
}

// DestinationHostListOfTask returns all hosts a parallel array job
// task is running on.
func (js *JobStatus) DestinationHostListOfTask(task int) []string {
	return js.gdilQueueNames("HostName", task)
}

// TasksCount retursn the amount of array job tasks an
// job array consists of.
func (js *JobStatus) TasksCount() int {
	return geparser.GetTaskCount(&js.js)
}

// ParallelEnvironment returns the name of the parallel environment
// requested by a job.
func (js *JobStatus) ParallelEnvironment() string {
	return geparser.GetParallelEnvironmentRequest(&js.js)
}

// ParallelEnvironmentMin contains the amount of slots requested
// by a parallel jobs.
func (js *JobStatus) ParallelEnvironmentMin() int64 {
	return geparser.GetParallelEnvironmentMin(&js.js)
}

// ParallelEnvironmentMax returns the maximum amount of slots
// required by the parallel job. It is equal to ParallelEnvironmentMin()
// in case of a fixed amount of slots were requested (which is
// the standard case).
func (js *JobStatus) ParallelEnvironmentMax() int64 {
	return geparser.GetParallelEnvironmentMax(&js.js)
}

// ParallelEnvironmentStep is the step size of a slot range
// request of a parallel job.
func (js *JobStatus) ParallelEnvironmentStep() int64 {
	return geparser.GetParallelEnvironmentStep(&js.js)
}

// ResourceUsage returns the measurements of resource consumption
// by the processes of a job.
func (js *JobStatus) ResourceUsage(task int) ([]string, []string) {
	return geparser.GetUsageList(&js.js, task)
}
