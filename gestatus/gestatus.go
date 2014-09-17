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

type ClusterJobs struct {
   jobs []private_gestatus.QstatJob
}

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

func convertQstatJobToJob(qsj private_gestatus.QstatJob) (job Job) {
   job.Number = qsj.JB_job_number
   job.Priority = qsj.JAT_prio
   job.Name = qsj.JB_name
   job.Owner = qsj.JB_owner
   job.State = qsj.State
   qsj.JAT_start_time = strings.Replace(qsj.JAT_start_time, "T", "-", 1)
   job.StartTime = qsj.JAT_start_time
   job.QueueName = qsj.Queue_Name
   job.JobClass = qsj.JClass_Name
   job.Slots = qsj.Slots
   return job
}

func GetClusterJobs() (clusterjobs ClusterJobs, err *drmaa.Error) {
   if cjs, err := private_gestatus.GetClusterJobsStatus(); err == nil {
      if cjs.JobList != nil {
         clusterjobs.jobs = cjs.JobList
         return clusterjobs, nil
      } else {
         return clusterjobs, nil
      }
   }
   return clusterjobs, err
}

func (cjs *ClusterJobs) AllJobs() []Job {
   if cjs.jobs == nil {
      return nil
   }

   jobs := make([]Job, 0)

   for _, j := range cjs.jobs {
      jobs = append(jobs, convertQstatJobToJob(j))
   }

   return jobs
}

// ----------------------------------------------------------------------------

type JobStatus struct {
   /* hide internal job status */
   js private_gestatus.InternalJobStatus
}

func GetJob(jobid string) (jobstat JobStatus, err error) {
   js, err := private_gestatus.GetJobStatusById(jobid)
   if err != nil {
      return jobstat, err
   }
   jobstat.js = js

   return jobstat, nil
}

// Returns the job status object, which contains all information
// about a job. In case of any error it is nil and a drmaa error
// is returned.
func GetJobStatus(session *drmaa.Session, jobIds string) (jobstat JobStatus, err error) {
   js, err := private_gestatus.GetJobStatus(session, jobIds)
   if err != nil {
      return jobstat, err
   }
   jobstat.js = js

   return jobstat, nil
}

/* Exported access methods for the JobInfo struct */

/* Returns the job name (given by -N submission option). */
func (js *JobStatus) JobName() string {
   return private_gestatus.GetJobName(&js.js)
}

/* Returns the Grid Engine job id number. */
func (js *JobStatus) JobId() int64 {
   return private_gestatus.GetJobNumber(&js.js)
}

/* this is the GE internatl script name which is executed on execd side */
func (js *JobStatus) execFileName() string {
   return private_gestatus.GetExecFileName(&js.js)
}

/* Returns the job script name as string. */
func (js *JobStatus) JobScript() string {
   return private_gestatus.GetScriptFile(&js.js)
}

/* Returns the job arguments as string slice. */
func (js *JobStatus) JobArgs() []string {
   return private_gestatus.GetJobArgs(&js.js)
}

/* Get owner of the job as string. */
func (js *JobStatus) JobOwner() string {
   return private_gestatus.GetOwner(&js.js)
}

/* Get ower of the job as Unix UID. */
func (js *JobStatus) JobUID() int {
   return private_gestatus.GetUID(&js.js)
}

func (js *JobStatus) JobGroup() string {
   return private_gestatus.GetGroup(&js.js)
}

func (js *JobStatus) JobGID() int {
   return private_gestatus.GetGID(&js.js)
}

func (js *JobStatus) JobAccountName() string {
   return private_gestatus.GetAccount(&js.js)
}

func (js *JobStatus) IsImmediateJob() bool {
   return private_gestatus.IsImmediate(&js.js)
}

func (js *JobStatus) HasReservation() bool {
   return private_gestatus.IsReservation(&js.js)
}

// Returns the group of the job owner.
func (js *JobStatus) IsBinaryJob() bool {
   return private_gestatus.IsBinary(&js.js)
}

// Returns true if the job had requested -shell no.
func (js *JobStatus) HasNoShell() bool {
   return private_gestatus.IsNoShell(&js.js)
}

// Returns true in case the job is an array job.
func (js *JobStatus) IsArrayJob() bool {
   return private_gestatus.IsArray(&js.js)
}

/* Returns true if job merges stderr to stdout. */
func (js *JobStatus) JobMergesStderr() bool {
   return private_gestatus.IsMergeStderr(&js.js)
}

/* Returns true in case the job has memory binding requested. */
func (js *JobStatus) HasMemoryBinding() bool {
   if private_gestatus.GetMbind(&js.js) == "no_bind" {
      return false
   }
   return true
}

/* Memory binding status. */
func (js *JobStatus) MemoryBinding() string {
   return private_gestatus.GetMbind(&js.js)
}

/* Start time of the job. */
func (js *JobStatus) StartTime() time.Time {
   return private_gestatus.GetStartTime(&js.js)
}

/* Since how long is the job running. */
func (js *JobStatus) RunTime() time.Duration {
   if js.StartTime().Unix() != 0 {
      return time.Since(js.StartTime())
   }
   d, _ := time.ParseDuration("0s")
   return d
}

/* Start time of a specific task of the job (for array jobs). */
func (js *JobStatus) TaskStartTime(taskId int) time.Time {
   return private_gestatus.GetTaskStartTime(&js.js, taskId)
}

/* End time of the job. ? */
func (js *JobStatus) executionTime() time.Time {
   return private_gestatus.GetExecutionTime(&js.js)
}

/* Submission time of the job. */
func (js *JobStatus) SubmissionTime() time.Time {
   return private_gestatus.GetSubmissionTime(&js.js)
}

/* The deadline of the job if set. */
func (js *JobStatus) JobDeadline() time.Time {
   return private_gestatus.GetDeadline(&js.js)
}

// The POSIX priority the job has requested.
func (js *JobStatus) PosixPriority() int {
   // priority is returned as positiv integer 1024 for 0
   return private_gestatus.GetPosixPriority(&js.js) - 1024
}

// The mail options which determines on which event emails
// about job status change is sent.
func (js *JobStatus) MailOptions() string {
   return private_gestatus.GetMailOptions(&js.js)
}

// The id of the advance reservation the job is running in.
func (js *JobStatus) AdvanceReservationID() int {
   return private_gestatus.GetAR(&js.js)
}

// The name of the requested job class.
func (js *JobStatus) JobClassName() string {
   return private_gestatus.GetJobClassName(&js.js)
}

// Returns all mail addresses information about the
// job is sent to, depending on the mailing options.
func (js *JobStatus) MailAdresses() []string {
   return private_gestatus.GetMailingAdresses(&js.js)
}

func (js *JobStatus) gdilQueueNames(what string, task int) []string {
   qil := make([]string, 16)

   gdil := private_gestatus.GetGDIL(&js.js, task)
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

// Returns all queue instance names where the job is running.
// A queue instance contains a host and a queue part, where the
// job is scheduled to.
func (js *JobStatus) DestinationQueueInstanceList() []string {
   return js.gdilQueueNames("QueueName", 0)
}

func (js *JobStatus) DestinationQueueInstanceListOfTask(task int) []string {
   return js.gdilQueueNames("QueueName", task)
}

func (js *JobStatus) DestinationSlotsList() []string {
   return js.gdilQueueNames("Slots", 0)
}

// Returns all host names where the job (the first task in case of array jobs)
// is running.
func (js *JobStatus) DestinationHostList() []string {
   return js.gdilQueueNames("HostName", 0)
}

func (js *JobStatus) DestinationHostListOfTask(task int) []string {
   return js.gdilQueueNames("HostName", task)
}

func (js *JobStatus) TasksCount() int {
   return private_gestatus.GetTaskCount(&js.js)
}

func (js *JobStatus) ParallelEnvironment() string {
   return private_gestatus.GetParallelEnvironmentRequest(&js.js)
}

func (js *JobStatus) ParallelEnvironmentMin() int64 {
   return private_gestatus.GetParallelEnvironmentMin(&js.js)
}

func (js *JobStatus) ParallelEnvironmentMax() int64 {
   return private_gestatus.GetParallelEnvironmentMax(&js.js)
}

func (js *JobStatus) ParallelEnvironmentStep() int64 {
   return private_gestatus.GetParallelEnvironmentStep(&js.js)
}

func (js *JobStatus) ResourceUsage(task int) ([]string, []string) {
   return private_gestatus.GetUsageList(&js.js, task)
}
