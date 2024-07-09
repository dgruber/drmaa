/*
   Copyright 2013 Daniel Gruber

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

   Note: This works on Univa Grid Engine 8.1 (tested with 8.1.7) and 8.2.
*/

// Package geparser contains functions for parsing Univa Grid Engine
// qstat -xml output. Note that those functions are not for using
// directly by the user of the gestatus library.
package geparser

/* Outsoure the XML parsing to a separate file,
   because the XML structs are exported but should not
   be visible for the user.
*/
import (
	"encoding/xml"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"time"

	"github.com/dgruber/drmaa"
)

var cachedJobStatus = map[string]jobStatusCache{}

/* TODO reap cache */

func getValidJobStatusFromCache(jobID string) (ijs InternalJobStatus, found bool) {
	/* only running jobs are cached */
	if cachedJobStatus == nil {
		return ijs, false
	}
	if cjs, found := cachedJobStatus[jobID]; found {
		/* check if expired (5 seconds) */
		lastUpdate := time.Since(cjs.lastUpdate)
		if lastUpdate.Seconds() <= 5 {
			/* valid cache */
			return cjs.jobStatus, true
		}
	}
	return ijs, false
}

// Unix converts a time from Epoch to Go time. It checks whether the
// timestamp is milliseconds or seconds since Epoch.
func Unix(value, unused int64) time.Time {
	// Open Cluster Scheduler (OCS) uses 6 digits more
	if value > 222001201000000 {
		return time.Unix(value/1000000, value%1000000*1000)
	}
	// Old Univa Grid Engine uses 3 digits more
	if value > 631152000000 {
		// must be millisecond time stamp (ms since Epoch at 1.1.1990)
		return time.Unix(int64(value/1000), 0)
	}
	// Sun Grid Engine uses seconds since Epoch
	return time.Unix(value, 0)
}

// JBEnvElem is a Grid Engine internal datatype
type JBEnvElem struct {
	// VAvariable is Grid Engine internal data
	VAvariable string `xml:"VA_variable"`
	// VAvalue is Grid Engine internal data
	VAvalue string `xml:"VA_value"`
	// VAaccessSpecifier is Grid Engine internal data
	VAaccessSpecifier int `xml:"VA_access_specifier"`
}

// JBEnvList is a Grid Engine internal datatype
type JBEnvList struct {
	// JBEnvElement is Grid Engine internal data
	JBEnvElement []JBEnvElem `xml:"element"`
}

// JBJobArgsElement is a Grid Engine internal datatype
type JBJobArgsElement struct {
	// STname is Grid Engine internal data
	STname string `xml:"ST_name"`
	// STpos  is Grid Engine internal data
	STpos int `xml:"ST_pos"`
	// STaccessSpecifier is Grid Engine internal data
	STaccessSpecifier int `xml:"ST_access_specifier"`
}

// JBJobArgs is a Grid Engine internal datatype
type JBJobArgs struct {
	// JBjobArgs is Grid Engine internal data
	JBjobArgs []JBJobArgsElement `xml:"element"`
}

// JATScaledElements is a Grid Engine internal datatype
type JATScaledElements struct {
	// UAname is Grid Engine internal data
	UAname string `xml:"UA_name"`
	// UAvalue  is Grid Engine internal data
	UAvalue string `xml:"UA_value"`
}

// JATScaledUsageList is a Grid Engine internal datatype
// This is 8.1.7 compatible but not 8.2 (use here Events)
type JATScaledUsageList struct {
	// JATscaled82 is Grid Engine internal data
	JATscaled82 []JATScaledElements `xml:"Events"`
	// JATscaled is Grid Engine internal data
	JATscaled []JATScaledElements `xml:"scaled"`
}

// JGBinding is a Grid Engine internal datatype
type JGBinding struct {
	// JGbindingElement is Grid Engine internal data
	JGbindingElement BNElement `xml:"binding"`
}

// GDILElement is a Grid Engine internal datatype
type GDILElement struct {
	// JGqname is Grid Engine internal data
	JGqname string `xml:"JG_qname"`
	// JGqversion is Grid Engine internal data
	JGqversion string `xml:"JG_qversion"`
	// JGqhostname is Grid Engine internal data
	JGqhostname string `xml:"JG_qhostname"`
	// JGslots is Grid Engine internal data
	JGslots int `xml:"JG_slots"`
	// JGqueue is Grid Engine internal data
	JGqueue string `xml:"JG_queue"`
	// JGtagslavejob is Grid Engine internal data
	JGtagslavejob int `xml:"JG_tag_slave_job"`
	// JGticket is Grid Engine internal data
	JGticket float64 `xml:"JG_ticket"`
	// JGoticket is Grid Engine internal data
	JGoticket float64 `xml:"JG_oticket"`
	// JGfticket is Grid Engine internal data
	JGfticket float64 `xml:"JG_fticket"`
	// JGsticket is Grid Engine internal data
	JGsticket float64 `xml:"JG_sticket"`
	// JGjcoticket is Grid Engine internal data
	JGjcoticket float64 `xml:"JG_jcoticket"`
	// JGjcfticket is Grid Engine internal data
	JGjcfticket float64 `xml:"JG_jcfticket"`
	// JGbinding is Grid Engine internal data
	JGbinding JGBinding `xml:"JG_binding"`
}

// JATGDIL is a Grid Engine internal datatype
type JATGDIL struct {
	// GdilElement is Grid Engine internal data
	GdilElement []GDILElement `xml:"element"`
}

// JBJaSublist is a Grid Engine internal datatype
type JBJaSublist struct {
	// JATstatus is Grid Engine internal data
	JATstatus int `xml:"JAT_status"`
	// JATtasknumber is Grid Engine internal data
	JATtasknumber int `xml:"JAT_task_number"`
	// JATscaledusagelist is Grid Engine internal data
	JATscaledusagelist JATScaledUsageList `xml:"JAT_scaled_usage_list"`
	// JATstarttime is Grid Engine internal data
	JATstarttime int64 `xml:"JAT_start_time"`
	// JATgranteddestinidentifierlist is Grid Engine internal data
	JATgranteddestinidentifierlist JATGDIL `xml:"JAT_granted_destin_identifier_list"`
}

// JBJaTasks is a Grid Engine internal datatype
// This is 8.1.7 compatible. In 8.2 it was changed to "element".
type JBJaTasks struct {
	// JaTaskSublist82 is Grid Engine internal data
	JaTaskSublist82 []JBJaSublist `xml:"element"`
	// JaTaskSublist is Grid Engine internal data
	JaTaskSublist []JBJaSublist `xml:"ulong_sublist"`
}

// TaskidRange is a Grid Engine internal datatype
type TaskidRange struct {
	// RNmin is Grid Engine internal data
	RNmin int `xml:"RN_min"`
	// RNmax is Grid Engine internal data
	RNmax int `xml:"RN_max"`
	// RNstep is Grid Engine internal data
	RNstep int `xml:"RN_step"`
}

// JBJAStructure is a Grid Engine internal datatype
type JBJAStructure struct {
	// TaskIDrange is Grid Engine internal data
	TaskIDrange TaskidRange `xml:"task_id_range"`
}

// BNElement is a Grid Engine internal datatype
type BNElement struct {
	// BNstrategy is Grid Engine internal data
	BNstrategy string `xml:"BN_strategy"`
	// BNtype is Grid Engine internal data
	BNtype int `xml:"BN_type"`
	// BNparameterN is Grid Engine internal data
	BNparameterN int `xml:"BN_parameter_n"`
	// BNparameterSocket is Grid Engine internal data
	BNparameterSocket int `xml:"BN_parameter_socket"`
	// BNparameterCoreOffset is Grid Engine internal data
	BNparameterCoreOffset int `xml:"BN_parameter_core_offset"`
	// BNparameterStridingStepSize is Grid Engine internal data
	BNparameterStridingStepSize int `xml:"BN_parameter_striding_step_size"`
	// BNparameterExplicit is Grid Engine internal data
	BNparameterExplicit string `xml:"BN_parameter_explicit"`
	// BNparameterNlocal is Grid Engine internal data
	BNparameterNlocal int `xml:"BN_parameter_nlocal"`
}

// JBBinding is a Grid Engine internal datatype
type JBBinding struct {
	// BindingElement is Grid Engine internal data
	BindingElement BNElement `xml:"element"`
}

// MRElement is a Grid Engine internal datatype
// one element of the mail address list
type MRElement struct {
	// User is Grid Engine internal data
	User string `xml:"MR_user"`
	// Host is Grid Engine internal data
	Host string `xml:"MR_host"`
}

// JBMailList is a Grid Engine internal datatype
// list of mail addresses
type JBMailList struct {
	// MailElement is Grid Engine internal data
	MailElement []MRElement `xml:"element"`
}

// JBHard is a Grid Engine internal datatype
// hard resource request list
type JBHard struct {
	// HardResourceRequest is Grid Engine internal data
	HardResourceRequest []HardElement `xml:"element"`
}

// HardElement is a Grid Engine internal datatype
// one element of a hard request -hard -l
type HardElement struct {
	// Name is Grid Engine internal data
	Name string `xml:"CE_name"`
	// ValType is Grid Engine internal data
	ValType int `xml:"CE_valtype"`
	// StringVal is Grid Engine internal data
	StringVal string `xml:"CE_stringval"`
	// DoubleVal is Grid Engine internal data
	DoubleVal float64 `xml:"CE_doubleval"`
	// Consumable is Grid Engine internal data
	Consumable int `xml:"CE_consumable"`
}

// RangeElement is a Grid Engine internal datatype
type RangeElement struct {
	// RnMin is Grid Engine internal data
	RnMin int64 `xml:"RN_min"`
	// RnMax is Grid Engine internal data
	RnMax int64 `xml:"RN_max"`
	// RnStep is Grid Engine internal data
	RnStep int64 `xml:"RN_step"`
}

// JBPERange is a Grid Engine internal datatype
type JBPERange struct {
	// Range is Grid Engine internal data
	Range RangeElement `xml:"element"`
}

// Element is a Grid Engine internal datatype
type Element struct {
	// JbJobNumber is Grid Engine internal data
	JbJobNumber int64 `xml:"JB_job_number"`
	// JbAr is Grid Engine internal data
	JbAr int `xml:"JB_ar"`
	// JbExecFile is Grid Engine internal data
	JbExecFile string `xml:"JB_exec_file"`
	// JbSubmissionTime is Grid Engine internal data
	JbSubmissionTime string `xml:"JB_submission_time"`
	// JbOwner is Grid Engine internal data
	JbOwner string `xml:"JB_owner"`
	// JbUID is Grid Engine internal data
	JbUID int `xml:"JB_uid"`
	// JbGroup is Grid Engine internal data
	JbGroup string `xml:"JB_group"`
	// JbGid is Grid Engine internal data
	JbGid int `xml:"JB_gid"`
	// JbAccount is Grid Engine internal data
	JbAccount string `xml:"JB_account"`
	// JbMergeStderr is Grid Engine internal data
	JbMergeStderr bool `xml:"JB_merge_stderr"`
	// JbNotify is Grid Engine internal data
	JbNotify string `xml:"JB_notify"`
	// JbJobName is Grid Engine internal data
	JbJobName string `xml:"JB_job_name"`
	// JbJobShare is Grid Engine internal data
	JbJobShare int `xml:"JB_job_share"`
	// JbEnvList is Grid Engine internal data
	JbEnvList JBEnvList `xml:"JB_env_list"`
	// JbJobArgs is Grid Engine internal data
	JbJobArgs JBJobArgs `xml:"JB_job_args"`
	// JbScriptFile is Grid Engine internal data
	JbScriptFile string `xml:"JB_script_file"`
	// JbJaTasks is Grid Engine internal data
	JbJaTasks JBJaTasks `xml:"JB_ja_tasks"`
	// JbDeadline is Grid Engine internal data
	JbDeadline int64 `xml:"JB_deadline"`
	// JbExecutionTime is Grid Engine internal data
	JbExecutionTime int64 `xml:"JB_execution_time"`
	// JbCheckpointAttr is Grid Engine internal data
	JbCheckpointAttr string `xml:"JB_checkpoint_attr"`
	// JbCheckpointIntvl is Grid Engine internal data
	JbCheckpointIntvl int `xml:"JB_checkpoint_interval"`
	// JbReserve is Grid Engine internal data
	JbReserve bool `xml:"JB_reserve"`
	// JbMailOptions is Grid Engine internal data
	JbMailOptions string `xml:"JB_mail_options"`
	// JbPriority is Grid Engine internal data
	JbPriority int `xml:"JB_priority"`
	// JbRestart is Grid Engine internal data
	JbRestart int `xml:"JB_restart"`
	// JbVerify is Grid Engine internal data
	JbVerify int `xml:"JB_verify"`
	// JbScriptSize is Grid Engine internal data
	JbScriptSize int `xml:"JB_script_size"`
	// JbVerifySuitableQueues is Grid Engine internal data
	JbVerifySuitableQueues int `xml:"JB_verify_suitable_queues"`
	// JbSoftWallclockGmt is Grid Engine internal data
	JbSoftWallclockGmt int `xml:"JB_soft_wallclock_gmt"`
	// JbHardWallclockGmt is Grid Engine internal data
	JbHardWallclockGmt int `xml:"JB_hard_wallclock_gmt"`
	// JbHardResourceRequest is Grid Engine internal data
	JbHardResourceRequest JBHard `xml:"JB_hard_resource_list"`
	// JbOverrideTickets is Grid Engine internal data
	JbOverrideTickets int `xml:"JB_override_tickets"`
	// JbVersion is Grid Engine internal data
	JbVersion int `xml:"JB_version"`
	// JbJaStructure is Grid Engine internal data
	JbJaStructure JBJAStructure `xml:"JB_ja_structure"`
	// JbType is Grid Engine internal data
	JbType int `xml:"JB_type"`
	// JbBinding is Grid Engine internal data
	JbBinding JBBinding `xml:"JB_binding"`
	// JbMbind is Grid Engine internal data
	JbMbind int `xml:"JB_mbind"`
	// JbIsBinary is Grid Engine internal data
	JbIsBinary bool `xml:"JB_is_binary"`
	// JbNoShell is Grid Engine internal data
	JbNoShell bool `xml:"JB_no_shell"`
	// JbIsArray is Grid Engine internal data
	JbIsArray bool `xml:"JB_is_array"`
	// JbIsImmediate is Grid Engine internal data
	JbIsImmediate bool `xml:"JB_is_immediate"`
	// JbJcName is Grid Engine internal data
	JbJcName string `xml:"JB_jc_name"`
	// JbMailList is Grid Engine internal data
	JbMailList JBMailList `xml:"JB_mail_list"`
	// JbPe is Grid Engine internal data
	JbPe string `xml:"JB_pe"`
	// JbPeRange is Grid Engine internal data
	JbPeRange JBPERange `xml:"JB_pe_range"`
}

// DjobInfo is a Grid Engine internal datatype
type DjobInfo struct {
	// Element is Grid Engine internal data
	Element Element `xml:"element"`
}

// InternalJobStatus contains scheduler messages and djob_info
type InternalJobStatus struct {
	// XMLName is Grid Engine internal data
	XMLName xml.Name `xml:"detailed_job_info"`
	// Jobinf is Grid Engine internal data
	Jobinf DjobInfo `xml:"djob_info"`
}

// -----------------------------------------------

// GDIL is a Grid Engine internal datatype
type GDIL struct {
	// QueueName is Grid Engine internal data
	QueueName string
	// Slots is Grid Engine internal data
	Slots int
	// Ticket is Grid Engine internal data
	Ticket float64
	// OverrideTicket is Grid Engine internal data
	OverrideTicket float64
	// FunctionalTicket is Grid Engine internal data
	FunctionalTicket float64
	// SharetreeTicket is Grid Engine internal data
	SharetreeTicket float64
}

// -----------------------------------------------
// XML of "qstat -xml" without parameters
// -----------------------------------------------

// QstatJob is a Grid Engine internal datatype
type QstatJob struct {
	// JBjobnumber is Grid Engine internal data
	JBjobnumber int64 `xml:"JB_job_number"`
	// JATprio is Grid Engine internal data
	JATprio float64 `xml:"JAT_prio"`
	// JBname is Grid Engine internal data
	JBname string `xml:"JB_name"`
	// JBowner is Grid Engine internal data
	JBowner string `xml:"JB_owner"`
	// State is Grid Engine internal data
	State string `xml:"state"`
	// JATstarttime is Grid Engine internal data
	JATstarttime string `xml:"JAT_start_time"`
	// QueueName is Grid Engine internal data
	QueueName string `xml:"queue_name"`
	// JClassName is Grid Engine internal data
	JClassName string `xml:"jclass_name"`
	// Slots is Grid Engine internal data
	Slots int64 `xml:"slots"`
}

// QstatJobInfoList is a Grid Engine internal datatype
type QstatJobInfoList struct {
	// XMLName is Grid Engine internal data
	XMLName xml.Name `xml:"job_info"`
	// JobList is Grid Engine internal data
	// contains queue_info and job_info
	JobList []QstatJob `xml:"queue_info>job_list"`
}

// -----------------------------------------------
// Wrapper functions
// -----------------------------------------------

// GetJobName return the name of the job.
func GetJobName(v *InternalJobStatus) string {
	return v.Jobinf.Element.JbJobName
}

// GetJobNumber returns the ID of the job.
func GetJobNumber(v *InternalJobStatus) int64 {
	return v.Jobinf.Element.JbJobNumber
}

// GetExecFileName returns the command of the job.
func GetExecFileName(v *InternalJobStatus) string {
	return v.Jobinf.Element.JbExecFile
}

// GetJobArgs returns the arguments of the remote command.
func GetJobArgs(v *InternalJobStatus) []string {
	args := make([]string, len(v.Jobinf.Element.JbJobArgs.JBjobArgs))
	for _, i := range v.Jobinf.Element.JbJobArgs.JBjobArgs {
		args = append(args, i.STname)
	}
	return args
}

// GetScriptFile return the job script executed.
func GetScriptFile(v *InternalJobStatus) string {
	return v.Jobinf.Element.JbScriptFile
}

// GetAdvanceReservationID returns the advance reservation ID the job has
// requested.
func GetAdvanceReservationID(v *InternalJobStatus) int {
	return v.Jobinf.Element.JbAr
}

// GetOwner returns the owner of the job.
func GetOwner(v *InternalJobStatus) string {
	return v.Jobinf.Element.JbOwner
}

// GetGroup returns the UNIX group the owner of the job is member of.
func GetGroup(v *InternalJobStatus) string {
	return v.Jobinf.Element.JbGroup
}

// GetUID return the job owner's UNIX user ID.
func GetUID(v *InternalJobStatus) int {
	return v.Jobinf.Element.JbUID
}

// GetGID returns the job owner's UNIX group ID.
func GetGID(v *InternalJobStatus) int {
	return v.Jobinf.Element.JbGid
}

// GetAR returns the advance reservation the job is running in.
func GetAR(v *InternalJobStatus) int {
	return v.Jobinf.Element.JbAr
}

// GetMailOptions returns the mail settings of the job.
func GetMailOptions(v *InternalJobStatus) string {
	return v.Jobinf.Element.JbMailOptions
}

// GetPosixPriority returns the posix priority requested or set to the job.
func GetPosixPriority(v *InternalJobStatus) int {
	return v.Jobinf.Element.JbPriority
}

// GetAccount returns the string set for acccounting for the job.
func GetAccount(v *InternalJobStatus) string {
	return v.Jobinf.Element.JbAccount
}

// IsImmediate returns if the job is tried to by scheduled just once.
func IsImmediate(v *InternalJobStatus) bool {
	return v.Jobinf.Element.JbIsImmediate
}

// IsReservation returns if the job had a reservation requested.
func IsReservation(v *InternalJobStatus) bool {
	return v.Jobinf.Element.JbReserve
}

// IsBinary returns if the job is a binary or not.
func IsBinary(v *InternalJobStatus) bool {
	return v.Jobinf.Element.JbIsBinary
}

// IsNoShell returns if the job should be started without a starter shell.
func IsNoShell(v *InternalJobStatus) bool {
	return v.Jobinf.Element.JbNoShell
}

// IsArray returns if the job is a job array.
func IsArray(v *InternalJobStatus) bool {
	return v.Jobinf.Element.JbIsArray
}

// IsMergeStderr returns if the stdout and stderr streams are merged by
// the jobs.
func IsMergeStderr(v *InternalJobStatus) bool {
	return v.Jobinf.Element.JbMergeStderr
}

// GetMbind returns the memory binding requested by the job.
func GetMbind(v *InternalJobStatus) string {
	switch v.Jobinf.Element.JbMbind {
	case '1':
		return "no_bind"
	case '2':
		return "cores"
	case '3':
		return "cores:strict"
	case '4':
		return "round_robin"
	}
	return "no_bind"
}

// GetSubmissionTime returns when the job was submitted.
func GetSubmissionTime(v *InternalJobStatus) time.Time {
	submissionTime := v.Jobinf.Element.JbSubmissionTime
	st, _ := strconv.Atoi(submissionTime)

	// Open Cluster Scheduler has 6 digits more, hence
	// check if the submission time value is higher than
	// that value.

	return Unix((int64)(st), 0)
}

// GetExecutionTime returns how long the job was running.
func GetExecutionTime(v *InternalJobStatus) time.Time {
	return Unix(v.Jobinf.Element.JbExecutionTime, 0)
}

// GetStartTime returns when the first task of the job was started.
func GetStartTime(v *InternalJobStatus) time.Time {
	return GetTaskStartTime(v, 0)
}

// GetTaskStartTime returns the start time of specific task of the
// job
func GetTaskStartTime(v *InternalJobStatus, taskID int) time.Time {
	// 8.1
	size := len(v.Jobinf.Element.JbJaTasks.JaTaskSublist)
	// 8.2. compatibility
	size82 := len(v.Jobinf.Element.JbJaTasks.JaTaskSublist82)

	// one of both is 0 and the other is not
	if taskID >= (size+size82) || taskID < 0 {
		// error taskId not in range
		return Unix(0, 0)
	}
	// only sublist or sublist82 is set by the XML parser
	var startTime int64
	if size > size82 {
		startTime = v.Jobinf.Element.JbJaTasks.JaTaskSublist[taskID].JATstarttime
	} else {
		startTime = v.Jobinf.Element.JbJaTasks.JaTaskSublist82[taskID].JATstarttime
	}

	return Unix(startTime, 0)
}

// GetDeadline returns the deadline time to be scheduled set for the job.
func GetDeadline(v *InternalJobStatus) time.Time {
	return Unix(v.Jobinf.Element.JbDeadline, 0)
}

// GetJobClassName returns the name of the requested job class for the job.
func GetJobClassName(v *InternalJobStatus) string {
	return v.Jobinf.Element.JbJcName
}

// GetMailingAdresses returns all mailing addresses which are informed when
// the state of the job is changing.
func GetMailingAdresses(v *InternalJobStatus) []string {
	list := make([]string, 1)
	for _, el := range v.Jobinf.Element.JbMailList.MailElement {
		address := fmt.Sprintf("%s@%s", el.User, el.Host)
		list = append(list, address)
	}
	return list
}

// GetParallelEnvironmentRequest returns if a parallel environment
// was requested by the job and hence the job is parallel meaning
// using multiple cores or even multiple hosts.
func GetParallelEnvironmentRequest(v *InternalJobStatus) string {
	return v.Jobinf.Element.JbPe
}

// GetParallelEnvironmentMin returns the amount of slots the parallel
// application requires.
func GetParallelEnvironmentMin(v *InternalJobStatus) int64 {
	return v.Jobinf.Element.JbPeRange.Range.RnMin
}

// GetParallelEnvironmentMax returns the amount of slots the parallel
// jobs wants at maximum.
func GetParallelEnvironmentMax(v *InternalJobStatus) int64 {
	return v.Jobinf.Element.JbPeRange.Range.RnMax
}

// GetParallelEnvironmentStep returns the step size for searching
// between the minimum and max. amount of slots a job can use.
func GetParallelEnvironmentStep(v *InternalJobStatus) int64 {
	return v.Jobinf.Element.JbPeRange.Range.RnStep
}

// GetUsageList returns the usage lists (resource[0] has value value[0])
func GetUsageList(v *InternalJobStatus, task int) (resource []string, value []string) {
	if len(v.Jobinf.Element.JbJaTasks.JaTaskSublist) > task {
		var scaled []JATScaledElements
		if len(v.Jobinf.Element.JbJaTasks.JaTaskSublist[task].JATscaledusagelist.JATscaled) <= 0 {
			scaled = v.Jobinf.Element.JbJaTasks.JaTaskSublist[task].JATscaledusagelist.JATscaled82
		} else {
			scaled = v.Jobinf.Element.JbJaTasks.JaTaskSublist[task].JATscaledusagelist.JATscaled
		}
		for _, v := range scaled {
			resource = append(resource, v.UAname)
			value = append(value, v.UAvalue)
		}
	}
	return resource, value
}

// GetHardRequests pairs of name and value from hard resource requests
// (-hard -l mem=1G) -> "mem" "1G
func GetHardRequests(v *InternalJobStatus) (nm []string, val []string) {
	for _, name := range v.Jobinf.Element.JbHardResourceRequest.HardResourceRequest {
		nm = append(nm, name.Name)
		val = append(val, name.StringVal)
	}
	return nm, val
}

// GetTaskCount returns the amount of tasks found
func GetTaskCount(v *InternalJobStatus) int {
	taskList := v.Jobinf.Element.JbJaTasks.JaTaskSublist
	taskList82 := v.Jobinf.Element.JbJaTasks.JaTaskSublist82
	return len(taskList) + len(taskList82)
}

// GetGDIL returns an array of granted destination identifiers
func GetGDIL(v *InternalJobStatus, task int) *[]GDIL {
	var taskList []JBJaSublist

	taskList = v.Jobinf.Element.JbJaTasks.JaTaskSublist
	taskListSize := len(taskList)
	if taskListSize <= 0 {
		taskList = v.Jobinf.Element.JbJaTasks.JaTaskSublist82
		taskListSize = len(taskList)
	}

	if task < 0 || task >= taskListSize {
		// err
		return nil
	}

	gdil := make([]GDIL, len(taskList[task].JATgranteddestinidentifierlist.GdilElement))

	for i, gdi := range taskList[task].JATgranteddestinidentifierlist.GdilElement {
		gdil[i].FunctionalTicket = gdi.JGfticket
		gdil[i].OverrideTicket = gdi.JGoticket
		gdil[i].QueueName = gdi.JGqname
		gdil[i].SharetreeTicket = gdi.JGsticket
		gdil[i].Slots = gdi.JGslots
		gdil[i].Ticket = gdi.JGticket
	}

	return &gdil
}

// parseXML parses the qstat -j -xml output and fills
// the InternalJobStatus struct
func parseXML(xmlOutput []byte) (ijs InternalJobStatus, err error) {
	err2 := xml.Unmarshal(xmlOutput, &ijs)

	if err2 != nil {
		//fmt.Println(err2)
		return ijs, fmt.Errorf("XML unmarshall error")
	}
	return ijs, nil
}

// parseQstatXML parses the qstat -j -xml output and fills
// the QstatJobInfoList struct
func parseQstatXML(xmlOutput []byte) (qjl QstatJobInfoList, err error) {
	err2 := xml.Unmarshal(xmlOutput, &qjl)
	if err2 != nil {
		return qjl, fmt.Errorf("XML unmarshall error")
	}
	return qjl, nil
}

// makeError is a internal function which creates an GO DRMAA error
func makeError(msg string, id drmaa.ErrorID) drmaa.Error {
	var ce drmaa.Error
	ce.Message = msg
	ce.ID = id
	return ce
}

// GetJobStatusByID makes a qstat -xml -j jobID call and returns
// the pared result.
func GetJobStatusByID(jobID string) (ijs InternalJobStatus, err error) {
	cmd := exec.Command("qstat", "-xml", "-j", jobID)
	out, err := cmd.Output()
	if err != nil {
		return ijs, err
	}
	return parseXML(out)
}

// GetJobStatus for use within Go DRMAA lib. It first queries the job
// status cache and if the entry is not found it makes a direct qstat -j -xml call
func GetJobStatus(s *drmaa.Session, jobID string) (ijs InternalJobStatus, er error) {
	/* check status of job - could slow down qmaster when having lots of clients.
	   Grid Engine DRMAA2 implementation will solve this. */
	pt, err := s.JobPs(jobID)
	if err != nil {
		return ijs, err
	}

	if pt != drmaa.PsRunning {
		err2 := makeError("Job is not in running state.", drmaa.InvalidJob)
		return ijs, &err2
	}

	cjs, found := getValidJobStatusFromCache(jobID)
	if found == false {
		/* job is not cached yet or outdated - we need to fetch it */
		//qstat := fmt.Sprint("-xml -j ", jobId)
		cmd := exec.Command("qstat", "-xml", "-j", jobID)
		out, err := cmd.Output()
		if err != nil {
			log.Fatal("Could not execute qstat -xml -j")
			err := makeError("Could not execute qstat.", drmaa.InternalError)
			return ijs, &err
		}

		js, err := parseXML(out)
		if err != nil {
			log.Fatal("Could not parse xml output of qstat.")
			err := makeError("Could not parse XML output of qstat.", drmaa.InternalError)
			return ijs, &err
		}

		// Cache job status
		cachedJobStatus[jobID] = jobStatusCache{time.Now(), js}
		return js, nil
	}
	// return cached job status
	return cjs, nil
}

// jobStatusCache is the global job status cache used for
// caching qstat results in order to reduce the actual amount
// of qstat calls.
type jobStatusCache struct {
	lastUpdate time.Time
	jobStatus  InternalJobStatus
}

// GetClusterJobsStatus returns all running jobs of a particular user by
// executing and parsing the qstat -xml output.
func GetClusterJobsStatus() (ijs QstatJobInfoList, er error) {
	cmd := exec.Command("qstat", "-xml")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal("Could not execute qstat -xml")
		err := makeError("Could not execute qstat.", drmaa.InternalError)
		return ijs, &err
	}
	qsjil, err := parseQstatXML(out)
	if err != nil {
		fmt.Println(err)
		return ijs, err
	}
	return qsjil, nil
}
