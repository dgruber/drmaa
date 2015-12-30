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

package private_gestatus

/* Outsoure the XML parsing to a separate file,
   because the XML structs are exported but should not
   be visible for the user.
*/
import (
	"encoding/xml"
	"fmt"
	"github.com/dgruber/drmaa"
	"log"
	"os/exec"
	"strconv"
	"time"
)

var cachedJobStatus = map[string]jobStatusCache{}

/* TODO reap cache */

func getValidJobStatusFromCache(jobId string) (ijs InternalJobStatus, found bool) {
	/* only running jobs are cached */
	if cachedJobStatus == nil {
		return ijs, false
	}
	if cjs, found := cachedJobStatus[jobId]; found {
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
	if value > 631152000000 {
		// must be millisecond time stamp (ms since Epoch at 1.1.1990)
		return time.Unix(int64(value/1000), 0)
	}
	return time.Unix(value, 0)
}

type JBEnvElem struct {
	VA_variable         string `xml:"VA_variable"`
	VA_value            string `xml:"VA_value"`
	VA_access_specifier int    `xml:"VA_access_specifier"`
}

type JBEnvList struct {
	JBEnvElement []JBEnvElem `xml:"element"`
}

type JBJobArgsElement struct {
	ST_name             string `xml:"ST_name"`
	ST_pos              int    `xml:"ST_pos"`
	ST_access_specifier int    `xml:"ST_access_specifier"`
}

type JBJobArgs struct {
	JB_job_args []JBJobArgsElement `xml:"element"`
}

type JATScaledElements struct {
	UA_name  string `xml:"UA_name"`
	UA_value string `xml:"UA_value"`
}

// This is 8.1.7 compatible but not 8.2 (use here Events)
type JATScaledUsageList struct {
	JAT_scaled82 []JATScaledElements `xml:"Events"`
	JAT_scaled   []JATScaledElements `xml:"scaled"`
}

type JGBinding struct {
	JG_binding_element BNElement `xml:"binding"`
}

type GDILElement struct {
	JG_qname         string    `xml:"JG_qname"`
	JG_qversion      string    `xml:"JG_qversion"`
	JG_qhostname     string    `xml:"JG_qhostname"`
	JG_slots         int       `xml:"JG_slots"`
	JG_queue         string    `xml:"JG_queue"`
	JG_tag_slave_job int       `xml:"JG_tag_slave_job"`
	JG_ticket        float64   `xml:"JG_ticket"`
	JG_oticket       float64   `xml:"JG_oticket"`
	JG_fticket       float64   `xml:"JG_fticket"`
	JG_sticket       float64   `xml:"JG_sticket"`
	JG_jcoticket     float64   `xml:"JG_jcoticket"`
	JG_jcfticket     float64   `xml:"JG_jcfticket"`
	JG_binding       JGBinding `xml:"JG_binding"`
}

type JATGDIL struct {
	GdilElement []GDILElement `xml:"element"`
}

type JBJaSublist struct {
	JAT_status                         int                `xml:"JAT_status"`
	JAT_task_number                    int                `xml:"JAT_task_number"`
	JAT_scaled_usage_list              JATScaledUsageList `xml:"JAT_scaled_usage_list"`
	JAT_start_time                     int64              `xml:"JAT_start_time"`
	JAT_granted_destin_identifier_list JATGDIL            `xml:"JAT_granted_destin_identifier_list"`
}

// This is 8.1.7 compatible. In 8.2 it was changed to "element".
type JBJaTasks struct {
	JaTaskSublist82 []JBJaSublist `xml:"element"`
	JaTaskSublist   []JBJaSublist `xml:"ulong_sublist"`
}

type TaskIdRange struct {
	RN_min  int `xml:"RN_min"`
	RN_max  int `xml:"RN_max"`
	RN_step int `xml:"RN_step"`
}

type JBJAStructure struct {
	Task_id_range TaskIdRange `xml:"task_id_range"`
}

type BNElement struct {
	BN_strategy                     string `xml:"BN_strategy"`
	BN_type                         int    `xml:"BN_type"`
	BN_parameter_n                  int    `xml:"BN_parameter_n"`
	BN_parameter_socket             int    `xml:"BN_parameter_socket"`
	BN_parameter_core_offset        int    `xml:"BN_parameter_core_offset"`
	BN_parameter_striding_step_size int    `xml:"BN_parameter_striding_step_size"`
	BN_parameter_explicit           string `xml:"BN_parameter_explicit"`
	BN_parameter_nlocal             int    `xml:"BN_parameter_nlocal"`
}

type JBBinding struct {
	BindingElement BNElement `xml:"element"`
}

// one element of the mail adress list
type MRElement struct {
	User string `xml:"MR_user"`
	Host string `xml:"MR_host"`
}

// list of mail adresses
type JBMailList struct {
	MailElement []MRElement `xml:"element"`
}

// hard resource request list
type JBHard struct {
	HardResourceRequest []HardElement `xml:"element"`
}

// one element of a hard request -hard -l
type HardElement struct {
	Name       string  `xml:"CE_name"`
	ValType    int     `xml:"CE_valtype"`
	StringVal  string  `xml:"CE_stringval"`
	DoubleVal  float64 `xml:"CE_doubleval"`
	Consumable int     `xml:"CE_consumable"`
}

type RangeElement struct {
	Rn_min  int64 `xml:"RN_min"`
	Rn_max  int64 `xml:"RN_max"`
	Rn_step int64 `xml:"RN_step"`
}
type JBPERange struct {
	Range RangeElement `xml:"element"`
}

type Element struct {
	Jb_job_number             int64         `xml:"JB_job_number"`
	Jb_ar                     int           `xml:"JB_ar"`
	Jb_exec_file              string        `xml:"JB_exec_file"`
	Jb_submission_time        string        `xml:"JB_submission_time"`
	Jb_owner                  string        `xml:"JB_owner"`
	Jb_uid                    int           `xml:"JB_uid"`
	Jb_group                  string        `xml:"JB_group"`
	Jb_gid                    int           `xml:"JB_gid"`
	Jb_account                string        `xml:"JB_account"`
	Jb_merge_stderr           bool          `xml:"JB_merge_stderr"`
	Jb_notify                 string        `xml:"JB_notify"`
	Jb_job_name               string        `xml:"JB_job_name"`
	Jb_job_share              int           `xml:"JB_job_share"`
	Jb_env_list               JBEnvList     `xml:"JB_env_list"`
	Jb_job_args               JBJobArgs     `xml:"JB_job_args"`
	Jb_script_file            string        `xml:"JB_script_file"`
	Jb_ja_tasks               JBJaTasks     `xml:"JB_ja_tasks"`
	Jb_deadline               int64         `xml:"JB_deadline"`
	Jb_execution_time         int64         `xml:"JB_execution_time"`
	Jb_checkpoint_attr        string        `xml:"JB_checkpoint_attr"`
	Jb_checkpoint_intvl       int           `xml:"JB_checkpoint_interval"`
	Jb_reserve                bool          `xml:"JB_reserve"`
	Jb_mail_options           string        `xml:"JB_mail_options"`
	Jb_priority               int           `xml:"JB_priority"`
	Jb_restart                int           `xml:"JB_restart"`
	Jb_verify                 int           `xml:"JB_verify"`
	Jb_script_size            int           `xml:"JB_script_size"`
	Jb_verify_suitable_queues int           `xml:"JB_verify_suitable_queues"`
	Jb_soft_wallclock_gmt     int           `xml:"JB_soft_wallclock_gmt"`
	Jb_hard_wallclock_gmt     int           `xml:"JB_hard_wallclock_gmt"`
	Jb_hard_resource_request  JBHard        `xml:"JB_hard_resource_list"`
	Jb_override_tickets       int           `xml:"JB_override_tickets"`
	Jb_version                int           `xml:"JB_version"`
	Jb_ja_structure           JBJAStructure `xml:"JB_ja_structure"`
	Jb_type                   int           `xml:"JB_type"`
	Jb_binding                JBBinding     `xml:"JB_binding"`
	Jb_mbind                  int           `xml:"JB_mbind"`
	Jb_is_binary              bool          `xml:"JB_is_binary"`
	Jb_no_shell               bool          `xml:"JB_no_shell"`
	Jb_is_array               bool          `xml:"JB_is_array"`
	Jb_is_immediate           bool          `xml:"JB_is_immediate"`
	Jb_jc_name                string        `xml:"JB_jc_name"`
	Jb_mail_list              JBMailList    `xml:"JB_mail_list"`
	Jb_pe                     string        `xml:"JB_pe"`
	Jb_pe_range               JBPERange     `xml:"JB_pe_range"`
}

type DjobInfo struct {
	Element Element `xml:"element"`
}

// contains scheduler messages and djob_info
type InternalJobStatus struct {
	XMLName xml.Name `xml:"detailed_job_info"`
	Jobinf  DjobInfo `xml:"djob_info"`
}

// -----------------------------------------------
type GDIL struct {
	QueueName        string
	Slots            int
	Ticket           float64
	OverrideTicket   float64
	FunctionalTicket float64
	SharetreeTicket  float64
}

// -----------------------------------------------
// XML of "qstat -xml" without parameters
// -----------------------------------------------
type QstatJob struct {
	JB_job_number  int64   `xml:"JB_job_number"`
	JAT_prio       float64 `xml:"JAT_prio"`
	JB_name        string  `xml:"JB_name"`
	JB_owner       string  `xml:"JB_owner"`
	State          string  `xml:"state"`
	JAT_start_time string  `xml:"JAT_start_time"`
	Queue_Name     string  `xml:"queue_name"`
	JClass_Name    string  `xml:"jclass_name"`
	Slots          int64   `xml:"slots"`
}

// Element describes "qstat -xml"
type QstatJobInfoList struct {
	XMLName xml.Name `xml:"job_info"`
	// contains queue_info and job_info
	JobList []QstatJob `xml:"queue_info>job_list"`
}

// -----------------------------------------------
// Wrapper functions
// -----------------------------------------------

func GetJobName(v *InternalJobStatus) string {
	return v.Jobinf.Element.Jb_job_name
}

func GetJobNumber(v *InternalJobStatus) int64 {
	return v.Jobinf.Element.Jb_job_number
}

func GetExecFileName(v *InternalJobStatus) string {
	return v.Jobinf.Element.Jb_exec_file
}

func GetJobArgs(v *InternalJobStatus) []string {
	args := make([]string, len(v.Jobinf.Element.Jb_job_args.JB_job_args))
	for _, i := range v.Jobinf.Element.Jb_job_args.JB_job_args {
		args = append(args, i.ST_name)
	}
	return args
}

func GetScriptFile(v *InternalJobStatus) string {
	return v.Jobinf.Element.Jb_script_file
}

func GetAdvanceReservationID(v *InternalJobStatus) int {
	return v.Jobinf.Element.Jb_ar
}

func GetOwner(v *InternalJobStatus) string {
	return v.Jobinf.Element.Jb_owner
}

func GetGroup(v *InternalJobStatus) string {
	return v.Jobinf.Element.Jb_group
}

func GetUID(v *InternalJobStatus) int {
	return v.Jobinf.Element.Jb_uid
}

func GetGID(v *InternalJobStatus) int {
	return v.Jobinf.Element.Jb_gid
}

func GetAR(v *InternalJobStatus) int {
	return v.Jobinf.Element.Jb_ar
}

func GetMailOptions(v *InternalJobStatus) string {
	return v.Jobinf.Element.Jb_mail_options
}

func GetPosixPriority(v *InternalJobStatus) int {
	return v.Jobinf.Element.Jb_priority
}

func GetAccount(v *InternalJobStatus) string {
	return v.Jobinf.Element.Jb_account
}

func IsImmediate(v *InternalJobStatus) bool {
	return v.Jobinf.Element.Jb_is_immediate
}

func IsReservation(v *InternalJobStatus) bool {
	return v.Jobinf.Element.Jb_reserve
}

func IsBinary(v *InternalJobStatus) bool {
	return v.Jobinf.Element.Jb_is_binary
}

func IsNoShell(v *InternalJobStatus) bool {
	return v.Jobinf.Element.Jb_no_shell
}

func IsArray(v *InternalJobStatus) bool {
	return v.Jobinf.Element.Jb_is_array
}

func IsMergeStderr(v *InternalJobStatus) bool {
	return v.Jobinf.Element.Jb_merge_stderr
}

func GetMbind(v *InternalJobStatus) string {
	switch v.Jobinf.Element.Jb_mbind {
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

func GetSubmissionTime(v *InternalJobStatus) time.Time {
	submissionTime := v.Jobinf.Element.Jb_submission_time
	st, _ := strconv.Atoi(submissionTime)
	return Unix((int64)(st), 0)
}

func GetExecutionTime(v *InternalJobStatus) time.Time {
	return Unix(v.Jobinf.Element.Jb_execution_time, 0)
}

// start time of first task started
func GetStartTime(v *InternalJobStatus) time.Time {
	return GetTaskStartTime(v, 0)
}

// get start time of specific task
func GetTaskStartTime(v *InternalJobStatus, taskId int) time.Time {
	// 8.1
	size := len(v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist)
	// 8.2. compatibility
	size82 := len(v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist82)

	if taskId >= (size+size82) || taskId < 0 {
		// error taskId not in range
		return Unix(0, 0)
	}
	// only sublist or sublist82 is set by the XML parser
	var startTime int64
	if size > size82 {
		startTime = v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist[taskId].JAT_start_time
	} else {
		startTime = v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist82[taskId].JAT_start_time
	}
	return Unix(startTime, 0)
}

func GetDeadline(v *InternalJobStatus) time.Time {
	return Unix(v.Jobinf.Element.Jb_deadline, 0)
}

func GetJobClassName(v *InternalJobStatus) string {
	return v.Jobinf.Element.Jb_jc_name
}

func GetMailingAdresses(v *InternalJobStatus) []string {
	list := make([]string, 1)
	for _, el := range v.Jobinf.Element.Jb_mail_list.MailElement {
		address := fmt.Sprintf("%s@%s", el.User, el.Host)
		list = append(list, address)
	}
	return list
}

func GetParallelEnvironmentRequest(v *InternalJobStatus) string {
	return v.Jobinf.Element.Jb_pe
}

func GetParallelEnvironmentMin(v *InternalJobStatus) int64 {
	return v.Jobinf.Element.Jb_pe_range.Range.Rn_min
}

func GetParallelEnvironmentMax(v *InternalJobStatus) int64 {
	return v.Jobinf.Element.Jb_pe_range.Range.Rn_max
}

func GetParallelEnvironmentStep(v *InternalJobStatus) int64 {
	return v.Jobinf.Element.Jb_pe_range.Range.Rn_step
}

// Resource usage lists (resource[0] has value value[0])
func GetUsageList(v *InternalJobStatus, task int) (resource []string, value []string) {
	if len(v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist) > task {
		var scaled []JATScaledElements
		if len(v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist[task].JAT_scaled_usage_list.JAT_scaled) <= 0 {
			scaled = v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist[task].JAT_scaled_usage_list.JAT_scaled82
		} else {
			scaled = v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist[task].JAT_scaled_usage_list.JAT_scaled
		}
		for _, v := range scaled {
			resource = append(resource, v.UA_name)
			value = append(value, v.UA_value)
		}
	}
	return resource, value
}

// Returns pairs of name and value from hard resource requests (-hard -l mem=1G)
// -> "mem" "1G
func GetHardRequests(v *InternalJobStatus) (nm []string, val []string) {
	for _, name := range v.Jobinf.Element.Jb_hard_resource_request.HardResourceRequest {
		nm = append(nm, name.Name)
		val = append(val, name.StringVal)
	}
	return nm, val
}

// Amount of array job tasks
func GetTaskCount(v *InternalJobStatus) int {
	taskList := v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist
	taskList82 := v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist82
	return len(taskList) + len(taskList82)
}

// GDIL: Granted destination identifier list
func GetGDIL(v *InternalJobStatus, task int) *[]GDIL {
	var taskList []JBJaSublist

	taskList = v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist
	taskListSize := len(taskList)
	if taskListSize <= 0 {
		taskList = v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist82
		taskListSize = len(taskList)
	}

	if task < 0 || task >= taskListSize {
		// err
		return nil
	}

	gdil := make([]GDIL, len(taskList[task].JAT_granted_destin_identifier_list.GdilElement))

	for i, gdi := range taskList[task].JAT_granted_destin_identifier_list.GdilElement {
		gdil[i].FunctionalTicket = gdi.JG_fticket
		gdil[i].OverrideTicket = gdi.JG_oticket
		gdil[i].QueueName = gdi.JG_qname
		gdil[i].SharetreeTicket = gdi.JG_sticket
		gdil[i].Slots = gdi.JG_slots
		gdil[i].Ticket = gdi.JG_ticket
	}

	return &gdil
}

/* Public functions for library */
func parseXML(xmlOutput []byte) (ijs InternalJobStatus, err error) {
	err2 := xml.Unmarshal(xmlOutput, &ijs)

	if err2 != nil {
		//fmt.Println(err2)
		return ijs, fmt.Errorf("XML unmarshall error")
	}
	return ijs, nil
}

/* Parses plain qstat output for all jobs. */
func parseQstatXML(xmlOutput []byte) (qjl QstatJobInfoList, err error) {
	err2 := xml.Unmarshal(xmlOutput, &qjl)

	if err2 != nil {
		return qjl, fmt.Errorf("XML unmarshall error")
	}
	return qjl, nil
}

// Intenal function which creats an GO DRMAA error.
func makeError(msg string, id drmaa.ErrorId) drmaa.Error {
	var ce drmaa.Error
	ce.Message = msg
	ce.Id = id
	return ce
}

// Call this when XML job status is parsed without Go drmaa.
func GetJobStatusById(jobId string) (ijs InternalJobStatus, err error) {
	cmd := exec.Command("qstat", "-xml", "-j", jobId)
	out, err := cmd.Output()
	if err != nil {
		return ijs, err
	}
	return parseXML(out)
}

// for use within Go DRMAA (caches results)
func GetJobStatus(s *drmaa.Session, jobId string) (ijs InternalJobStatus, er error) {
	/* check status of job - could slow down qmaster when having lots of clients.
	   Grid Engine DRMAA2 implementation will solve this. */
	pt, err := s.JobPs(jobId)
	if err != nil {
		return ijs, err
	}

	if pt != drmaa.PsRunning {
		err2 := makeError("Job is not in running state.", drmaa.InvalidJob)
		return ijs, &err2
	}

	cjs, found := getValidJobStatusFromCache(jobId)
	if found == false {
		/* job is not cached yet or outdated - we need to fetch it */
		//qstat := fmt.Sprint("-xml -j ", jobId)
		cmd := exec.Command("qstat", "-xml", "-j", jobId)
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
		cachedJobStatus[jobId] = jobStatusCache{time.Now(), js}
		return js, nil
	}
	/* return cached job status */
	return cjs, nil
}

/* global job status cache */
type jobStatusCache struct {
	lastUpdate time.Time
	jobStatus  InternalJobStatus
}

/**
 * Get all information from qstat -xml for all running jobs by a particular
 * user.
 */
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
