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

package private_gestatus

/* Outsoure the XML parsing to a separate file,
   because the XML structs are exported but should not
   be visible for the user.
*/
import (
	"github.com/dgruber/drmaa"
	"encoding/xml"
	"fmt"
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

type JATScaledUsageList struct {
	JAT_scaled []JATScaledElements `xml:"scaled"`
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
	GdilElement GDILElement `xml:"element"`
}

type JBJaSublist struct {
	JAT_status                         int                `xml:"JAT_status"`
	JAT_task_number                    int                `xml:"JAT_task_number"`
	JAT_scaled_usage_list              JATScaledUsageList `xml:"JAT_scaled_usage_list"`
	JAT_start_time                     int64              `xml:"JAT_start_time"`
	JAT_granted_destin_identifier_list []JATGDIL          `xml:"JAT_granted_destin_identifier_list"`
}

type JBJaTasks struct {
	JaTaskSublist []JBJaSublist `xml:"ulong_sublist"`
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
// XML of q"stat -xml" without parameters
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
	return time.Unix((int64)(st), 0)
}

func GetExecutionTime(v *InternalJobStatus) time.Time {
	return time.Unix(v.Jobinf.Element.Jb_execution_time, 0)
}

// start time of first task started
func GetStartTime(v *InternalJobStatus) time.Time {
	return GetTaskStartTime(v, 0)
}

// get start time of specific task
func GetTaskStartTime(v *InternalJobStatus, taskId int) time.Time {
	size := len(v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist)

	if taskId >= size || taskId < 0 {
		// error taskId not in range
		return time.Unix(0, 0)
	}

	startTime := v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist[taskId].JAT_start_time
	return time.Unix(startTime, 0)
}

func GetDeadline(v *InternalJobStatus) time.Time {
	return time.Unix(v.Jobinf.Element.Jb_deadline, 0)
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

func GetTaskCount(v *InternalJobStatus) int {
	taskList := v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist
	return len(taskList)
}

// GDIL
func GetGDIL(v *InternalJobStatus, task int) *[]GDIL {
	var taskList []JBJaSublist

	taskList = v.Jobinf.Element.Jb_ja_tasks.JaTaskSublist
	taskListSize := len(taskList)

	if task < 0 || task >= taskListSize {
		// err
		return nil
	}

	gdil := make([]GDIL, len(taskList[task].JAT_granted_destin_identifier_list))

	for i, gdi := range taskList[task].JAT_granted_destin_identifier_list {
		gdil[i].FunctionalTicket = gdi.GdilElement.JG_fticket
		gdil[i].OverrideTicket = gdi.GdilElement.JG_oticket
		gdil[i].QueueName = gdi.GdilElement.JG_qname
		gdil[i].SharetreeTicket = gdi.GdilElement.JG_sticket
		gdil[i].Slots = gdi.GdilElement.JG_slots
		gdil[i].Ticket = gdi.GdilElement.JG_ticket
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

func GetJobStatus(s *drmaa.Session, jobId string) (ijs InternalJobStatus, er *drmaa.Error) {
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

	if cjs, found := getValidJobStatusFromCache(jobId); found == false {
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
	} else {
		/* return cached job status */
		return cjs, nil
	}

	/* unreachable */
	return ijs, nil
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
