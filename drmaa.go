/*
    Copyright 2012, 2013 Daniel Gruber, info@gridengine.eu

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

   This is a Go DRMAA language binding implementation version 0.5.

   Changelog:
   0.1 - First Release: Proof of Concept
   0.2 - Added methods for accessing JobInfo struct.
   0.3 - Minor cleanups
   0.4 - Added gestatus for detailed job status fetching with "qstat -xml"
   0.5 - Go DRMAA is now under BSD License (instead in the jail of GPL)!

   If you have questions or have problema contact me at info @ gridengine.eu.
*/

package drmaa

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// You can adapt LDFLAGS and CFLAGS below pointing to your
// Grid Engine (or other DRM) installation. ($SGE_ROOT/lib/<arch> and
// $SGE_ROOT/include). Then setting the variables in the build.sh
// would not be required anymore (go install would just work).

/*
 #cgo LDFLAGS: -ldrmaa
 #cgo CFLAGS: -O2 -g
 #include <stdio.h>
 #include <stdlib.h>
 #include <stddef.h>
 #include "drmaa.h"

static char* makeString(size_t size) {
   return calloc(sizeof(char), size);
}

static char** makeStringArray(const int size) {
   return calloc(sizeof(char*), size);
}

static void setString(char **a, char *s, const int n) {
   a[n] = s;
}

static void freeStringArray(char **a, const int size) {
   int i = 0;
   for (; i < size; i++)
      free(a[i]);
   free(a);
}

int _drmaa_get_num_attr_values(drmaa_attr_values_t* values, int *size) {
#ifdef TORQUE
    return drmaa_get_num_attr_values(values, (size_t *) size);
#else
    return drmaa_get_num_attr_values(values, size);
#endif
}
*/
import "C"

const version string = "0.6"

// default string size
const stringSize C.size_t = C.DRMAA_ERROR_STRING_BUFFER
const jobnameSize C.size_t = C.DRMAA_JOBNAME_BUFFER

// Placeholder for output directory when filling out job template.
const PLACEHOLDER_HOME_DIR string = "$drmaa_hd_ph$"
const PLACEHOLDER_WORKING_DIR string = "$drmaa_wd_ph$"
const PLACEHOLDER_TASK_ID string = "$drmaa_incr_ph$"

// Job state according to last query.
type PsType int

const (
	PsUndetermined PsType = iota
	PsQueuedActive
	PsSystemOnHold
	PsUserOnHold
	PsUserSystemOnHold
	PsRunning
	PsSystemSuspended
	PsUserSuspended
	PsUserSystemSuspended
	PsDone
	PsFailed
)

// Job state implements Stringer interface for simplified output.
func (pt PsType) String() string {
	switch pt {
	case PsUndetermined:
		return "Undetermined"
	case PsQueuedActive:
		return "QueuedActive"
	case PsSystemOnHold:
		return "SystemOnHold"
	case PsUserOnHold:
		return "UserOnHold"
	case PsUserSystemOnHold:
		return "UserSystemOnHold"
	case PsRunning:
		return "Running"
	case PsSystemSuspended:
		return "SystemSuspended"
	case PsUserSuspended:
		return "UserSuspended"
	case PsUserSystemSuspended:
		return "UserSystemSuspended"
	case PsDone:
		return "Done"
	case PsFailed:
		return "Failed"
	}
	return "UnknownDRMAAState"
}

// Initial job state when sub is submitted.
type SubmissionState int

const (
	HoldState SubmissionState = iota
	ActiveState
)

// Timeout is either a positive number in seconds or one of
// those constants.
const (
	TimeoutWaitForever int64 = -1
	TimeoutNoWait      int64 = 0
)

// Job controls for session.Control().
type controlType int

const (
	Suspend controlType = iota
	Resume
	Hold
	Release
	Terminate
)

// Job states.
type jobState int

const (
	Undetermined jobState = iota
	QueuedActive
	SystemOnHold
	UserOnHold
	UserSystemOnHold
	Running
	SystemSuspended
	UserSystemSuspended
	Done
	Failed
)

// DRMAA error IDs.
type ErrorId int

const (
	Success ErrorId = iota
	InternalError
	DrmCommunicationFailure
	AuthFailure
	InvalidArgument
	NoActiveSession
	NoMemory
	InvalidContactString
	DefaultContactStringError
	NoDefaultContactStringSelected
	DrmsInitFailed
	AlreadyActiveSession
	DrmsExitError
	InvalidAttributeFormat
	InvalidAttributeValue
	ConflictingAttributeValues
	TryLater
	DeniedByDrm
	InvalidJob
	ResumeInconsistentState
	SuspendInconsistentState
	HoldInconsistentState
	ReleaseInconsistentState
	ExitTimeout
	NoRusage
	NoMoreElements
)

// Internal map between C DRMAA error and Go DRMAA error.
var errorId = map[C.int]ErrorId{
	C.DRMAA_ERRNO_SUCCESS:                            Success,
	C.DRMAA_ERRNO_INTERNAL_ERROR:                     InternalError,
	C.DRMAA_ERRNO_DRM_COMMUNICATION_FAILURE:          DrmCommunicationFailure,
	C.DRMAA_ERRNO_AUTH_FAILURE:                       AuthFailure,
	C.DRMAA_ERRNO_INVALID_ARGUMENT:                   InvalidArgument,
	C.DRMAA_ERRNO_NO_ACTIVE_SESSION:                  NoActiveSession,
	C.DRMAA_ERRNO_NO_MEMORY:                          NoMemory,
	C.DRMAA_ERRNO_INVALID_CONTACT_STRING:             InvalidContactString,
	C.DRMAA_ERRNO_DEFAULT_CONTACT_STRING_ERROR:       DefaultContactStringError,
	C.DRMAA_ERRNO_NO_DEFAULT_CONTACT_STRING_SELECTED: NoDefaultContactStringSelected,
	C.DRMAA_ERRNO_DRMS_INIT_FAILED:                   DrmsInitFailed,
	C.DRMAA_ERRNO_ALREADY_ACTIVE_SESSION:             AlreadyActiveSession,
	C.DRMAA_ERRNO_DRMS_EXIT_ERROR:                    DrmsExitError,
	C.DRMAA_ERRNO_INVALID_ATTRIBUTE_FORMAT:           InvalidAttributeFormat,
	C.DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:            InvalidAttributeValue,
	C.DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:       ConflictingAttributeValues,
	C.DRMAA_ERRNO_TRY_LATER:                          TryLater,
	C.DRMAA_ERRNO_DENIED_BY_DRM:                      DeniedByDrm,
	C.DRMAA_ERRNO_INVALID_JOB:                        InvalidJob,
	C.DRMAA_ERRNO_RESUME_INCONSISTENT_STATE:          ResumeInconsistentState,
	C.DRMAA_ERRNO_SUSPEND_INCONSISTENT_STATE:         SuspendInconsistentState,
	C.DRMAA_ERRNO_HOLD_INCONSISTENT_STATE:            HoldInconsistentState,
	C.DRMAA_ERRNO_RELEASE_INCONSISTENT_STATE:         ReleaseInconsistentState,
	C.DRMAA_ERRNO_EXIT_TIMEOUT:                       ExitTimeout,
	C.DRMAA_ERRNO_NO_RUSAGE:                          NoRusage,
	C.DRMAA_ERRNO_NO_MORE_ELEMENTS:                   NoMoreElements,
}

// Internal map between GO DRMAA error und C DRMAA error.
var internalError = map[ErrorId]C.int{
	Success:                        C.DRMAA_ERRNO_SUCCESS,
	InternalError:                  C.DRMAA_ERRNO_INTERNAL_ERROR,
	DrmCommunicationFailure:        C.DRMAA_ERRNO_DRM_COMMUNICATION_FAILURE,
	AuthFailure:                    C.DRMAA_ERRNO_AUTH_FAILURE,
	InvalidArgument:                C.DRMAA_ERRNO_INVALID_ARGUMENT,
	NoActiveSession:                C.DRMAA_ERRNO_NO_ACTIVE_SESSION,
	NoMemory:                       C.DRMAA_ERRNO_NO_MEMORY,
	InvalidContactString:           C.DRMAA_ERRNO_INVALID_CONTACT_STRING,
	DefaultContactStringError:      C.DRMAA_ERRNO_DEFAULT_CONTACT_STRING_ERROR,
	NoDefaultContactStringSelected: C.DRMAA_ERRNO_NO_DEFAULT_CONTACT_STRING_SELECTED,
	DrmsInitFailed:                 C.DRMAA_ERRNO_DRMS_INIT_FAILED,
	AlreadyActiveSession:           C.DRMAA_ERRNO_ALREADY_ACTIVE_SESSION,
	DrmsExitError:                  C.DRMAA_ERRNO_DRMS_EXIT_ERROR,
	InvalidAttributeFormat:         C.DRMAA_ERRNO_INVALID_ATTRIBUTE_FORMAT,
	InvalidAttributeValue:          C.DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE,
	ConflictingAttributeValues:     C.DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES,
	TryLater:                       C.DRMAA_ERRNO_TRY_LATER,
	DeniedByDrm:                    C.DRMAA_ERRNO_DENIED_BY_DRM,
	InvalidJob:                     C.DRMAA_ERRNO_INVALID_JOB,
	ResumeInconsistentState:        C.DRMAA_ERRNO_RESUME_INCONSISTENT_STATE,
	SuspendInconsistentState:       C.DRMAA_ERRNO_SUSPEND_INCONSISTENT_STATE,
	HoldInconsistentState:          C.DRMAA_ERRNO_HOLD_INCONSISTENT_STATE,
	ReleaseInconsistentState:       C.DRMAA_ERRNO_RELEASE_INCONSISTENT_STATE,
	ExitTimeout:                    C.DRMAA_ERRNO_EXIT_TIMEOUT,
	NoRusage:                       C.DRMAA_ERRNO_NO_RUSAGE,
	NoMoreElements:                 C.DRMAA_ERRNO_NO_MORE_ELEMENTS,
}

// File transfer mode struct.
type FileTransferMode struct {
	ErrorStream  bool
	InputStream  bool
	OutputStream bool
}

// Job information struct.
type JobInfo struct {
	resourceUsage     map[string]string
	jobId             string
	hasExited         bool
	exitStatus        int64
	hasSignaled       bool
	terminationSignal string
	hasAborted        bool
	hasCoreDump       bool
}

// Retursn the resource usage as a map.
func (ji *JobInfo) ResourceUsage() map[string]string {
	return ji.resourceUsage
}

// Returns the job id as string.
func (ji *JobInfo) JobId() string {
	return ji.jobId
}

// Returns if the job has exited.
func (ji *JobInfo) HasExited() bool {
	return ji.hasExited
}

func (ji *JobInfo) ExitStatus() int64 {
	return ji.exitStatus
}

// Returns if the job has been signaled.
func (ji *JobInfo) HasSignaled() bool {
	return ji.hasSignaled
}

// Returns the termination signal of the job.
func (ji *JobInfo) TerminationSignal() string {
	return ji.terminationSignal
}

// Returns if the job was aborted.
func (ji *JobInfo) HasAborted() bool {
	return ji.hasAborted
}

// Returns if the job has generated a core dump.
func (ji *JobInfo) HasCoreDump() bool {
	return ji.hasCoreDump
}

// GO DRMAA error (implements GO Error interface).
type Error struct {
	Message string
	Id      ErrorId
}

// Intenal function which creats an GO DRMAA error.
func makeError(msg string, id ErrorId) Error {
	var ce Error
	ce.Message = msg
	ce.Id = id
	return ce
}

// A GO DRMAA error implements GO error interface.
func (ce Error) Error() string {
	return ce.Message
}

// DRMAA functions (not methods required)

// Maps an ErrorId to an error string.
func StrError(id ErrorId) string {
	// check if not found
	var ie C.int
	ie = internalError[id]

	errStr := C.drmaa_strerror(ie)
	return C.GoString(errStr)
}

// Get contact string.
func GetContact() (string, *Error) {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	contact := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(contact))

	errNumber := C.drmaa_get_contact(contact, stringSize, diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return "", &ce
	}
	return C.GoString(contact), nil
}

// Get the version of the DRMAA standard.
func GetVersion() (int, int, *Error) {
	return 1, 0, nil
}

// Get the DRM system.
func (s *Session) GetDrmSystem() (string, *Error) {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	drm := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(drm))

	errNumber := C.drmaa_get_DRM_system(drm, stringSize,
		diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return "", &ce
	}

	return C.GoString(drm), nil
}

// Get information about the DRMAA implementation.
func (s *Session) GetDrmaaImplementation() string {
	return "GO DRMAA Implementation by Daniel Gruber Version 0.2"
}

// A DRMAA session.
type Session struct {
	// go internal
	initialized bool
}

// Creates and initializes a new DRMAA session.
func MakeSession() (Session, *Error) {
	var session Session
	if err := session.Init(""); err != nil {
		return session, err
	}
	return session, nil
}

// Initializes a DRMAA session. If contact string is ""
// a new session is created otherwise an existing session
// is connected.
func (s *Session) Init(contactString string) *Error {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))
	var errNumber C.int

	if contactString == "" {
		errNumber = C.drmaa_init(nil, diag, stringSize)
	} else {
		csp := C.CString(contactString)
		defer C.free(unsafe.Pointer(csp))
		errNumber = C.drmaa_init(csp, diag, stringSize)
	}

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		// convert ERROR string back
		s.initialized = false
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return &ce
	} else {
		s.initialized = true
	}
	return nil
}

// Disengages a session frmo the DRMAA library and cleans it up.
func (s *Session) Exit() *Error {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	errNumber := C.drmaa_exit(diag, stringSize)
	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return &ce
	}
	return nil
}

// Allocates a new job template.
func (s *Session) AllocateJobTemplate() (jt JobTemplate, err *Error) {
	if s.initialized == false {
		// error, need a connection (active session)
		ce := makeError("No active session", NoActiveSession)
		return jt, &ce
	}
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	//var jtp *C.drmaa_job_template_t
	errNumber := C.drmaa_allocate_job_template(&jt.jt, diag, stringSize)
	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return jt, &ce
	}
	return jt, nil
}

// Deletes (and frees memory) of an allocated job template.
// Must be called in to prevent memory leaks. JobTemplates
// are not handled in GO garbage collector.
func (s *Session) DeleteJobTemplate(jt *JobTemplate) *Error {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	// de-allocate job template in memory
	errNumber := C.drmaa_delete_job_template(jt.jt, diag, stringSize)
	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return &ce
	}
	return nil
}

// Submits a job in a (initialized) session to the DRM.
func (s *Session) RunJob(jt *JobTemplate) (string, *Error) {
	jobId := C.makeString(jobnameSize)
	defer C.free(unsafe.Pointer(jobId))

	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	errNumber := C.drmaa_run_job(jobId, jobnameSize, jt.jt, diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return "", &ce
	}
	return C.GoString(jobId), nil
}

// Submits a job as an array job.
func (s *Session) RunBulkJobs(jt *JobTemplate, start, end, incr int) ([]string, *Error) {
	var ids *C.drmaa_job_ids_t
	jobId := C.makeString(jobnameSize)
	defer C.free(unsafe.Pointer(jobId))

	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	errNumber := C.drmaa_run_bulk_jobs(&ids, jt.jt, C.int(start), C.int(end), C.int(incr),
		diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return nil, &ce
	}

	// collect job ids
	var jobIds []string
	for C.drmaa_get_next_job_id(ids, jobId, C.DRMAA_JOBNAME_BUFFER) == C.DRMAA_ERRNO_SUCCESS {
		jobIds = append(jobIds, C.GoString(jobId))
	}

	return jobIds, nil
}

// Controls a job, i.e. terminates, suspends, resumes a job or sets
// it in a the hold state or release it from the hold state.
func (s *Session) Control(jobId string, action controlType) *Error {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))
	var ca C.int

	switch action {
	case Terminate:
		ca = C.DRMAA_CONTROL_TERMINATE
	case Suspend:
		ca = C.DRMAA_CONTROL_SUSPEND
	case Resume:
		ca = C.DRMAA_CONTROL_RESUME
	case Hold:
		ca = C.DRMAA_CONTROL_HOLD
	case Release:
		ca = C.DRMAA_CONTROL_RELEASE
	}

	if errNumber := C.drmaa_control(C.CString(jobId), ca, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return &ce
	}
	return nil
}

// make job control more straightforward

// Simple wrapper for Control(jobId, Terminate).
func (s *Session) TerminateJob(jobId string) *Error {
	return s.Control(jobId, Terminate)
}

// Simple wrapper for Control(jobId, Suspend).
func (s *Session) SuspendJob(jobId string) *Error {
	return s.Control(jobId, Suspend)
}

// Simple wrapper for Control(jobId, Resume).
func (s *Session) ResumeJob(jobId string) *Error {
	return s.Control(jobId, Resume)
}

// Simple wrapper for Control(jobId, Hold).
func (s *Session) HoldJob(jobId string) *Error {
	return s.Control(jobId, Hold)
}

// Simple wrapper for Control(jobId, Release).
func (s *Session) ReleaseJob(jobId string) *Error {
	return s.Control(jobId, Release)
}

// Blocks the the programm until the given jobs left the system or
// a specific timeout is reached.
func (s *Session) Synchronize(jobIds []string, timeout int64, dispose bool) *Error {
	// TODO handle special string: DRMAA_ID_SESSION_ALL
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	job_ids := C.makeStringArray(C.int(len(jobIds) + 1))

	for i, jobid := range jobIds {
		C.setString(job_ids, C.CString(jobid), C.int(i))
	}
	C.setString(job_ids, nil, C.int(len(jobIds)))

	var disp C.int
	if dispose {
		disp = C.int(1)
	} else {
		disp = C.int(0)
	}

	if errNumber := C.drmaa_synchronize(job_ids, C.long(timeout), disp,
		diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return &ce
	}

	return nil
}

// Blocks until the job left the DRM system or a timeout is reached and
// returns a JobInfo structure.
func (s *Session) Wait(jobId string, timeout int64) (jobinfo JobInfo, err *Error) {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	job_id_out := C.makeString(jobnameSize)
	defer C.free(unsafe.Pointer(job_id_out))

	// out
	cstat := C.int(0)
	var crusage *C.struct_drmaa_attr_values_s

	if errNumber := C.drmaa_wait(C.CString(jobId), job_id_out, jobnameSize, &cstat,
		C.long(timeout), &crusage, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return jobinfo, &ce
	}

	// fill JobInfo struct
	exited := C.int(0)
	if errNumber := C.drmaa_wifexited(&exited, cstat, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return jobinfo, &ce
	}
	if exited == 0 {
		jobinfo.hasExited = false
	} else {
		jobinfo.hasExited = true
		// set exit status
		exitstatus := C.int(0)
		if errNumber := C.drmaa_wexitstatus(&exitstatus, cstat, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
			C.drmaa_release_attr_values(crusage)
			ce := makeError(C.GoString(diag), errorId[errNumber])
			return jobinfo, &ce
		}
		jobinfo.exitStatus = int64(exitstatus)
	}

	signaled := C.int(0)
	if errNumber := C.drmaa_wifsignaled(&signaled, cstat, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		C.drmaa_release_attr_values(crusage)
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return jobinfo, &ce
	}
	if signaled == 0 {
		jobinfo.hasSignaled = false
	} else {
		jobinfo.hasSignaled = true
		termsig := C.makeString(stringSize)
		defer C.free(unsafe.Pointer(termsig))
		if errNumber := C.drmaa_wtermsig(termsig, stringSize, cstat, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
			C.drmaa_release_attr_values(crusage)
			ce := makeError(C.GoString(diag), errorId[errNumber])
			return jobinfo, &ce
		}
		jobinfo.terminationSignal = C.GoString(termsig)
	}

	aborted := C.int(0)
	if errNumber := C.drmaa_wifsignaled(&aborted, cstat, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		C.drmaa_release_attr_values(crusage)
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return jobinfo, &ce
	}
	if aborted == 0 {
		jobinfo.hasAborted = false
	} else {
		jobinfo.hasAborted = true
	}

	coreDumped := C.int(0)
	if errNumber := C.drmaa_wcoredump(&coreDumped, cstat, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		C.drmaa_release_attr_values(crusage)
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return jobinfo, &ce
	}
	if coreDumped == 0 {
		jobinfo.hasCoreDump = false
	} else {
		jobinfo.hasCoreDump = true
	}

	jobinfo.jobId = C.GoString(job_id_out)

	// rusage
	usageLength := C.int(0)
	if errNumber := C._drmaa_get_num_attr_values(crusage, &usageLength); errNumber != C.DRMAA_ERRNO_SUCCESS {
		C.drmaa_release_attr_values(crusage)
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return jobinfo, &ce
	}

	usageArray := make([]string, 0)
	for i := 0; i < int(usageLength); i++ {
		usage := C.makeString(stringSize)
		defer C.free(unsafe.Pointer(usage))
		if C.drmaa_get_next_attr_value(crusage, usage, stringSize) == C.DRMAA_ERRNO_SUCCESS {
			usageArray = append(usageArray, C.GoString(usage))
		}
	}
	C.drmaa_release_attr_values(crusage)
	// make a map out of the array
	usageMap := make(map[string]string)
	for i := range usageArray {
		nameVal := strings.Split(usageArray[i], "=")
		usageMap[nameVal[0]] = nameVal[1]
	}
	jobinfo.resourceUsage = usageMap

	return jobinfo, nil
}

// Returns the state of a job.
func (s *Session) JobPs(jobId string) (PsType, *Error) {
	outPs := C.int(0)
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	if errNumber := C.drmaa_job_ps(C.CString(jobId), &outPs, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return 0, &ce
	}

	var psType PsType

	switch outPs {
	case C.DRMAA_PS_UNDETERMINED:
		psType = PsUndetermined
	case C.DRMAA_PS_QUEUED_ACTIVE:
		psType = PsQueuedActive
	case C.DRMAA_PS_SYSTEM_ON_HOLD:
		psType = PsSystemOnHold
	case C.DRMAA_PS_USER_ON_HOLD:
		psType = PsUserOnHold
	case C.DRMAA_PS_USER_SYSTEM_ON_HOLD:
		psType = PsUserSystemOnHold
	case C.DRMAA_PS_RUNNING:
		psType = PsRunning
	case C.DRMAA_PS_SYSTEM_SUSPENDED:
		psType = PsSystemSuspended
	case C.DRMAA_PS_USER_SUSPENDED:
		psType = PsUserSuspended
	case C.DRMAA_PS_USER_SYSTEM_SUSPENDED:
		psType = PsUserSystemSuspended
	case C.DRMAA_PS_DONE:
		psType = PsDone
	case C.DRMAA_PS_FAILED:
		psType = PsFailed
	}

	return psType, nil
}

type JobTemplate struct {
	// reference to C job template
	jt *C.drmaa_job_template_t
}

// TODO Args() is a mission method from Job Template

// String implements the Stringer interface for the JobTemplate.
// Note that this operation is not very efficient since it needs
// to get all values out of the C object.
func (jt *JobTemplate) String() string {
	var s string

	rc, _ := jt.RemoteCommand()
	jn, _ := jt.JobName()
	ns, _ := jt.NativeSpecification()
	op, _ := jt.OutputPath()
	ip, _ := jt.InputPath()
	wd, _ := jt.WD()
	jf, _ := jt.JoinFiles()            // FileTransferMode
	js, _ := jt.JobSubmissionState()   // SubmissionState
	st, _ := jt.StartTime()            // time
	sr, _ := jt.SoftRunDurationLimit() // duration
	hr, _ := jt.HardRunDurationLimit()
	ep, _ := jt.ErrorPath()
	be, _ := jt.BlockEmail()   // bool
	dl, _ := jt.DeadlineTime() // time

	s = fmt.Sprintf("Remote command: %s\nJob name: %s\nNative specification: %s\nOutput path: %s\nInput path: %s\nWorking directory: %s\nJoin files: %b\nSubmission state: %s\nStart time: %s\nSoft run duration limit: %s\nHard run duration limit: %s\nError path: %s\nBlock email: %b\nDeadline time: %s",
		rc, jn, ns, op, ip, wd, jf, js, st, sr, hr, ep, be, dl)

	return s
}

// private JOB TEMPLATE helpers
func setNameValue(jt *C.drmaa_job_template_t, name string, value string) *Error {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))

	v := C.CString(value)
	defer C.free(unsafe.Pointer(v))

	errNumber := C.drmaa_set_attribute(jt, n, v, diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return &ce
	}
	return nil
}

func getStringValue(jt *C.drmaa_job_template_t, name string) (string, *Error) {
	if jt == nil {
		ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
		return "", &ce
	}

	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))

	v := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(v))

	errNumber := C.drmaa_get_attribute(jt, n, v, stringSize,
		diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return "", &ce
	}

	return C.GoString(v), nil
}

// JOB TEMPLATE methods

// Sets the name of the binary to start in the job template.
func (jt *JobTemplate) SetRemoteCommand(cmd string) *Error {
	if jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_REMOTE_COMMAND, cmd)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// Returns the currently set binary.
func (jt *JobTemplate) RemoteCommand() (string, *Error) {
	return getStringValue(jt.jt, C.DRMAA_REMOTE_COMMAND)
}

// Sets the input path of the remote command in the job template.
func (jt *JobTemplate) SetInputPath(path string) *Error {
	if jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_INPUT_PATH, path)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// Returns the set input path ot the remote command in the job template.
func (jt *JobTemplate) InputPath() (string, *Error) {
	return getStringValue(jt.jt, C.DRMAA_INPUT_PATH)
}

func (jt *JobTemplate) SetOutputPath(path string) *Error {
	if jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_OUTPUT_PATH, path)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

func (jt *JobTemplate) OutputPath() (string, *Error) {
	return getStringValue(jt.jt, C.DRMAA_OUTPUT_PATH)
}

func (jt *JobTemplate) SetErrorPath(path string) *Error {
	if jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_ERROR_PATH, path)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

func (jt *JobTemplate) ErrorPath() (string, *Error) {
	return getStringValue(jt.jt, C.DRMAA_ERROR_PATH)
}

// vector attributes
func setVectorAttributes(jt *JobTemplate, name *C.char, args []string) *Error {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	//name := C.CString(C.DRMAA_V_ENV)
	//defer C.free(unsafe.Pointer(name))

	values := C.makeStringArray(C.int(len(args) + 1))
	defer C.freeStringArray(values, C.int(len(args)+1))

	for i, a := range args {
		C.setString(values, C.CString(a), C.int(i))
	}

	errNumber := C.drmaa_set_vector_attribute(jt.jt, name, values, diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return &ce
	}
	return nil
}

func (jt *JobTemplate) SetArgs(args []string) *Error {
	if jt.jt != nil {
		drmaa_v_argv := C.CString(C.DRMAA_V_ARGV)
		return setVectorAttributes(jt, drmaa_v_argv, args)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// Set single argument. Simple wrapper for SetArgs([]string{arg}).
func (jt *JobTemplate) SetArg(arg string) *Error {
	return jt.SetArgs([]string{arg})
}

func (jt *JobTemplate) SetEnv(envs []string) *Error {
	if jt.jt != nil {
		drmaa_v_env := C.CString(C.DRMAA_V_ENV)
		return setVectorAttributes(jt, drmaa_v_env, envs)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

func (jt *JobTemplate) SetEmail(emails []string) *Error {
	if jt.jt != nil {
		drmaa_v_email := C.CString(C.DRMAA_V_EMAIL)
		return setVectorAttributes(jt, drmaa_v_email, emails)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// template attributes
func (jt *JobTemplate) SetJobSubmissionState(state SubmissionState) *Error {
	if jt.jt != nil {
		drmaa_js_state := C.DRMAA_JS_STATE
		if state == HoldState {
			return setNameValue(jt.jt, drmaa_js_state, "drmaa_hold")
		} else { //if (state == ActiveState) {
			return setNameValue(jt.jt, drmaa_js_state, "drmaa_active")
		}
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

func (jt *JobTemplate) JobSubmissionState() (SubmissionState, *Error) {
	value, err := getStringValue(jt.jt, C.DRMAA_JS_STATE)
	if err != nil {
		return ActiveState, err
	}

	if value == "drmaa_hold" {
		return HoldState, nil
	} //else { // if value == "drmaa_active" {

	return ActiveState, nil
}

func (jt *JobTemplate) SetWD(dir string) *Error {
	if jt.jt != nil {
		drmaa_wd := C.DRMAA_WD
		return setNameValue(jt.jt, drmaa_wd, dir)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// Gets the working directory set in the job template
func (jt *JobTemplate) WD() (string, *Error) {
	return getStringValue(jt.jt, C.DRMAA_WD)
}

// Sets the native specification (DRM system depended job submission settings)
// for the job.
func (jt *JobTemplate) SetNativeSpecification(native string) *Error {
	if jt.jt != nil {
		drmaa_native_specification := C.DRMAA_NATIVE_SPECIFICATION
		return setNameValue(jt.jt, drmaa_native_specification, native)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// Gets the native specificatio set in the job template.
func (jt *JobTemplate) NativeSpecification() (string, *Error) {
	return getStringValue(jt.jt, C.DRMAA_NATIVE_SPECIFICATION)
}

func (jt *JobTemplate) SetBlockEmail(blockmail bool) *Error {
	if jt.jt != nil {
		drmaa_block_email := C.DRMAA_BLOCK_EMAIL
		if blockmail {
			return setNameValue(jt.jt, drmaa_block_email, "1")
		} else {
			return setNameValue(jt.jt, drmaa_block_email, "0")
		}
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

func (jt *JobTemplate) BlockEmail() (bool, *Error) {
	value, err := getStringValue(jt.jt, C.DRMAA_BLOCK_EMAIL)
	if err != nil {
		return false, err
	}
	if value == "1" {
		return true, nil
	}
	return false, nil
}

// SetStartTime sets the earliest job start time for the job.
func (jt *JobTemplate) SetStartTime(time time.Time) *Error {
	if jt.jt != nil {
		drmaa_start_time := C.DRMAA_START_TIME
		timeString := fmt.Sprintf("%d", time.Unix())
		return setNameValue(jt.jt, drmaa_start_time, timeString)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// StartTime returns the job start time set for the job.
func (jt *JobTemplate) StartTime() (time.Time, *Error) {
	if value, err := getStringValue(jt.jt, C.DRMAA_START_TIME); err != nil {
		var t time.Time
		return t, err
	} else {
		sec, err := strconv.ParseInt(value, 10, 64)
		if t := time.Unix(sec, 0); err == nil {
			return t, nil
		} else {
			ce := makeError("Unknown timestamp", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
			var t time.Time
			return t, &ce
		}
	}
	// unreachable code
	var t time.Time
	return t, nil
}

// SetJobName sets the name of the job in the job template.
func (jt *JobTemplate) SetJobName(jobname string) *Error {
	if jt.jt != nil {
		drmaa_job_name := C.DRMAA_JOB_NAME
		return setNameValue(jt.jt, drmaa_job_name, jobname)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// JobName returns the name set in the job template.
func (jt *JobTemplate) JobName() (string, *Error) {
	return getStringValue(jt.jt, C.DRMAA_JOB_NAME)
}

// SetJoinFiles sets that the error and output files have to be joined.
func (jt *JobTemplate) SetJoinFiles(join bool) *Error {
	if jt.jt != nil {
		drmaa_join_files := C.DRMAA_JOIN_FILES
		if join {
			return setNameValue(jt.jt, drmaa_join_files, "y")
		} else {
			return setNameValue(jt.jt, drmaa_join_files, "n")
		}
	} else {
		ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
		return &ce
	}
	return nil
}

// JoinFiles returns if join files is set in the job template.
func (jt *JobTemplate) JoinFiles() (bool, *Error) {
	if val, err := getStringValue(jt.jt, C.DRMAA_JOB_NAME); err != nil {
		return false, err
	} else {
		if val == "y" {
			return true, nil
		}
	}
	return false, nil
}

// SetTransferFiles sets the file transfer mode in the job template.
func (jt *JobTemplate) SetTransferFiles(mode FileTransferMode) *Error {
	if jt.jt != nil {
		var ftm string
		if mode.InputStream {
			ftm += "i"
		}
		if mode.OutputStream {
			ftm += "o"
		}
		if mode.ErrorStream {
			ftm += "e"
		}
		drmaa_transfer_files := C.DRMAA_TRANSFER_FILES
		return setNameValue(jt.jt, drmaa_transfer_files, ftm)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// TransferFiles returns the FileTransferModes set in the job template.
func (jt *JobTemplate) TransferFiles() (FileTransferMode, *Error) {
	if jt.jt != nil {
		if val, err := getStringValue(jt.jt, C.DRMAA_TRANSFER_FILES); err != nil {
			var ftm FileTransferMode
			return ftm, err
		} else {
			var ftm FileTransferMode
			for _, c := range val {
				switch string(c) {
				case "i":
					ftm.InputStream = true
				case "o":
					ftm.OutputStream = true
				case "e":
					ftm.ErrorStream = true
				}
			}
			return ftm, nil
		}

	}
	var ftm FileTransferMode
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return ftm, &ce
}

// SetDeadlineTime sets deadline time in job template. Unsupported in Grid Engine.
func (jt *JobTemplate) SetDeadlineTime(deadline time.Duration) *Error {
	if jt.jt != nil {
		drmaa_deadline_time := C.DRMAA_DEADLINE_TIME
		limitString := fmt.Sprintf("%d", int64(deadline.Seconds()))
		return setNameValue(jt.jt, drmaa_deadline_time, limitString)
	} else {
		ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
		return &ce
	}
	return nil
}

// Unsupported in Grid Engine.
func (jt *JobTemplate) parseDuration(field string) (defaultDuration time.Duration, err *Error) {
	if jt.jt != nil {
		if val, err := getStringValue(jt.jt, field); err != nil {
			return defaultDuration, err
		} else {
			if sec, err := time.ParseDuration(val + "s"); err == nil {
				return sec, nil
			} else {
				ce := makeError("Couldn't parse duration.", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
				var t time.Duration
				return t, &ce
			}
		}
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return defaultDuration, &ce
}

// DeadlineTime returns deadline time. Unsupported in Grid Engine.
func (jt *JobTemplate) DeadlineTime() (deadlineTime time.Duration, err *Error) {
	return jt.parseDuration(C.DRMAA_DEADLINE_TIME)
}

// SetHardWallclockTimeLimit sets a hard wall-clock time limit for the job.
func (jt *JobTemplate) SetHardWallclockTimeLimit(limit time.Duration) *Error {
	if jt.jt != nil {
		drmaa_wct_hlimit := C.DRMAA_WCT_HLIMIT
		limitString := fmt.Sprintf("%d", int64(limit.Seconds()))
		return setNameValue(jt.jt, drmaa_wct_hlimit, limitString)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// HardWallclockTimeLimit returns the wall-clock time set in the job template.
func (jt *JobTemplate) HardWallclockTimeLimit() (deadlineTime time.Duration, err *Error) {
	return jt.parseDuration(C.DRMAA_WCT_HLIMIT)
}

// SetSoftWallclockTimeLimit sets a soft wall-clock time limit for the job in the job template.
func (jt *JobTemplate) SetSoftWallclockTimeLimit(limit time.Duration) *Error {
	if jt.jt != nil {
		drmaa_wct_slimit := C.DRMAA_WCT_SLIMIT
		limitString := fmt.Sprintf("%d", int64(limit.Seconds()))
		return setNameValue(jt.jt, drmaa_wct_slimit, limitString)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// SoftWallclockTimeLimit returns the soft wall-clock time limit for the job set in the job template.
func (jt *JobTemplate) SoftWallclockTimeLimit() (deadlineTime time.Duration, err *Error) {
	return jt.parseDuration(C.DRMAA_WCT_HLIMIT)
}

// SetHardRunDurationLimit sets a hard run-duration limit for the job in the job tempplate.
func (jt *JobTemplate) SetHardRunDurationLimit(limit time.Duration) *Error {
	if jt.jt != nil {
		drmaa_duration_hlimit := C.DRMAA_DURATION_HLIMIT
		limitString := fmt.Sprintf("%d", int64(limit.Seconds()))
		return setNameValue(jt.jt, drmaa_duration_hlimit, limitString)
	} else {
		ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
		return &ce
	}
	return nil
}

// HardRunDurationLimit returns the hard run-duration limit for the job in the job template.
func (jt *JobTemplate) HardRunDurationLimit() (deadlineTime time.Duration, err *Error) {
	return jt.parseDuration(C.DRMAA_DURATION_HLIMIT)
}

// SetSoftRunDurationLimit sets the soft run duration limit for the job in the job template.
func (jt *JobTemplate) SetSoftRunDurationLimit(limit time.Duration) *Error {
	if jt.jt != nil {
		drmaa_duration_slimit := C.DRMAA_DURATION_SLIMIT
		limitString := fmt.Sprintf("%d", int64(limit.Seconds()))
		return setNameValue(jt.jt, drmaa_duration_slimit, limitString)
	} else {
		ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
		return &ce
	}
	return nil
}

// SoftRunDurationLimit returns the soft run duration limit set in the job template.
func (jt *JobTemplate) SoftRunDurationLimit() (deadlineTime time.Duration, err *Error) {
	return jt.parseDuration(C.DRMAA_DURATION_SLIMIT)
}
