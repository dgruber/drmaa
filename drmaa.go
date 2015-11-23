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
   0.6 - Smaller issues
   0.7 - Changed *Error results to error. Old implementation is in DRMAA_OLD_ERROR branch.
   0.7b - Added sudo functionality supported only by Univa Grid Engine. Not DRMAAA specific.

   If you have questions or have problems contact me at info @ gridengine.eu.
*/

package drmaa

import (
	"errors"
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
 #include <string.h>
 #include "drmaa.h"

static drmaa_sudo_t * makeSudo(char *uname, char *gname, long uid, long gid) {
	drmaa_sudo_t * sudo = (drmaa_sudo_t *)calloc(sizeof(drmaa_sudo_t), 1);
	strncpy(sudo->username, uname, strlen(uname) + 1);
	strncpy(sudo->groupname, gname, strlen(gname) + 1);
	sudo->uid = (uid_t) uid;
	sudo->gid = (gid_t) gid;
	return sudo;
}

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

const version string = "0.7_sudo"

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

// String implements Stringer interface for simplified output of
// the job state (PsType).
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

// SubmissionState is the initial job state when the job
// is submitted.
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

// Sudo is a sudoers requrest in order to submit a job on behalf
// of another user. This is not part of the DRMAA spec but it is
// included in Univa Grid Engine's DRMAA implementation since 8.3 FCS.
type Sudo struct {
	Username  string
	Groupname string
	UID       int
	GID       int
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

// ResourceUsage returns the resource usage as a map.
func (ji *JobInfo) ResourceUsage() map[string]string {
	return ji.resourceUsage
}

// JobId returns the job id as string.
func (ji *JobInfo) JobId() string {
	return ji.jobId
}

// HasExited returns if the job has exited.
func (ji *JobInfo) HasExited() bool {
	return ji.hasExited
}

// ExitStatus returns the exit status of the job.
func (ji *JobInfo) ExitStatus() int64 {
	return ji.exitStatus
}

// HasSignaled returns if the job has been signaled.
func (ji *JobInfo) HasSignaled() bool {
	return ji.hasSignaled
}

// TerminationSeignal returns the termination signal of the job.
func (ji *JobInfo) TerminationSignal() string {
	return ji.terminationSignal
}

// HasAborted returns if the job was aborted.
func (ji *JobInfo) HasAborted() bool {
	return ji.hasAborted
}

// HasCoreDump returns if the job has generated a core dump.
func (ji *JobInfo) HasCoreDump() bool {
	return ji.hasCoreDump
}

// GO DRMAA error (implements GO Error interface).
// Each external error can be casted to a pointer
// to that struct in order to get more information
// about the error (the error id).
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

// GetContact returns the contact string.
func GetContact() (string, error) {
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

// GetVersion returns the version of the DRMAA standard.
func GetVersion() (int, int, error) {
	return 1, 0, nil
}

// GetDrmSystem returns the DRM system.
func (s *Session) GetDrmSystem() (string, error) {
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

// GetDrmaaImplementation returns information about the DRMAA implementation.
func (s *Session) GetDrmaaImplementation() string {
	return "GO DRMAA Implementation by Daniel Gruber Version 0.2"
}

// A DRMAA session.
type Session struct {
	// go internal
	initialized bool
}

// MakeSession creates and initializes a new DRMAA session.
func MakeSession() (Session, error) {
	var session Session
	if err := session.Init(""); err != nil {
		return session, err
	}
	return session, nil
}

// Init intitializes a DRMAA session. If contact string is ""
// a new session is created otherwise an existing session
// is connected.
func (s *Session) Init(contactString string) error {
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

// Exit disengages a session frmo the DRMAA library and cleans it up.
func (s *Session) Exit() error {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	errNumber := C.drmaa_exit(diag, stringSize)
	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return &ce
	}
	return nil
}

// AllocateJobTemplate allocates a new C drmaa job template.
// On successful allocation the DeleteJobTemplate() method
// must be called in order to avoid memory leaks.
func (s *Session) AllocateJobTemplate() (jt JobTemplate, err error) {
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

// DeleteJobTemplate delets (and frees memory) of an allocated job template.
// Must be called in to prevent memory leaks. JobTemplates
// are not handled in Go garbage collector.
func (s *Session) DeleteJobTemplate(jt *JobTemplate) error {
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

func sudoToC(delegate Sudo) *C.drmaa_sudo_t {
	if len(delegate.Username) <= 128 && len(delegate.Groupname) <= 128 {
		uname := C.CString(delegate.Username)
		defer C.free(unsafe.Pointer(uname))
		gname := C.CString(delegate.Groupname)
		defer C.free(unsafe.Pointer(gname))
		return C.makeSudo(uname, gname, C.long(delegate.UID), C.long(delegate.GID))
	}
	return nil
}

// RunJobAs submits a job in a (initialized) session to the cluster scheduler
// and executes the job as the user given by the Sudo structure. Note that
// this might not be allowed when the user has not the priviledges in the
// DRM. This is not a DRMAA standardized function!
func (s *Session) RunJobAs(delegate Sudo, jt *JobTemplate) (string, error) {
	jobId := C.makeString(jobnameSize)
	defer C.free(unsafe.Pointer(jobId))

	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	as := sudoToC(delegate)
	if as == nil {
		return "", errors.New("Couldn't convert sudo request.")
	}
	defer C.free(unsafe.Pointer(as))
	errNumber := C.drmaa_run_job_as(as, jobId, jobnameSize, jt.jt, diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return "", &ce
	}
	return C.GoString(jobId), nil
}

// RunJob submits a job in a (initialized) session to the cluster scheduler.
func (s *Session) RunJob(jt *JobTemplate) (string, error) {
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

// RunBulkJobsAs submits a job as an array job on behalf of the user
// given by the Sudo structure. Note that this is not
func (s *Session) RunBulkJobsAs(delegate Sudo, jt *JobTemplate, start, end, incr int) ([]string, error) {
	var ids *C.drmaa_job_ids_t
	jobId := C.makeString(jobnameSize)
	defer C.free(unsafe.Pointer(jobId))

	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	as := sudoToC(delegate)
	if as == nil {
		return nil, errors.New("Could not create sudo structure.")
	}
	defer C.free(unsafe.Pointer(as))
	errNumber := C.drmaa_run_bulk_jobs_as(as, &ids, jt.jt, C.int(start), C.int(end), C.int(incr),
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

// RunBulkJobs submits a job as an array job.
func (s *Session) RunBulkJobs(jt *JobTemplate, start, end, incr int) ([]string, error) {
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

// ControlAs sends a job modification request, i.e. terminates, suspends,
// resumes a job or sets it in a the hold state or release it from the
// job hold state. The given Sudo structure will make the call as the
// encoded user when allowed.
func (s *Session) ControlAs(delegate Sudo, jobId string, action controlType) error {
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

	as := sudoToC(delegate)
	if as == nil {
		return errors.New("Could not create sudo structure.")
	}
	defer C.free(unsafe.Pointer(as))

	id := C.CString(jobId)
	defer C.free(unsafe.Pointer(id))

	if errNumber := C.drmaa_control_as(as, id, ca, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorId[errNumber])
		return &ce
	}
	return nil
}

// Control sends a job modification request, i.e. terminates, suspends,
// resumes a job or sets it in a the hold state or release it from the
// job hold state.
func (s *Session) Control(jobId string, action controlType) error {
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

// TerminateJob sends a job termination request to the job executor.
func (s *Session) TerminateJob(jobId string) error {
	return s.Control(jobId, Terminate)
}

// SuspendJob sends a job suspenion request to the job executor.
func (s *Session) SuspendJob(jobId string) error {
	return s.Control(jobId, Suspend)
}

// ResumeJob sends a job resume request to the job executor.
func (s *Session) ResumeJob(jobId string) error {
	return s.Control(jobId, Resume)
}

// HoldJob put a job into the hold state.
func (s *Session) HoldJob(jobId string) error {
	return s.Control(jobId, Hold)
}

// ReleaseJob removes a hold state from a job.
func (s *Session) ReleaseJob(jobId string) error {
	return s.Control(jobId, Release)
}

// Synchornize blocks the programm until the given jobs finshed or
// a specific timeout is reached.
func (s *Session) Synchronize(jobIds []string, timeout int64, dispose bool) error {
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

// Wait blocks until the job left the DRM system or a timeout is reached and
// returns a JobInfo structure.
func (s *Session) Wait(jobId string, timeout int64) (jobinfo JobInfo, err error) {
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

// JobPs returns the cuurent state of a job.
func (s *Session) JobPs(jobId string) (PsType, error) {
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

// JobTemplate represents a job template which is required to
// submit a job.
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
func setNameValue(jt *C.drmaa_job_template_t, name string, value string) error {
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

func getStringValue(jt *C.drmaa_job_template_t, name string) (string, error) {
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

// SetRemoteCommand sets the path or the name of the binary to be started
// as a job in the job template.
func (jt *JobTemplate) SetRemoteCommand(cmd string) error {
	if jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_REMOTE_COMMAND, cmd)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// RemoteCommand returns the currently set binary which is going to be
// executed from the job template.
func (jt *JobTemplate) RemoteCommand() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_REMOTE_COMMAND)
}

// SetInputPath sets the input file which the job gets
// set when it is executed. The content of the file is
// forwarded as STDIN to the job.
func (jt *JobTemplate) SetInputPath(path string) error {
	if jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_INPUT_PATH, path)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// InputPath returns the input file of the remote command set
// in the job template.
func (jt *JobTemplate) InputPath() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_INPUT_PATH)
}

// SetOuputPath sets the path to a directory or a file which is
// used as output file or directory. Everything the job writes
// to standard output (stdout) is written in that file.
func (jt *JobTemplate) SetOutputPath(path string) error {
	if jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_OUTPUT_PATH, path)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// OutputPath returns the output path set in the job template.
func (jt *JobTemplate) OutputPath() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_OUTPUT_PATH)
}

// SetErrorPath sets the path to a directory or a file which is
// used as error file or directory. Everything the job writes to
// standard error (stderr) is written in that file.
func (jt *JobTemplate) SetErrorPath(path string) error {
	if jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_ERROR_PATH, path)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// ErrorPath returns the error path set in the job template.
func (jt *JobTemplate) ErrorPath() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_ERROR_PATH)
}

// vector attributes
func setVectorAttributes(jt *JobTemplate, name *C.char, args []string) error {
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

// SetArgs sets the arguments for the job executable in the job template.
func (jt *JobTemplate) SetArgs(args []string) error {
	if jt.jt != nil {
		drmaa_v_argv := C.CString(C.DRMAA_V_ARGV)
		return setVectorAttributes(jt, drmaa_v_argv, args)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// DG TODO Args() not specified!!!

// Set single argument. Simple wrapper for SetArgs([]string{arg}).
func (jt *JobTemplate) SetArg(arg string) error {
	return jt.SetArgs([]string{arg})
}

// SetEnv sets a set of environment variables inherited from the
// current environment forwarded to the environment of the job
// when it is executed.
func (jt *JobTemplate) SetEnv(envs []string) error {
	if jt.jt != nil {
		drmaa_v_env := C.CString(C.DRMAA_V_ENV)
		return setVectorAttributes(jt, drmaa_v_env, envs)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// DG TODO Env() is not specified!!!

// SetEmail sets the emails addresses in the job template used by the
// cluster scheduler to send emails to.
func (jt *JobTemplate) SetEmail(emails []string) error {
	if jt.jt != nil {
		drmaa_v_email := C.CString(C.DRMAA_V_EMAIL)
		return setVectorAttributes(jt, drmaa_v_email, emails)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// DG TODO Email() is not specified!!!

// SetJobSubmissionState sets the job submission state (like the hold state)
// in the job template.
func (jt *JobTemplate) SetJobSubmissionState(state SubmissionState) error {
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

// JobSubmissionState returns the job submission state set in the
// job template.
func (jt *JobTemplate) JobSubmissionState() (SubmissionState, error) {
	value, err := getStringValue(jt.jt, C.DRMAA_JS_STATE)
	if err != nil {
		return ActiveState, err
	}

	if value == "drmaa_hold" {
		return HoldState, nil
	} //else { // if value == "drmaa_active" {

	return ActiveState, nil
}

// SetWD sets the working directory for the job in the job template.
func (jt *JobTemplate) SetWD(dir string) error {
	if jt.jt != nil {
		drmaa_wd := C.DRMAA_WD
		return setNameValue(jt.jt, drmaa_wd, dir)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// WD returns the working directory set in the job template
func (jt *JobTemplate) WD() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_WD)
}

// SetNativeSpecification sets the native specification (DRM system depended
// job submission settings) for the job.
func (jt *JobTemplate) SetNativeSpecification(native string) error {
	if jt.jt != nil {
		drmaa_native_specification := C.DRMAA_NATIVE_SPECIFICATION
		return setNameValue(jt.jt, drmaa_native_specification, native)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// NativeSpecification returns the native specification set i in the job template.
// The native specification string is used for injecting DRM specific job submission
// requests to the system.
func (jt *JobTemplate) NativeSpecification() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_NATIVE_SPECIFICATION)
}

// SetBlockEmail set the DRMAA_BLOCK_EMAIL in the job template. When this is
// set it overrides any default behavior of the that might send emails when a job
// reached a specific state. This is used to prevent emails are going to be
// send.
func (jt *JobTemplate) SetBlockEmail(blockmail bool) error {
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

// BlockEmail returns true if BLOCK_EMAIL is set in the job template.
func (jt *JobTemplate) BlockEmail() (bool, error) {
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
func (jt *JobTemplate) SetStartTime(time time.Time) error {
	if jt.jt != nil {
		drmaa_start_time := C.DRMAA_START_TIME
		timeString := fmt.Sprintf("%d", time.Unix())
		return setNameValue(jt.jt, drmaa_start_time, timeString)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// StartTime returns the job start time set for the job.
func (jt *JobTemplate) StartTime() (time.Time, error) {
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
func (jt *JobTemplate) SetJobName(jobname string) error {
	if jt.jt != nil {
		drmaa_job_name := C.DRMAA_JOB_NAME
		return setNameValue(jt.jt, drmaa_job_name, jobname)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// JobName returns the name set in the job template.
func (jt *JobTemplate) JobName() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_JOB_NAME)
}

// SetJoinFiles sets that the error and output files have to be joined.
func (jt *JobTemplate) SetJoinFiles(join bool) error {
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
func (jt *JobTemplate) JoinFiles() (bool, error) {
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
func (jt *JobTemplate) SetTransferFiles(mode FileTransferMode) error {
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
func (jt *JobTemplate) TransferFiles() (FileTransferMode, error) {
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
func (jt *JobTemplate) SetDeadlineTime(deadline time.Duration) error {
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
func (jt *JobTemplate) parseDuration(field string) (defaultDuration time.Duration, err error) {
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
func (jt *JobTemplate) DeadlineTime() (deadlineTime time.Duration, err error) {
	return jt.parseDuration(C.DRMAA_DEADLINE_TIME)
}

// SetHardWallclockTimeLimit sets a hard wall-clock time limit for the job.
func (jt *JobTemplate) SetHardWallclockTimeLimit(limit time.Duration) error {
	if jt.jt != nil {
		drmaa_wct_hlimit := C.DRMAA_WCT_HLIMIT
		limitString := fmt.Sprintf("%d", int64(limit.Seconds()))
		return setNameValue(jt.jt, drmaa_wct_hlimit, limitString)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// HardWallclockTimeLimit returns the wall-clock time set in the job template.
func (jt *JobTemplate) HardWallclockTimeLimit() (deadlineTime time.Duration, err error) {
	return jt.parseDuration(C.DRMAA_WCT_HLIMIT)
}

// SetSoftWallclockTimeLimit sets a soft wall-clock time limit for the job in the job template.
func (jt *JobTemplate) SetSoftWallclockTimeLimit(limit time.Duration) error {
	if jt.jt != nil {
		drmaa_wct_slimit := C.DRMAA_WCT_SLIMIT
		limitString := fmt.Sprintf("%d", int64(limit.Seconds()))
		return setNameValue(jt.jt, drmaa_wct_slimit, limitString)
	}
	ce := makeError("No job template", errorId[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// SoftWallclockTimeLimit returns the soft wall-clock time limit for the job set in the job template.
func (jt *JobTemplate) SoftWallclockTimeLimit() (deadlineTime time.Duration, err error) {
	return jt.parseDuration(C.DRMAA_WCT_HLIMIT)
}

// SetHardRunDurationLimit sets a hard run-time limit for the job in the job tempplate.
func (jt *JobTemplate) SetHardRunDurationLimit(limit time.Duration) error {
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

// HardRunDurationLimit returns the hard run-time limit for the job in the job template.
func (jt *JobTemplate) HardRunDurationLimit() (deadlineTime time.Duration, err error) {
	return jt.parseDuration(C.DRMAA_DURATION_HLIMIT)
}

// SetSoftRunDurationLimit sets the soft run-time limit for the job in the job template.
func (jt *JobTemplate) SetSoftRunDurationLimit(limit time.Duration) error {
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

// SoftRunDurationLimit returns the soft run-time limit set in the job template.
func (jt *JobTemplate) SoftRunDurationLimit() (deadlineTime time.Duration, err error) {
	return jt.parseDuration(C.DRMAA_DURATION_SLIMIT)
}
