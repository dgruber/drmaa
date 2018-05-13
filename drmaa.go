/*
   Copyright 2012, 2013, 2014, 2015, 2016 Daniel Gruber, info@gridengine.eu

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

   This is a Go DRMAA language binding implementation based on the
   Open Grid Forum DRMAA C API standard.

   Changelog:
   0.1 - First Release: Proof of Concept
   0.2 - Added methods for accessing JobInfo struct.
   0.3 - Minor cleanups
   0.4 - Added gestatus for detailed job status fetching with "qstat -xml"
   0.5 - Go DRMAA is now under BSD License (instead in the jail of GPL)!
   0.6 - Smaller issues
   0.7 - Changed *Error results to error. Old implementation is in DRMAA_OLD_ERROR branch.
   0.8 - Added jtemplate support queries (GetAttributeNames(),GetVectorAttributeNames()).
         Mem leaks fixed. Added Email() / Args() / Env() for querying
         vector attributes set in the job template.
   2016/1
   0.9 - Changed ErrorId to ErrorID and JobId to JobID as well as Placeholder constants.
         No more interface changes planned. Sorry for that...

   If you have any questions contact me at info @ gridengine.eu.
*/

// Package drmaa is a job submission library for job schedulers like Univa Grid Engine.
// It is based on the open Distributed Resource Management Application API standard
// (version 1). It requires a C library (libdrmaa.so) usually shipped with a job
// job scheduler.
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
#if __APPLE__
 #include <unistd.h>
#endif
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
   if (a == NULL || *a == NULL) {
      return;
   }
   for (; i < size; i++)
      free(a[i]);
   free(a);
}

static int _drmaa_get_num_attr_values(drmaa_attr_values_t* values, int *size) {
#if defined(TORQUE) || defined(SLURM)
    return drmaa_get_num_attr_values(values, (size_t *) size);
#else
    return drmaa_get_num_attr_values(values, size);
#endif
}

static int _drmaa_get_num_attr_names(drmaa_attr_names_t* names, int *size) {
#if defined(TORQUE) || defined(SLURM)
    return drmaa_get_num_attr_names(names, (size_t *) size);
#else
    return drmaa_get_num_attr_names(names, size);
#endif
}
*/
import "C"

const version string = "0.9"

// default string size
const stringSize C.size_t = C.DRMAA_ERROR_STRING_BUFFER
const jobnameSize C.size_t = C.DRMAA_JOBNAME_BUFFER
const attrnameSize C.size_t = C.DRMAA_ATTR_BUFFER

// PlaceholderHomeDirectory is a placeholder for the user's home directory when filling out job template.
const PlaceholderHomeDirectory string = "$drmaa_hd_ph$"

// PlaceholderWorkingDirectory is a placeholder for the working directory path which can be used in
// the job template (like in the input or output path specification).
const PlaceholderWorkingDirectory string = "$drmaa_wd_ph$"

// PlaceholderTaskID is a placeholder for the array job task ID which can be used in the job
// template (like in the input or output path specification).
const PlaceholderTaskID string = "$drmaa_incr_ph$"

// PsType specifies a job state (output of JobPs()).
type PsType int

const (
	// PsUndetermined represents an unknown job state
	PsUndetermined PsType = iota
	// PsQueuedActive means the job is queued and eligible to run
	PsQueuedActive
	// PsSystemOnHold means the job is put into an hold state by the system
	PsSystemOnHold
	// PsUserOnHold means the job is put in the hold state by the user
	PsUserOnHold
	// PsUserSystemOnHold means the job is put in the hold state by the system and by the user
	PsUserSystemOnHold
	// PsRunning means the job is currently executed
	PsRunning
	// PsSystemSuspended means the job is suspended by the DRM
	PsSystemSuspended
	// PsUserSuspended means the job is suspended by the user
	PsUserSuspended
	// PsUserSystemSuspended means the job is suspended by the DRM and by the user
	PsUserSystemSuspended
	// PsDone means the job finished normally
	PsDone
	// PsFailed means the job  finished and failed
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
	// HoldState is a job submission state which means the job should not be scheduled.
	HoldState SubmissionState = iota
	// ActiveState is a job submission state which means the job is allowed to be scheduled.
	ActiveState
)

// Timeout is either a positive number in seconds or one of
// those constants.
const (
	// TimeoutWaitForever is a time value of infinit.
	TimeoutWaitForever int64 = -1
	// TimeoutNoWait is a time value for zero.
	TimeoutNoWait int64 = 0
)

// Job controls for session.Control().
type controlType int

const (
	// Suspend is a control action for suspending a job (usually sending SIGSTOP).
	Suspend controlType = iota
	// Resume is a control action fo resuming a suspended job.
	Resume
	// Hold is a control action for preventing that a job is going to be scheduled.
	Hold
	// Release is a control action for allowing that a job in hold state is allowed to be scheduled.
	Release
	// Terminate is a control action for deleting a job.
	Terminate
)

// ErrorID is DRMAA error ID representation type
type ErrorID int

const (
	// Success indicates that no errors occurred
	Success ErrorID = iota
	// InternalError indicates an error within the DRM
	InternalError
	// DrmCommunicationFailure indicates a communication problem
	DrmCommunicationFailure
	// AuthFailure indication an error during authentification
	AuthFailure
	// InvalidArgument indicates a wrong imput parameter or an unsupported method call
	InvalidArgument
	// NoActiveSession indicates an error due to a non valid session state
	NoActiveSession
	// NoMemory indicates an OOM situation
	NoMemory
	// InvalidContactString indicates a wrong contact string
	InvalidContactString
	// DefaultContactStringError indicates an error with the contact string
	DefaultContactStringError
	// NoDefaultContactStringSelected indicates an error with the contact string
	NoDefaultContactStringSelected
	// DrmsInitFailed indicates an error when establishing a connection to the DRM
	DrmsInitFailed
	// AlreadyActiveSession indicates an error with an already existing connection
	AlreadyActiveSession
	// DrmsExitError indicates an error when shutting down the connection to the DRM
	DrmsExitError
	// InvalidAttributeFormat is an attribute format error
	InvalidAttributeFormat
	// InvalidAttributeValue is an attribute value error
	InvalidAttributeValue
	// ConflictingAttributeValues is a semantic error with conflicting attribute settings
	ConflictingAttributeValues
	// TryLater indicates a temporal problem with the DRM
	TryLater
	// DeniedByDrm indicates a permission problem
	DeniedByDrm
	// InvalidJob indicates a problem with the job or job ID
	InvalidJob
	// ResumeInconsistentState indicates a state problem
	ResumeInconsistentState
	// SuspendInconsistentState indicates a state problem
	SuspendInconsistentState
	// HoldInconsistentState indicates a state problem
	HoldInconsistentState
	// ReleaseInconsistentState indicates a state problem
	ReleaseInconsistentState
	// ExitTimeout indicates a timeout issue
	ExitTimeout
	// NoRusage indicates an issue with resource usage values
	NoRusage
	// NoMoreElements indicates that no more elements are available
	NoMoreElements
)

// Internal map between C DRMAA error and Go DRMAA error.
var errorID = map[C.int]ErrorID{
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
var internalError = map[ErrorID]C.int{
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

// FileTransferMode determines which files should be staged.
type FileTransferMode struct {
	ErrorStream  bool
	InputStream  bool
	OutputStream bool
}

// JobInfo contains all runtime information about a job.
type JobInfo struct {
	resourceUsage     map[string]string
	jobID             string
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

// JobID returns the job id as string.
func (ji *JobInfo) JobID() string {
	return ji.jobID
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

// TerminationSignal returns the termination signal of the job.
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

// Error is a Go DRMAA error type which implements the
// Go Error interface with the Error() method.
// Each external error can be casted to a pointer
// to that struct in order to get more information
// about the error (the error id).
type Error struct {
	Message string
	ID      ErrorID
}

// makeError is an intenal function which creates an GO DRMAA error.
func makeError(msg string, id ErrorID) Error {
	var ce Error
	ce.Message = msg
	ce.ID = id
	return ce
}

// Error implements the Go error interface for the drmaa.Error type.
func (ce Error) Error() string {
	return ce.Message
}

// DRMAA functions (not methods required)

// StrError maps an ErrorId to an error string.
func StrError(id ErrorID) string {
	// check if not found
	var ie C.int
	ie = internalError[id]

	errStr := C.drmaa_strerror(ie)
	return C.GoString(errStr)
}

// GetContact returns the contact string of the DRM system.
func GetContact() (string, error) {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	contact := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(contact))

	errNumber := C.drmaa_get_contact(contact, stringSize, diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return "", &ce
	}
	return C.GoString(contact), nil
}

// GetVersion returns the version of the DRMAA standard.
func GetVersion() (int, int, error) {
	return 1, 0, nil
}

// GetDrmSystem returns the DRM system identification string.
func (s *Session) GetDrmSystem() (string, error) {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	drm := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(drm))

	errNumber := C.drmaa_get_DRM_system(drm, stringSize,
		diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return "", &ce
	}

	return C.GoString(drm), nil
}

// GetDrmaaImplementation returns information about the DRMAA implementation.
func (s *Session) GetDrmaaImplementation() string {
	return fmt.Sprintf("GO DRMAA Implementation by Daniel Gruber Version %s", version)
}

// Session is a type which represents a DRMAA session.
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
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return &ce
	}
	s.initialized = true
	return nil
}

// Exit disengages a session frmo the DRMAA library and cleans it up.
func (s *Session) Exit() error {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	errNumber := C.drmaa_exit(diag, stringSize)
	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return &ce
	}
	return nil
}

// GetAttributeNames returns a set of supported DRMAA attributes
// allowed in the job template.
func (s *Session) GetAttributeNames() ([]string, error) {
	return s.getAttributeNames(false)
}

// GetVectorAttributeNames returns a set of supported DRMAA vector
// attributes allowed in a C job template.
// This functionality is not required for the Go DRMAA implementation.
func (s *Session) GetVectorAttributeNames() ([]string, error) {
	return s.getAttributeNames(true)
}

func (s *Session) getAttributeNames(vectorAttributes bool) ([]string, error) {
	if s.initialized == false {
		// error, need a connection (active session)
		ce := makeError("No active session", NoActiveSession)
		return nil, &ce
	}
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	var vector *C.drmaa_attr_names_t
	var errNumber C.int
	if vectorAttributes {
		errNumber = C.drmaa_get_vector_attribute_names(&vector, diag, stringSize)
	} else {
		errNumber = C.drmaa_get_attribute_names(&vector, diag, stringSize)
	}

	if errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return nil, &ce
	}

	// the size is not really required but a good test if that
	// method is implemented in the underlying C DRMAA library
	var size C.int
	errNumber = C._drmaa_get_num_attr_names(vector, &size)
	if errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return nil, &ce
	}

	attributes := make([]string, 0, int(size))

	// convert all C attribute names
	for {
		attrName := C.makeString(attrnameSize)
		defer C.free(unsafe.Pointer(attrName))
		errNumber = C.drmaa_get_next_attr_name(vector, attrName, attrnameSize-1)
		if errNumber != C.DRMAA_ERRNO_SUCCESS {
			break
		}
		attributes = append(attributes, C.GoString(attrName))
	}
	// free vector
	C.drmaa_release_attr_names(vector)
	return attributes, nil
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
		ce := makeError(C.GoString(diag), errorID[errNumber])
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
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return &ce
	}
	return nil
}

// RunJob submits a job in a (initialized) session to the cluster scheduler.
func (s *Session) RunJob(jt *JobTemplate) (string, error) {
	jobID := C.makeString(jobnameSize)
	defer C.free(unsafe.Pointer(jobID))

	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	errNumber := C.drmaa_run_job(jobID, jobnameSize, jt.jt, diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return "", &ce
	}
	return C.GoString(jobID), nil
}

// RunBulkJobs submits a job as an array job.
func (s *Session) RunBulkJobs(jt *JobTemplate, start, end, incr int) ([]string, error) {
	var ids *C.drmaa_job_ids_t
	jobID := C.makeString(jobnameSize)
	defer C.free(unsafe.Pointer(jobID))

	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	errNumber := C.drmaa_run_bulk_jobs(&ids, jt.jt, C.int(start), C.int(end), C.int(incr),
		diag, stringSize)

	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return nil, &ce
	}

	// collect job ids
	var jobIds []string
	for C.drmaa_get_next_job_id(ids, jobID, C.DRMAA_JOBNAME_BUFFER) == C.DRMAA_ERRNO_SUCCESS {
		jobIds = append(jobIds, C.GoString(jobID))
	}

	return jobIds, nil
}

// Control sends a job modification request, i.e. terminates, suspends,
// resumes a job or sets it in a the hold state or release it from the
// job hold state.
func (s *Session) Control(jobID string, action controlType) error {
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

	jobid := C.CString(jobID)
	defer C.free(unsafe.Pointer(jobid))
	if errNumber := C.drmaa_control(jobid, ca, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return &ce
	}
	return nil
}

// make job control more straightforward

// TerminateJob sends a job termination request to the job executor.
func (s *Session) TerminateJob(jobID string) error {
	return s.Control(jobID, Terminate)
}

// SuspendJob sends a job suspenion request to the job executor.
func (s *Session) SuspendJob(jobID string) error {
	return s.Control(jobID, Suspend)
}

// ResumeJob sends a job resume request to the job executor.
func (s *Session) ResumeJob(jobID string) error {
	return s.Control(jobID, Resume)
}

// HoldJob put a job into the hold state.
func (s *Session) HoldJob(jobID string) error {
	return s.Control(jobID, Hold)
}

// ReleaseJob removes a hold state from a job.
func (s *Session) ReleaseJob(jobID string) error {
	return s.Control(jobID, Release)
}

// Synchronize blocks the programm until the given jobs finshed or
// a specific timeout is reached.
func (s *Session) Synchronize(jobIds []string, timeout int64, dispose bool) error {
	// TODO handle special string: DRMAA_ID_SESSION_ALL
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	jobids := C.makeStringArray(C.int(len(jobIds) + 1))

	for i, jobid := range jobIds {
		id := C.CString(jobid)
		defer C.free(unsafe.Pointer(id))
		C.setString(jobids, id, C.int(i))
	}
	C.setString(jobids, nil, C.int(len(jobIds)))

	var disp C.int
	if dispose {
		disp = C.int(1)
	} else {
		disp = C.int(0)
	}

	if errNumber := C.drmaa_synchronize(jobids, C.long(timeout), disp,
		diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return &ce
	}

	return nil
}

// Wait blocks until the job left the DRM system or a timeout is reached and
// returns a JobInfo structure.
func (s *Session) Wait(jobID string, timeout int64) (jobinfo JobInfo, err error) {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	jobIDOut := C.makeString(jobnameSize)
	defer C.free(unsafe.Pointer(jobIDOut))

	// out
	cstat := C.int(0)
	var crusage *C.struct_drmaa_attr_values_s

	jobid := C.CString(jobID)
	defer C.free(unsafe.Pointer(jobid))

	if errNumber := C.drmaa_wait(jobid, jobIDOut, jobnameSize, &cstat,
		C.long(timeout), &crusage, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return jobinfo, &ce
	}

	// fill JobInfo struct
	exited := C.int(0)
	if errNumber := C.drmaa_wifexited(&exited, cstat, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorID[errNumber])
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
			ce := makeError(C.GoString(diag), errorID[errNumber])
			return jobinfo, &ce
		}
		jobinfo.exitStatus = int64(exitstatus)
	}

	signaled := C.int(0)
	if errNumber := C.drmaa_wifsignaled(&signaled, cstat, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		C.drmaa_release_attr_values(crusage)
		ce := makeError(C.GoString(diag), errorID[errNumber])
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
			ce := makeError(C.GoString(diag), errorID[errNumber])
			return jobinfo, &ce
		}
		jobinfo.terminationSignal = C.GoString(termsig)
	}

	aborted := C.int(0)
	if errNumber := C.drmaa_wifsignaled(&aborted, cstat, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS {
		C.drmaa_release_attr_values(crusage)
		ce := makeError(C.GoString(diag), errorID[errNumber])
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
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return jobinfo, &ce
	}
	if coreDumped == 0 {
		jobinfo.hasCoreDump = false
	} else {
		jobinfo.hasCoreDump = true
	}

	jobinfo.jobID = C.GoString(jobIDOut)

	// rusage
	usageLength := C.int(0)
	if errNumber := C._drmaa_get_num_attr_values(crusage, &usageLength); errNumber != C.DRMAA_ERRNO_SUCCESS {
		C.drmaa_release_attr_values(crusage)
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return jobinfo, &ce
	}

	usageArray := make([]string, 0, int(usageLength))
	for i := 0; i < int(usageLength); i++ {
		usage := C.makeString(stringSize)
		defer C.free(unsafe.Pointer(usage))
		if C.drmaa_get_next_attr_value(crusage, usage, stringSize) == C.DRMAA_ERRNO_SUCCESS {
			usageArray = append(usageArray, C.GoString(usage))
		}
	}
	C.drmaa_release_attr_values(crusage)
	// make a map out of the array
	jobinfo.resourceUsage = makeUsageMap(usageArray)

	return jobinfo, nil
}

// makeUsageMap creates out of an array of job usage
// values (structure is: resource=measurement) a map
// mapping the resources to its usage values.
func makeUsageMap(usageArray []string) map[string]string {
	// make a map out of the array
	usageMap := make(map[string]string)
	for i := range usageArray {
		nameVal := strings.Split(usageArray[i], "=")
		usageMap[nameVal[0]] = nameVal[1]
	}
	return usageMap
}

// JobPs returns the current state of a job.
func (s *Session) JobPs(jobID string) (PsType, error) {
	outPs := C.int(0)
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	id := C.CString(jobID)
	defer C.free(unsafe.Pointer(id))

	if errNumber := C.drmaa_job_ps(id, &outPs, diag, stringSize); errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorID[errNumber])
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

// JobTemplate represents a job template which is required to submit a job.
// A JobTemplate contains job submission parameters like a name, accounting
// string, command to execute, it's arguments and so on. In this implementation
// within a job template a pointer to an allocated C job template is stored
// which must be freed by the user. The values can only be accessed by the
// defined methods.
type JobTemplate struct {
	// reference to C job template
	jt *C.drmaa_job_template_t
}

// String implements the Stringer interface for the JobTemplate.
// Note that this operation is not very efficient since it needs
// to get all values out of the C object.
func (jt *JobTemplate) String() string {
	// TODO vector attributes missing here (Args, Email, ...)
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
	s = fmt.Sprintf("Remote command: %s\nJob name: %s\nNative specification: %s\nOutput path: %s\nInput path: %s\nWorking directory: %s\nJoin files: %t\nSubmission state: %v\nStart time: %s\nSoft run duration limit: %s\nHard run duration limit: %s\nError path: %s\nBlock email: %t\nDeadline time: %s",
		rc, jn, ns, op, ip, wd, jf, js, st, sr, hr, ep, be, dl)

	return s
}

// setNameValue sets a name, value attribute pair into a job template. The name
// is usually a constant defined by the DRMAA header file (spec).
func setNameValue(jt *C.drmaa_job_template_t, name string, value string) error {
	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	v := C.CString(value)
	defer C.free(unsafe.Pointer(v))

	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))

	errNumber := C.drmaa_set_attribute(jt, n, v, diag, stringSize)
	if errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return &ce
	}
	return nil
}

func getStringValue(jt *C.drmaa_job_template_t, name string) (string, error) {
	if jt == nil {
		ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
		return "", &ce
	}

	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	v := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(v))

	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))

	errNumber := C.drmaa_get_attribute(jt, n, v, stringSize, diag, stringSize)
	if errNumber != C.DRMAA_ERRNO_SUCCESS {
		ce := makeError(C.GoString(diag), errorID[errNumber])
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
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
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
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// InputPath returns the input file of the remote command set
// in the job template.
func (jt *JobTemplate) InputPath() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_INPUT_PATH)
}

// SetOutputPath sets the path to a directory or a file which is
// used as output file or directory. Everything the job writes
// to standard output (stdout) is written in that file.
func (jt *JobTemplate) SetOutputPath(path string) error {
	if jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_OUTPUT_PATH, path)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
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
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// ErrorPath returns the error path set in the job template.
func (jt *JobTemplate) ErrorPath() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_ERROR_PATH)
}

// setVectorAttributes sets a Go string slice as a char* array in the
// C job template structure stored in the Go JobTemplate struct
func setVectorAttributes(jt *JobTemplate, name *C.char, args []string) error {
	if jt == nil || jt.jt == nil {
		ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
		return &ce
	}

	if len(args) == 0 {
		return nil
	}

	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	values := C.makeStringArray(C.int(len(args) + 1))
	defer C.freeStringArray(values, C.int(len(args)+1))

	for i, a := range args {
		carg := C.CString(a)
		// do not free carg here! it will be freed from freeStringArray
		C.setString(values, carg, C.int(i))
	}

	errNumber := C.drmaa_set_vector_attribute(jt.jt, name, values, diag, stringSize)
	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return &ce
	}
	return nil
}

// getVectorAttribute returns a slice of attributes set for a specific
// vector attribute out of the underlying C job template.
func getVectorAttribute(jt *JobTemplate, name *C.char) ([]string, error) {
	if jt == nil || jt.jt == nil {
		ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
		return nil, &ce
	}

	diag := C.makeString(stringSize)
	defer C.free(unsafe.Pointer(diag))

	var values *C.drmaa_attr_values_t

	errNumber := C.drmaa_get_vector_attribute(jt.jt, name, &values, diag, stringSize)
	if errNumber != C.DRMAA_ERRNO_SUCCESS && diag != nil {
		ce := makeError(C.GoString(diag), errorID[errNumber])
		return nil, &ce
	}

	attributes := make([]string, 0, 16)

	// convert all C attribute names
	for {
		attrName := C.makeString(attrnameSize)
		defer C.free(unsafe.Pointer(attrName))
		errNumber = C.drmaa_get_next_attr_value(values, attrName, attrnameSize-1)
		if errNumber != C.DRMAA_ERRNO_SUCCESS {
			break
		}
		attributes = append(attributes, C.GoString(attrName))
	}
	// free the attributes
	C.drmaa_release_attr_values(values)
	return attributes, nil
}

// SetArgs sets the arguments for the job executable in the job template.
func (jt *JobTemplate) SetArgs(args []string) error {
	if jt != nil && jt.jt != nil {
		drmaaVArgv := C.CString(C.DRMAA_V_ARGV)
		defer C.free(unsafe.Pointer(drmaaVArgv))
		return setVectorAttributes(jt, drmaaVArgv, args)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// Args returns the arguments set in the job template for the jobs process.
func (jt *JobTemplate) Args() ([]string, error) {
	if jt == nil || jt.jt == nil {
		ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
		return nil, &ce
	}
	drmaaVArgv := C.CString(C.DRMAA_V_ARGV)
	defer C.free(unsafe.Pointer(drmaaVArgv))
	return getVectorAttribute(jt, drmaaVArgv)
}

// SetArg sets a single argument. Simple wrapper for SetArgs([]string{arg}).
func (jt *JobTemplate) SetArg(arg string) error {
	return jt.SetArgs([]string{arg})
}

// SetEnv sets a set of environment variables inherited from the
// current environment forwarded to the environment of the job
// when it is executed.
func (jt *JobTemplate) SetEnv(envs []string) error {
	if jt.jt != nil {
		venv := C.CString(C.DRMAA_V_ENV)
		defer C.free(unsafe.Pointer(venv))
		return setVectorAttributes(jt, venv, envs)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
	return &ce
}

// Env returns the environment variables set in the job template for the jobs
// process.
func (jt *JobTemplate) Env() ([]string, error) {
	if jt == nil || jt.jt == nil {
		ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
		return nil, &ce
	}
	venv := C.CString(C.DRMAA_V_ENV)
	defer C.free(unsafe.Pointer(venv))
	return getVectorAttribute(jt, venv)
}

// SetEmail sets the emails addresses in the job template used by the
// cluster scheduler to send emails to.
func (jt *JobTemplate) SetEmail(emails []string) error {
	if jt.jt != nil {
		drmaaVEmail := C.CString(C.DRMAA_V_EMAIL)
		defer C.free(unsafe.Pointer(drmaaVEmail))
		return setVectorAttributes(jt, drmaaVEmail, emails)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// Email returns the email addresses set in the job template which are
// notified on defined events of the underlying job.
func (jt *JobTemplate) Email() ([]string, error) {
	if jt == nil || jt.jt == nil {
		ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_JOB])
		return nil, &ce
	}
	drmaaVEmail := C.CString(C.DRMAA_V_EMAIL)
	defer C.free(unsafe.Pointer(drmaaVEmail))
	return getVectorAttribute(jt, drmaaVEmail)
}

// SetJobSubmissionState sets the job submission state (like the hold state)
// in the job template.
func (jt *JobTemplate) SetJobSubmissionState(state SubmissionState) error {
	if jt.jt != nil {
		if state == HoldState {
			return setNameValue(jt.jt, C.DRMAA_JS_STATE, "drmaa_hold")
		} //if (state == ActiveState) {
		return setNameValue(jt.jt, C.DRMAA_JS_STATE, "drmaa_active")
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
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
	if jt != nil && jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_WD, dir)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// WD returns the working directory set in the job template
func (jt *JobTemplate) WD() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_WD)
}

// SetNativeSpecification sets the native specification (DRM system depended
// job submission settings) for the job.
func (jt *JobTemplate) SetNativeSpecification(native string) error {
	if jt != nil && jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_NATIVE_SPECIFICATION, native)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
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
	if jt != nil && jt.jt != nil {
		if blockmail {
			return setNameValue(jt.jt, C.DRMAA_BLOCK_EMAIL, "1")
		}
		return setNameValue(jt.jt, C.DRMAA_BLOCK_EMAIL, "0")
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
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
	if jt != nil && jt.jt != nil {
		timeString := fmt.Sprintf("%d", time.Unix())
		return setNameValue(jt.jt, C.DRMAA_START_TIME, timeString)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// StartTime returns the job start time set for the job.
func (jt *JobTemplate) StartTime() (time.Time, error) {
	value, err := getStringValue(jt.jt, C.DRMAA_START_TIME)
	if err != nil {
		var t time.Time
		return t, err
	}
	sec, errParse := strconv.ParseInt(value, 10, 64)
	if t := time.Unix(sec, 0); errParse == nil {
		return t, nil
	}
	ce := makeError("Unknown timestamp", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	var t time.Time
	return t, &ce
}

// SetJobName sets the name of the job in the job template.
func (jt *JobTemplate) SetJobName(jobname string) error {
	if jt != nil && jt.jt != nil {
		return setNameValue(jt.jt, C.DRMAA_JOB_NAME, jobname)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// JobName returns the name set in the job template.
func (jt *JobTemplate) JobName() (string, error) {
	return getStringValue(jt.jt, C.DRMAA_JOB_NAME)
}

// SetJoinFiles sets that the error and output files of the job have to be
// joined.
func (jt *JobTemplate) SetJoinFiles(join bool) error {
	if jt != nil && jt.jt != nil {
		if join {
			return setNameValue(jt.jt, C.DRMAA_JOIN_FILES, "y")
		}
		return setNameValue(jt.jt, C.DRMAA_JOIN_FILES, "n")
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// JoinFiles returns if join files is set in the job template.
func (jt *JobTemplate) JoinFiles() (bool, error) {
	val, err := getStringValue(jt.jt, C.DRMAA_JOB_NAME)
	if err != nil {
		return false, err
	}
	if val == "y" {
		return true, nil
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
		return setNameValue(jt.jt, C.DRMAA_TRANSFER_FILES, ftm)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// TransferFiles returns the FileTransferModes set in the job template.
func (jt *JobTemplate) TransferFiles() (FileTransferMode, error) {
	if jt != nil && jt.jt != nil {
		val, err := getStringValue(jt.jt, C.DRMAA_TRANSFER_FILES)
		if err != nil {
			var ftm FileTransferMode
			return ftm, err
		}
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
			return ftm, nil
		}
	}
	var ftm FileTransferMode
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return ftm, &ce
}

// SetDeadlineTime sets deadline time in job template. Unsupported in Grid Engine.
func (jt *JobTemplate) SetDeadlineTime(deadline time.Duration) error {
	if jt != nil && jt.jt != nil {
		limitString := fmt.Sprintf("%d", int64(deadline.Seconds()))
		return setNameValue(jt.jt, C.DRMAA_DEADLINE_TIME, limitString)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// Unsupported in Grid Engine.
func (jt *JobTemplate) parseDuration(field string) (defaultDuration time.Duration, err error) {
	if jt != nil && jt.jt != nil {
		val, err := getStringValue(jt.jt, field)
		if err != nil {
			return defaultDuration, err
		}
		if sec, err := time.ParseDuration(val + "s"); err == nil {
			return sec, nil
		}
		ce := makeError("Couldn't parse duration.", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
		var t time.Duration
		return t, &ce
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return defaultDuration, &ce
}

// DeadlineTime returns deadline time. Unsupported in Grid Engine.
func (jt *JobTemplate) DeadlineTime() (deadlineTime time.Duration, err error) {
	return jt.parseDuration(C.DRMAA_DEADLINE_TIME)
}

// SetHardWallclockTimeLimit sets a hard wall-clock time limit for the job.
func (jt *JobTemplate) SetHardWallclockTimeLimit(limit time.Duration) error {
	if jt != nil && jt.jt != nil {
		limitString := fmt.Sprintf("%d", int64(limit.Seconds()))
		fmt.Printf("limit string: %s", limitString)
		return setNameValue(jt.jt, C.DRMAA_WCT_HLIMIT, limitString)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// HardWallclockTimeLimit returns the wall-clock time set in the job template.
func (jt *JobTemplate) HardWallclockTimeLimit() (deadlineTime time.Duration, err error) {
	return jt.parseDuration(C.DRMAA_WCT_HLIMIT)
}

// SetSoftWallclockTimeLimit sets a soft wall-clock time limit for the job in the job template.
func (jt *JobTemplate) SetSoftWallclockTimeLimit(limit time.Duration) error {
	if jt != nil && jt.jt != nil {
		limitString := fmt.Sprintf("%d", int64(limit.Seconds()))
		return setNameValue(jt.jt, C.DRMAA_WCT_SLIMIT, limitString)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// SoftWallclockTimeLimit returns the soft wall-clock time limit for the job set in the job template.
func (jt *JobTemplate) SoftWallclockTimeLimit() (deadlineTime time.Duration, err error) {
	return jt.parseDuration(C.DRMAA_WCT_HLIMIT)
}

// SetHardRunDurationLimit sets a hard run-time limit for the job in the job template.
func (jt *JobTemplate) SetHardRunDurationLimit(limit time.Duration) error {
	if jt != nil && jt.jt != nil {
		limitString := fmt.Sprintf("%d", int64(limit.Seconds()))
		return setNameValue(jt.jt, C.DRMAA_DURATION_HLIMIT, limitString)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// HardRunDurationLimit returns the hard run-time limit for the job from the job template.
func (jt *JobTemplate) HardRunDurationLimit() (deadlineTime time.Duration, err error) {
	return jt.parseDuration(C.DRMAA_DURATION_HLIMIT)
}

// SetSoftRunDurationLimit sets the soft run-time limit for the job in the job template.
func (jt *JobTemplate) SetSoftRunDurationLimit(limit time.Duration) error {
	if jt != nil && jt.jt != nil {
		limitString := fmt.Sprintf("%d", int64(limit.Seconds()))
		return setNameValue(jt.jt, C.DRMAA_DURATION_SLIMIT, limitString)
	}
	ce := makeError("No job template", errorID[C.DRMAA_ERRNO_INVALID_ARGUMENT])
	return &ce
}

// SoftRunDurationLimit returns the soft run-time limit set in the job template.
func (jt *JobTemplate) SoftRunDurationLimit() (deadlineTime time.Duration, err error) {
	return jt.parseDuration(C.DRMAA_DURATION_SLIMIT)
}
