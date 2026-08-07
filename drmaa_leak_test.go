/*
    Copyright 2026 Daniel Gruber, info@gridengine.eu

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
*/

package drmaa

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
)

// Memory allocated through the C library is invisible to the Go garbage
// collector and to testing.AllocsPerRun, so a leak in the cgo layer can only
// be observed as growth of the process. The tests below drive the affected
// calls in a loop and compare the resident set size before and after.

// pointerSize is the size of a char* on the target platform. It determines
// how much a single unfreed job id array costs.
const pointerSize = 8

// residentSetSize returns the resident set size of the current process in
// bytes. It reads /proc/self/statm and is therefore Linux only, callers have
// to skip the test on other platforms.
func residentSetSize(t *testing.T) uint64 {
	t.Helper()

	content, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		t.Fatalf("Could not read /proc/self/statm: %s", err)
	}
	// format: size resident shared text lib data dt (all in pages)
	fields := strings.Fields(string(content))
	if len(fields) < 2 {
		t.Fatalf("Unexpected content in /proc/self/statm: %q", content)
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		t.Fatalf("Could not parse resident pages %q: %s", fields[1], err)
	}
	return pages * uint64(os.Getpagesize())
}

// TestSynchronizeDoesNotLeakJobIDArray detects the leak of the C string array
// which Synchronize() builds in order to hand the job ids over to
// drmaa_synchronize(). The job ids do not need to exist, the array is
// allocated before the DRMAA library is called, hence the leak happens
// independently of the outcome of the call.
func TestSynchronizeDoesNotLeakJobIDArray(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}
	if runtime.GOOS != "linux" {
		t.Skipf("Resident set size is read from /proc, not available on %s", runtime.GOOS)
	}

	s, err := MakeSession()
	if err != nil {
		t.Fatalf("Error during MakeSession(): %s\n", err)
	}
	defer s.Exit()

	// An unfixed Synchronize() leaks (jobIDCount+1)*pointerSize bytes per
	// call, which sums up to roughly 80 MB over all iterations. That is far
	// above any allocator noise.
	const (
		jobIDCount = 5000
		iterations = 2000
		warmup     = 100
	)

	jobIds := make([]string, jobIDCount)
	for i := range jobIds {
		jobIds[i] = fmt.Sprintf("godrmaanonexistentjob%d", i)
	}

	// Let the Go heap and the allocator arenas reach their working size
	// before the measurement starts, otherwise their growth shows up as a
	// leak.
	for i := 0; i < warmup; i++ {
		s.Synchronize(jobIds, TimeoutNoWait, false)
	}
	runtime.GC()
	debug.FreeOSMemory()
	before := residentSetSize(t)

	for i := 0; i < iterations; i++ {
		s.Synchronize(jobIds, TimeoutNoWait, false)
	}
	runtime.GC()
	debug.FreeOSMemory()
	after := residentSetSize(t)

	leakedWhenBroken := uint64((jobIDCount + 1) * pointerSize * iterations)
	// A quarter of the expected leak keeps a wide margin in both directions,
	// the fixed implementation stays near zero.
	limit := leakedWhenBroken / 4

	var grown uint64
	if after > before {
		grown = after - before
	}
	t.Logf("RSS before %d KB, after %d KB, grown %d KB, limit %d KB",
		before/1024, after/1024, grown/1024, limit/1024)

	if grown > limit {
		t.Errorf("Synchronize() grew the process by %d KB over %d calls, expected less than %d KB. "+
			"The job id array built by makeStringArray() is most likely not freed.",
			grown/1024, iterations, limit/1024)
	}
}

// TestRunBulkJobsReleasesJobIDs submits a small array job and checks the task
// ids returned by RunBulkJobs(). The job id vector handed out by the DRMAA
// library is released inside RunBulkJobs(), so this test also verifies that
// the release happens after the ids have been read out. Releasing too early
// would produce wrong ids or abort the process.
func TestRunBulkJobsReleasesJobIDs(t *testing.T) {
	s, err := MakeSession()
	if err != nil {
		t.Fatalf("Error during MakeSession(): %s\n", err)
	}
	defer s.Exit()

	jt, errAlloc := s.AllocateJobTemplate()
	if errAlloc != nil {
		t.Fatalf("Failed allocating a new job template: %s", errAlloc)
	}
	defer s.DeleteJobTemplate(&jt)

	jt.SetRemoteCommand("/bin/sleep")
	jt.SetArg("10")
	jt.SetJobName("TestRunBulkJobsGoDRMAATestJob")
	// submitted in hold state so that the tasks do not occupy the cluster
	jt.SetJobSubmissionState(HoldState)

	const tasks = 3
	ids, errRun := s.RunBulkJobs(&jt, 1, tasks, 1)
	if errRun != nil {
		t.Fatalf("Error submitting array job: %s", errRun)
	}
	// the ids are read from C memory which is freed before RunBulkJobs()
	// returns, they have to be valid Go copies here
	if len(ids) != tasks {
		t.Errorf("Expected %d task ids, got %d: %v", tasks, len(ids), ids)
	}
	for i, id := range ids {
		if id == "" {
			t.Errorf("Task id at position %d is empty: %v", i, ids)
		}
		if !strings.Contains(id, ".") {
			t.Errorf("Task id %q does not look like an array job task id", id)
		}
	}
	t.Logf("Submitted array job task ids: %v", ids)

	for _, id := range ids {
		if errTerm := s.TerminateJob(id); errTerm != nil {
			t.Errorf("Error terminating task %s: %s", id, errTerm)
		}
	}
}

// TestRunBulkJobsRepeatedly submits and removes array jobs in a loop. The job
// id vector released by RunBulkJobs() is a small fixed size struct, so the
// leak is too small to be measured reliably. This test therefore only
// verifies that repeated submissions keep working, a double free of the
// vector would abort the process here.
func TestRunBulkJobsRepeatedly(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping repeated array job submission in short mode")
	}

	s, err := MakeSession()
	if err != nil {
		t.Fatalf("Error during MakeSession(): %s\n", err)
	}
	defer s.Exit()

	jt, errAlloc := s.AllocateJobTemplate()
	if errAlloc != nil {
		t.Fatalf("Failed allocating a new job template: %s", errAlloc)
	}
	defer s.DeleteJobTemplate(&jt)

	jt.SetRemoteCommand("/bin/sleep")
	jt.SetArg("10")
	jt.SetJobName("TestRunBulkJobsRepeatedlyGoDRMAATestJob")
	jt.SetJobSubmissionState(HoldState)

	const submissions = 20
	for i := 0; i < submissions; i++ {
		ids, errRun := s.RunBulkJobs(&jt, 1, 2, 1)
		if errRun != nil {
			t.Fatalf("Error submitting array job %d: %s", i, errRun)
		}
		if len(ids) != 2 {
			t.Fatalf("Submission %d returned %d ids, expected 2: %v", i, len(ids), ids)
		}
		for _, id := range ids {
			if errTerm := s.TerminateJob(id); errTerm != nil {
				t.Errorf("Error terminating task %s: %s", id, errTerm)
			}
		}
	}
}
