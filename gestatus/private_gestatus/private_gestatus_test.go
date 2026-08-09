/*
   Copyright 2026 Daniel Gruber

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

package geparser

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

// A library must never terminate the process of its caller. The functions
// below used to call log.Fatal(), and therefore os.Exit(1), when qstat could
// not be executed. That cannot be observed from inside the same process, so
// the check re-runs the test binary as a child process and inspects how the
// child terminated.

// subprocessEnv marks the child process run of the test below.
const subprocessEnv = "GODRMAA_QSTAT_SUBPROCESS"

// Exit codes the child uses to report what it observed. Anything else,
// in particular the 1 of log.Fatal, means the library killed the process.
const (
	exitReturnedError = 3
	exitReturnedNil   = 4
)

// TestGetClusterJobsStatusDoesNotExitOnQstatFailure verifies that a failing
// qstat call is reported as an error instead of terminating the caller. The
// child process runs with an empty PATH, so qstat cannot be found.
func TestGetClusterJobsStatusDoesNotExitOnQstatFailure(t *testing.T) {
	if os.Getenv(subprocessEnv) == "1" {
		if _, err := GetClusterJobsStatus(); err != nil {
			os.Exit(exitReturnedError)
		}
		os.Exit(exitReturnedNil)
	}

	cmd := exec.Command(os.Args[0],
		"-test.run=TestGetClusterJobsStatusDoesNotExitOnQstatFailure")
	// an empty PATH makes exec.Command("qstat", ...) fail to start
	cmd.Env = append(os.Environ(), subprocessEnv+"=1", "PATH=")

	out, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		exitErr, isExitErr := err.(*exec.ExitError)
		if !isExitErr {
			t.Fatalf("Could not run the test binary as a child process: %s", err)
		}
		exitCode = exitErr.ExitCode()
	}

	switch exitCode {
	case exitReturnedError:
		// expected, GetClusterJobsStatus() returned an error and returned control
	case exitReturnedNil:
		t.Errorf("GetClusterJobsStatus() returned a nil error although qstat "+
			"could not be executed. Child output: %s", out)
	default:
		t.Errorf("GetClusterJobsStatus() terminated the process with exit code %d "+
			"instead of returning an error. A library must not call log.Fatal() or "+
			"os.Exit(). Child output: %s", exitCode, out)
	}
}

// resetJobStatusCache empties the shared cache so that a test starts from a
// known state and does not leave entries behind for the next one.
func resetJobStatusCache(t *testing.T) {
	t.Helper()

	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	cachedJobStatus = map[string]jobStatusCache{}
}

// TestJobStatusCacheConcurrentAccess drives the shared job status cache from
// several goroutines at once. One goroutine per observed job is the natural
// way to use gestatus, and the cache used to be an unsynchronized global map.
// Without the mutex the Go runtime aborts the whole process with
// "fatal error: concurrent map writes", which is a throw and not a panic, so
// recover() cannot turn it into a test failure. Run with -race to also catch
// the read/write races which the runtime detector does not always report.
func TestJobStatusCacheConcurrentAccess(t *testing.T) {
	resetJobStatusCache(t)
	t.Cleanup(func() { resetJobStatusCache(t) })

	const (
		goroutines = 8
		iterations = 250
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// distinct ids keep the writers hitting different buckets,
				// a shared id keeps them hitting the same one
				storeJobStatusInCache(fmt.Sprintf("%d.%d", worker, i), InternalJobStatus{})
				storeJobStatusInCache("shared", InternalJobStatus{})
				getValidJobStatusFromCache("shared")
			}
		}(g)
	}
	wg.Wait()
}

// TestJobStatusCacheEvictsExpiredEntries verifies that the cache does not grow
// without bound. Entries used to be added and never removed, so a long running
// process which polls many different jobs retained a full InternalJobStatus,
// including every environment variable of the job, for every job id it ever
// queried.
func TestJobStatusCacheEvictsExpiredEntries(t *testing.T) {
	resetJobStatusCache(t)
	t.Cleanup(func() { resetJobStatusCache(t) })

	const expiredEntries = 100

	cacheMutex.Lock()
	for i := 0; i < expiredEntries; i++ {
		cachedJobStatus[fmt.Sprintf("expired%d", i)] = jobStatusCache{
			lastUpdate: time.Now().Add(-2 * cacheValidity),
		}
	}
	cacheMutex.Unlock()

	storeJobStatusInCache("fresh", InternalJobStatus{})

	cacheMutex.RLock()
	size := len(cachedJobStatus)
	_, freshFound := cachedJobStatus["fresh"]
	cacheMutex.RUnlock()

	if !freshFound {
		t.Errorf("The entry just stored is not in the cache.")
	}
	if size != 1 {
		t.Errorf("Cache holds %d entries after storing one fresh entry, expected 1. "+
			"%d expired entries were not reaped.", size, size-1)
	}
}

// TestJobStatusCacheExpiry checks that an entry older than cacheValidity is
// reported as a cache miss, so that a new qstat call is made.
func TestJobStatusCacheExpiry(t *testing.T) {
	resetJobStatusCache(t)
	t.Cleanup(func() { resetJobStatusCache(t) })

	storeJobStatusInCache("fresh", InternalJobStatus{})
	if _, found := getValidJobStatusFromCache("fresh"); !found {
		t.Errorf("A freshly stored entry is reported as a cache miss.")
	}

	cacheMutex.Lock()
	cachedJobStatus["stale"] = jobStatusCache{
		lastUpdate: time.Now().Add(-2 * cacheValidity),
	}
	cacheMutex.Unlock()

	if _, found := getValidJobStatusFromCache("stale"); found {
		t.Errorf("An entry older than %s is reported as a cache hit.", cacheValidity)
	}
	if _, found := getValidJobStatusFromCache("neverstored"); found {
		t.Errorf("An entry which was never stored is reported as a cache hit.")
	}
}
