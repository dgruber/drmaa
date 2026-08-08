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
	"os"
	"os/exec"
	"testing"
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
