/*
    Copyright 2012, 2013, 2014, 2015, 2025 Daniel Gruber, info@gridengine.eu

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
	"testing"
	"time"
)

// testVectorSetter calls a job template set function (f) with the given
// slice of values (like setting email addresses) and expects the output
// of the function g (like get email addresses) contains the same values
// as set before.
func testVectorSetter(t *testing.T, f func([]string) error, arg []string, g func() ([]string, error)) {
	if err := f(arg); err != nil {
		t.Error(err)
	} else {
		garg, gerr := g()
		if gerr != nil {
			t.Error(gerr)
		} else {
			if len(arg) != len(garg) {
				t.Error("Length of array set in the job template does not match length of retrieved values")
			} else {
				for i := range arg {
					if arg[i] != garg[i] {
						t.Errorf("Values are not the same: %s vs %s", arg[i], garg[i])
					}
				}
			}
		}
	}
}

func testStringSetter(t *testing.T, f func(string) error, arg string, g func() (string, error)) {
	if err := f(arg); err != nil {
		t.Error(err)
	} else {
		garg, gerr := g()
		if gerr != nil {
			t.Error(gerr)
		} else {
			if arg != garg {
				t.Errorf("Value is not the same: %s vs %s", arg, garg)
			}
		}
	}
}

func testDurationSetter(t *testing.T, f func(time.Duration) error, arg time.Duration, g func() (time.Duration, error)) {
	if err := f(arg); err != nil {
		t.Error(err)
	} else {
		garg, gerr := g()
		if gerr != nil {
			t.Error(gerr)
		} else {
			if arg.String() != garg.String() {
				t.Errorf("Value is not the same: %s vs %s", arg, garg)
			}
		}
	}
}

// skipIfAttributeUnsupported skips the running test when the DRM does not
// support the given job template attribute. Not all attributes of the DRMAA
// standard are optional, GetAttributeNames() reports what the underlying
// libdrmaa really implements. Open Cluster Scheduler and Grid Engine for
// example compile the optional duration limits out of the library, and they
// only offer drmaa_transfer_files when delegated_file_staging is set to true
// in the global configuration (qconf -mconf).
func skipIfAttributeUnsupported(t *testing.T, s *Session, attribute string) {
	t.Helper()

	names, err := s.GetAttributeNames()
	if err != nil {
		t.Fatalf("Error during GetAttributeNames(): %s", err)
	}
	for _, name := range names {
		if name == attribute {
			return
		}
	}
	t.Skipf("DRM does not support the %q job template attribute, supported are: %v",
		attribute, names)
}

func TestJobTemplate(t *testing.T) {
	if s, err := MakeSession(); err != nil {
		t.Fatalf("Error during MakeSession(): %s\n", err)
	} else {
		defer s.Exit()

		jt, errJT := s.AllocateJobTemplate()
		if errJT != nil {
			t.Fatalf("Error during AllocateJobTemplate(): %s\n", errJT)
		}

		// test vector attribute settings
		testVectorSetter(t, jt.SetArgs, []string{"arg1", "arg2"}, jt.Args)
		testVectorSetter(t, jt.SetEmail, []string{"a@b.c", "c@d.a"}, jt.Email)
		testVectorSetter(t, jt.SetEnv, []string{"LD_LIBRARY_PATH", "PATH"}, jt.Env)

		// test other attribute settings

		jt.SetJobSubmissionState(HoldState)
		if state, _ := jt.JobSubmissionState(); state != HoldState {
			t.Errorf("Error when setting JobSubmissionState.")
		}

		// test string attribute settings
		testStringSetter(t, jt.SetWD, "/", jt.WD)
		testStringSetter(t, jt.SetNativeSpecification, "-binding linear:1", jt.NativeSpecification)
		testStringSetter(t, jt.SetJobName, "MY_TEST_JOB", jt.JobName)

		// test duration attribute settings (they are not supported by SGE)
		if false {
			duration := time.Second * 77
			testDurationSetter(t, jt.SetHardWallclockTimeLimit, duration, jt.HardWallclockTimeLimit)
			testDurationSetter(t, jt.SetSoftWallclockTimeLimit, duration, jt.SoftWallclockTimeLimit)
			testDurationSetter(t, jt.SetHardRunDurationLimit, duration, jt.HardRunDurationLimit)
			testDurationSetter(t, jt.SetSoftRunDurationLimit, duration, jt.SoftRunDurationLimit)
		}

		if e := jt.SetBlockEmail(true); e != nil {
			t.Errorf("Error during SetBlockEmail(true): %s", e)
		} else {
			if em, e2 := jt.BlockEmail(); e2 != nil {
				t.Errorf("Error during BlockEmail(): %s", e2)
			} else {
				if em != true {
					t.Error("SetBlockEmail() set to true but BlocEmail() returns false")
				}
			}
		}
	}
}

// TestJoinFilesFix tests the bug fix for JoinFiles() method
// Previously it was using C.DRMAA_JOB_NAME instead of C.DRMAA_JOIN_FILES
func TestJoinFilesFix(t *testing.T) {
	if s, err := MakeSession(); err != nil {
		t.Fatalf("Error during MakeSession(): %s\n", err)
	} else {
		defer s.Exit()
		jt, errJT := s.AllocateJobTemplate()
		if errJT != nil {
			t.Fatalf("Error during AllocateJobTemplate(): %s\n", errJT)
		}
		defer s.DeleteJobTemplate(&jt)

		// Test setting JoinFiles to true
		if err := jt.SetJoinFiles(true); err != nil {
			t.Errorf("Error setting JoinFiles to true: %s", err)
		}

		// Test getting JoinFiles value - this should work correctly with the fix
		joined, err := jt.JoinFiles()
		if err != nil {
			t.Errorf("Error getting JoinFiles value: %s", err)
		} else if !joined {
			t.Error("JoinFiles should return true after being set to true")
		}

		// Test setting JoinFiles to false
		if err := jt.SetJoinFiles(false); err != nil {
			t.Errorf("Error setting JoinFiles to false: %s", err)
		}

		joined, err = jt.JoinFiles()
		if err != nil {
			t.Errorf("Error getting JoinFiles value after setting to false: %s", err)
		} else if joined {
			t.Error("JoinFiles should return false after being set to false")
		}
	}
}

// TestTransferFilesFix tests the bug fix for TransferFiles() method
// Previously it was returning after processing only the first character.
// This is optional and need to be enabled in Open Cluster Scheduler with
// qconf -mconf changing delegated_file_staging to "true"
func TestTransferFilesFix(t *testing.T) {
	if s, err := MakeSession(); err != nil {
		t.Fatalf("Error during MakeSession(): %s\n", err)
	} else {
		defer s.Exit()
		skipIfAttributeUnsupported(t, &s, "drmaa_transfer_files")

		jt, errJT := s.AllocateJobTemplate()
		if errJT != nil {
			t.Fatalf("Error during AllocateJobTemplate(): %s\n", errJT)
		}
		defer s.DeleteJobTemplate(&jt)

		// Test setting transfer modes for all streams
		mode := FileTransferMode{
			InputStream:  true,
			OutputStream: true,
			ErrorStream:  true,
		}

		if err := jt.SetTransferFiles(mode); err != nil {
			t.Errorf("Error setting TransferFiles (ensure if it is supported and delegated_file_staging is set to true in OCS): %s", err)
		}

		// Test getting transfer modes - this should process all characters with the fix
		retrievedMode, err := jt.TransferFiles()
		if err != nil {
			t.Errorf("Error getting TransferFiles: %s", err)
		} else {
			if !retrievedMode.InputStream {
				t.Error("InputStream should be true")
			}
			if !retrievedMode.OutputStream {
				t.Error("OutputStream should be true")
			}
			if !retrievedMode.ErrorStream {
				t.Error("ErrorStream should be true")
			}
		}

		// Test partial transfer modes
		partialMode := FileTransferMode{
			InputStream:  true,
			OutputStream: false,
			ErrorStream:  true,
		}

		if err := jt.SetTransferFiles(partialMode); err != nil {
			t.Errorf("Error setting partial TransferFiles: %s", err)
		}

		retrievedPartialMode, err := jt.TransferFiles()
		if err != nil {
			t.Errorf("Error getting partial TransferFiles: %s", err)
		} else {
			if !retrievedPartialMode.InputStream {
				t.Error("InputStream should be true in partial mode")
			}
			if retrievedPartialMode.OutputStream {
				t.Error("OutputStream should be false in partial mode")
			}
			if !retrievedPartialMode.ErrorStream {
				t.Error("ErrorStream should be true in partial mode")
			}
		}
	}
}

// TestSoftWallclockTimeLimitFix tests the bug fix for SoftWallclockTimeLimit() method
// Previously it was using C.DRMAA_WCT_HLIMIT instead of C.DRMAA_WCT_SLIMIT
func TestSoftWallclockTimeLimitFix(t *testing.T) {
	if s, err := MakeSession(); err != nil {
		t.Fatalf("Error during MakeSession(): %s\n", err)
	} else {
		defer s.Exit()
		skipIfAttributeUnsupported(t, &s, "drmaa_wct_slimit")

		jt, errJT := s.AllocateJobTemplate()
		if errJT != nil {
			t.Fatalf("Error during AllocateJobTemplate(): %s\n", errJT)
		}
		defer s.DeleteJobTemplate(&jt)

		// Test setting soft wallclock time limit
		testDuration := time.Second * 300 // 5 minutes
		if err := jt.SetSoftWallclockTimeLimit(testDuration); err != nil {
			t.Errorf("Error setting SoftWallclockTimeLimit: %s", err)
		}

		// Test getting soft wallclock time limit - this should work correctly with the fix
		retrievedDuration, err := jt.SoftWallclockTimeLimit()
		if err != nil {
			t.Errorf("Error getting SoftWallclockTimeLimit: %s", err)
		} else {
			if retrievedDuration.String() != testDuration.String() {
				t.Errorf("SoftWallclockTimeLimit mismatch: expected %s, got %s", testDuration, retrievedDuration)
			}
		}

		// Test with different duration to ensure consistency
		testDuration2 := time.Second * 600 // 10 minutes
		if err := jt.SetSoftWallclockTimeLimit(testDuration2); err != nil {
			t.Errorf("Error setting second SoftWallclockTimeLimit: %s", err)
		}

		retrievedDuration2, err := jt.SoftWallclockTimeLimit()
		if err != nil {
			t.Errorf("Error getting second SoftWallclockTimeLimit: %s", err)
		} else {
			if retrievedDuration2.String() != testDuration2.String() {
				t.Errorf("Second SoftWallclockTimeLimit mismatch: expected %s, got %s", testDuration2, retrievedDuration2)
			}
		}
	}
}

func TestGetVectorAttributeNames(t *testing.T) {
	if s, err := MakeSession(); err != nil {
		t.Fatalf("Error during MakeSession(): %s\n", err)
	} else {
		defer s.Exit()
		names, err := s.GetVectorAttributeNames()
		if err != nil {
			t.Errorf("Error during calling GetVectorAttributeNames(): %s", err)
		} else {
			t.Logf("Supported job template vector attributes: %v\n", names)
		}
	}
}

func TestGetAttributeNames(t *testing.T) {
	if s, err := MakeSession(); err != nil {
		t.Fatalf("Error during MakeSession(): %s\n", err)
	} else {
		defer s.Exit()
		names, err := s.GetAttributeNames()
		if err != nil {
			t.Errorf("Error during calling GetAttributeNames(): %s", err)
		} else {
			t.Logf("Supported job template attributes: %v\n", names)
		}
	}
}

// TestSimpleJobSubmission requires a connected cluster / DRM.
// Note that this test submits one job to the system running 10 seconds
// doing nothing.
func TestSimpleJobSubmission(t *testing.T) {
	if s, err := MakeSession(); err != nil {
		t.Fatalf("Error during MakeSession(): %s\n", err)
	} else {
		defer s.Exit()
		jt, errAlloc := s.AllocateJobTemplate()
		if errAlloc != nil {
			t.Fatalf("Failed allocating a new job template: %s", errAlloc)
		}
		defer s.DeleteJobTemplate(&jt)
		// /bin/sleep should be available (Linux / Unix system)
		jt.SetRemoteCommand("/bin/sleep")
		// runtime
		jt.SetArg("10")
		jt.SetJobName("TestSimpleJobSubmissionGoDRMAATestJob")
		id, errRun := s.RunJob(&jt)
		if errRun != nil {
			t.Fatalf("Error submitting job: %s", errRun)
		}
		errHold := s.HoldJob(id)
		if errHold != nil {
			t.Errorf("Error holding job: %s", errHold)
		}
		errRls := s.ReleaseJob(id)
		if errRls != nil {
			t.Errorf("Error releasing job: %s", errRls)
		}
		ps, errPs := s.JobPs(id)
		if errPs != nil {
			t.Errorf("Error during job status requests: %s", errPs)
		}
		if ps == PsRunning {
			t.Log("Job is running")
		} else {
			t.Logf("Job is in state %s", ps)
		}
		errTerm := s.TerminateJob(id)
		if errTerm != nil {
			t.Fatalf("Error terminating job %s: %s", id, errTerm)
		}
	}
}

// TestMakeUsageMap covers the conversion of the resource usage vector into a
// map. The entries come from the DRMAA library, so the function must not trust
// their shape. It used to do strings.Split(entry, "=") and index element 1
// unconditionally, which panicked inside Session.Wait() on any entry without a
// separator, and silently truncated any measurement which contained one.
//
// This is pure Go, it needs no session and no cluster.
func TestMakeUsageMap(t *testing.T) {
	tests := []struct {
		name  string
		usage []string
		want  map[string]string
	}{
		{
			name:  "single entry",
			usage: []string{"cpu=1.5"},
			want:  map[string]string{"cpu": "1.5"},
		},
		{
			name:  "several entries",
			usage: []string{"cpu=1.5", "mem=1024.0000", "wallclock=42"},
			want:  map[string]string{"cpu": "1.5", "mem": "1024.0000", "wallclock": "42"},
		},
		{
			name:  "measurement containing a separator is kept intact",
			usage: []string{"start_time=2026-08-09T07=40=17"},
			want:  map[string]string{"start_time": "2026-08-09T07=40=17"},
		},
		{
			name:  "entry without a separator is skipped",
			usage: []string{"cpu=1.5", "nonsense"},
			want:  map[string]string{"cpu": "1.5"},
		},
		{
			name:  "empty entry is skipped",
			usage: []string{"", "cpu=1.5", ""},
			want:  map[string]string{"cpu": "1.5"},
		},
		{
			name:  "empty measurement is kept",
			usage: []string{"cpu="},
			want:  map[string]string{"cpu": ""},
		},
		{
			name:  "no entries",
			usage: nil,
			want:  map[string]string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := makeUsageMap(test.usage)
			if got == nil {
				t.Fatalf("makeUsageMap() returned a nil map.")
			}
			if len(got) != len(test.want) {
				t.Errorf("makeUsageMap(%q) returned %d entries (%v), expected %d (%v).",
					test.usage, len(got), got, len(test.want), test.want)
			}
			for name, wantValue := range test.want {
				value, found := got[name]
				if !found {
					t.Errorf("makeUsageMap(%q) has no entry %q, got %v.",
						test.usage, name, got)
					continue
				}
				if value != wantValue {
					t.Errorf("makeUsageMap(%q)[%q] is %q, expected %q.",
						test.usage, name, value, wantValue)
				}
			}
		})
	}
}
