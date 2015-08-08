package drmaa

import (
	"fmt"
	"testing"
)

func TestJobTemplate(t *testing.T) {
	if s, err := MakeSession(); err != nil {
		t.Errorf("Error during MakeSession(): %s\n", err)
		t.Fail()
	} else {
		jt, err2 := s.AllocateJobTemplate()
		if err2 != nil {
			t.Errorf("Error during AllocateJobTemplate(): %s\n", err2)
			t.Fail()
			return
		}
		jt.SetArg("ARG")
		// TODO get args is missing
		email := make([]string, 2, 2)
		email[0] = "a@mail1org"
		email[1] = "b@mail.org"
		jt.SetEmail(email)
		// TODO get email method is missing
		if e := jt.SetBlockEmail(true); e != nil {
			t.Errorf("Error during SetBlockEmail(true)", e)
		} else {
			if em, e2 := jt.BlockEmail(); e2 != nil {
				t.Errorf("Error during BlockEmail(): %s\n", e2)
			} else {
				if em != true {
					t.Error("SetBlockEmail() set to true but BlocEmail() returns false")
				}
			}
		}
		fmt.Println(jt)
	}
}
