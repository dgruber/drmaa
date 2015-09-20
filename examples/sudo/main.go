package main

import (
	"fmt"
	"github.com/dgruber/drmaa"
)

func main() {

	session, errInit := drmaa.MakeSession()
	if errInit != nil {
		fmt.Println("Couldn't create a session.")
		panic(errInit)
	}
	defer session.Exit()

	jt, errJt := session.AllocateJobTemplate()
	if errJt != nil {
		fmt.Println("Error creating a job template.")
		panic(errJt)
	}
	jt.SetRemoteCommand("/bin/sleep")
	jt.SetArgs([]string{"123"})

	// we want to run a job as root
	var sudo drmaa.Sudo
	sudo.Username = "root"
	sudo.Groupname = "root"
	sudo.GID = 0
	sudo.UID = 0

	// This only works if the current user is in sudoers list in Univa Grid Engine 8.3
	// and it is allowed to run as root (just an example here you really want to
	// run the jobs as other normal users!).

	// Example configuration in Univa Grid Engine. Add service owner (here daniel)
	// to sudomasters list. Add the user names for which the service owner is allowed
	// to submit in the sudoers list.
	// qconf -au daniel sudomasters
	// qconf -au root sudoers

	// submit as user daniel but run it as root
	jobid, errRun := session.RunJobAs(sudo, &jt)

	if errRun != nil {
		panic(errRun)
	}

	fmt.Printf("Job submitted as %s with ID: %s\n", sudo.Username, jobid)
}
