package main

import (
	"fmt"
	"github.com/dgruber/drmaa"
	"os"
)

func main() {
	if s, err := drmaa.MakeSession(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	} else {
		s.Exit()
	}
}
