package raft

import (
	"fmt"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		print("\n===================================================\n")
		fmt.Printf(format, a...)
		print("\n===================================================\n")
	}
	return
}
