package raft

import (
	"fmt"
	"github.com/sirupsen/logrus"
)

// Debugging
var Debug bool

func init() {
	Debug = false
	logrus.SetLevel(logrus.FatalLevel)

	enableDebug()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func enableDebug() {
	Debug = true
	logrus.SetLevel(logrus.DebugLevel)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		print("\n===================================================\n")
		fmt.Printf(format, a...)
		print("\n===================================================\n")
	}
}
