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

func enableDebug() {
	Debug = true
	logrus.SetLevel(logrus.DebugLevel)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		print("\n===================================================\n")
		fmt.Printf(format, a...)
		print("\n===================================================\n")
	}
	return
}
