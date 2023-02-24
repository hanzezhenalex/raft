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
}

func enableDebug() {
	Debug = true
	logrus.SetLevel(logrus.InfoLevel)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		print("\n===================================================\n")
		fmt.Printf(format, a...)
		print("\n===================================================\n")
	}
	return
}
