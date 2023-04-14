package raft

import (
	"bytes"
	"fmt"

	"github.com/sirupsen/logrus"
)

var (
	noOpCommand = "no-op"

	logBuffer = new(bytes.Buffer)
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(logBuffer)
}

func DPrintf(format string, a ...interface{}) {
	printf := func(format string, a ...interface{}) {
		_, _ = fmt.Fprintf(logBuffer, format, a...)
	}
	printBounder := func() {
		_, _ = fmt.Fprintln(logBuffer, "====================================================")

	}

	printBounder()
	printf(format, a...)
	printBounder()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
