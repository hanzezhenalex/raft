package raft

import (
	"bytes"
	"fmt"
	"os"

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

func assert(cond bool, format string, args ...interface{}) {
	if !cond {
		panic(fmt.Sprintf(format, args...))
	}
}

func Fatalf(format string, msg ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, format, msg...)
	fmt.Print(logBuffer.String())
	os.Exit(1)
}
