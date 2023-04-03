package raft

import (
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestApplier(t *testing.T) {
	rq := require.New(t)
	applyCh := make(chan ApplyMsg, 10)
	applier := NewApplier(0, applyCh, logrus.WithField("id", "1"))
	go applier.Daemon()
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < 7; i++ {
			msg, ok := <-applyCh
			rq.Equal(true, ok)
			rq.Equal(i, msg.Command)
		}
	}()

	applier.Apply(ApplyRequest{
		Start: 0,
		Logs: []Log{
			{Command: 0}, {Command: 1}, {Command: 2},
		},
	})

	applier.Apply(ApplyRequest{
		Start: 5,
		Logs: []Log{
			{Command: 5}, {Command: 6},
		},
	})

	applier.Apply(ApplyRequest{
		Start: 3,
		Logs: []Log{
			{Command: 3},
		},
	})

	applier.Apply(ApplyRequest{
		Start: 4,
		Logs: []Log{
			{Command: 4},
		},
	})

	wg.Wait()
	applier.Stop()
}
