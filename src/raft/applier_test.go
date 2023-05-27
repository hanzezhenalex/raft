package raft

import (
	"sync"
	"testing"
	"time"

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

func TestApplier_ApplySnapshot(t *testing.T) {
	rq := require.New(t)
	applyCh := make(chan ApplyMsg, 10)
	applier := NewApplier(0, applyCh, logrus.WithField("id", "TestApplier_Snapshot"))
	go applier.Daemon()
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		snapshot, ok := <-applyCh
		rq.Equal(true, ok)
		rq.Equal(true, snapshot.SnapshotValid)
		rq.Equal(2, snapshot.SnapshotIndex)

		for i := 5; i < 7; i++ {
			msg, ok := <-applyCh
			rq.Equal(true, ok)
			rq.Equal(i, msg.Command)
			rq.Equal(i-2, msg.CommandIndex)
		}
	}()

	applier.Apply(ApplyRequest{
		Start: 3,
		Logs: []Log{
			{Command: 3}, {Command: 4}, {Command: 5}, {Command: 6},
		},
	})

	time.Sleep(20 * time.Millisecond)

	applier.ApplySnapshot(ApplySnapshotRequest{
		Term:             1,
		LastIncludeIndex: 4,
		LastIncludeNoops: 2,
		Data:             []byte("TestApplier_Snapshot"),
	})

	wg.Wait()
	applier.Stop()
}

func TestApplier_adjustReq(t *testing.T) {
	rq := require.New(t)
	applier := NewApplier(0, nil, nil)

	applier.nextIndex = 3

	req := applier.adjustReq(ApplyRequest{
		Start: 2,
		Logs: []Log{
			{Command: 2}, {Command: 3}, {Command: 4},
		},
	})

	rq.Equal(3, req.Start)
	rq.Equal(2, len(req.Logs))
}
