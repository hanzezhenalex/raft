package raft

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var tracer = logrus.WithField("test", "test")

func makeRaft(n int) *Raft {
	raft := &Raft{
		tracer:    tracer,
		persister: MakePersister(),
		applier:   NewApplier(1, nil, nil),
	}
	raft.logs = NewLogService(raft, DefaultServiceState(), tracer)
	for i := 0; i < n; i++ {
		raft.logs.AddLogs([]Log{{i, fmt.Sprintf("%d", i)}})
	}
	return raft
}

type testCase struct {
	name                      string
	shouldSendInstallSnapshot bool

	// init status for replicator & raft
	logs             int
	snapshot         []byte
	lastIncludeIndex int

	// expected
	hasEntryToAppend bool

	// expected, specific for AppendEntries
	start, length int
	nextIndex     int
}

func TestReplicator_fillAppendEntries_matching(t *testing.T) {
	rq := require.New(t)

	matchingCases := []testCase{
		{name: "1", start: 0, length: 1, logs: 1, hasEntryToAppend: true},
		{name: "half of maxLogEntries", start: 0, length: maxLogEntries / 2, logs: maxLogEntries / 2, hasEntryToAppend: true},
		{name: "twice maxLogEntries", start: maxLogEntries, length: maxLogEntries, logs: maxLogEntries * 2, hasEntryToAppend: true},
		{name: "all in snapshot", logs: 10, lastIncludeIndex: 9, snapshot: []byte("test"), shouldSendInstallSnapshot: true, hasEntryToAppend: true},
		{
			name:                      "half in snapshot",
			logs:                      10,
			lastIncludeIndex:          5,
			snapshot:                  []byte("test"),
			shouldSendInstallSnapshot: false,
			hasEntryToAppend:          true,
			start:                     6,
			length:                    4,
		},
	}

	for _, c := range matchingCases {
		t.Run(c.name, func(t *testing.T) {
			raft := makeRaft(c.logs)
			if c.snapshot != nil {
				raft.logs.Snapshot(c.lastIncludeIndex, c.lastIncludeIndex, c.snapshot)
			}
			rep := NewReplicator(1, 0, nil, raft, nil, nil, func() {}, raft.logs.GetLastLogIndex())
			defer rep.stop()

			args, installSnapshotArgs, hasEntryToAppend := rep.fillRequest()

			rq.Equal(c.hasEntryToAppend, hasEntryToAppend)

			if c.shouldSendInstallSnapshot {
				rq.NotNil(installSnapshotArgs)
				rq.Equal(0, installSnapshotArgs.LeaderId)
				rq.Equal(c.lastIncludeIndex, installSnapshotArgs.LastIncludeIndex)
				rq.EqualValues(c.snapshot, installSnapshotArgs.data)
				// see makeRaft
				rq.Equal(0, installSnapshotArgs.Term)
				rq.Equal(c.lastIncludeIndex, installSnapshotArgs.LastIncludeTerm)
			} else {
				rq.Nil(installSnapshotArgs)
				rq.Equal(c.start, args.Offset)
				rq.Equal(c.length, len(args.Entries))
				rq.Equal(c.start, args.Entries[0].Term)
			}
		})
	}
}

func TestReplicator_fillAppendEntries_replicating(t *testing.T) {
	rq := require.New(t)

	replicatingCases := []testCase{
		{name: "1", start: 0, length: 1, logs: 1, nextIndex: 0, hasEntryToAppend: true},
		{name: "half maxLogEntries", start: 0, length: maxLogEntries / 2, logs: maxLogEntries / 2, nextIndex: 0, hasEntryToAppend: true},
		{name: "Offset from 3", start: 3, length: maxLogEntries/2 - 3, logs: maxLogEntries / 2, nextIndex: 4, hasEntryToAppend: true},
		{name: "more than maxLogEntries", start: 3, length: maxLogEntries, logs: maxLogEntries * 2, nextIndex: 4, hasEntryToAppend: true},
		{name: "no entry to append", start: 1, length: 0, logs: 1, nextIndex: 1, hasEntryToAppend: false},
		{name: "all in snapshot", logs: 10, lastIncludeIndex: 9, snapshot: []byte("test"), shouldSendInstallSnapshot: true, hasEntryToAppend: false},
		{name: "all in snapshot", logs: 10, lastIncludeIndex: 5, snapshot: []byte("test"), shouldSendInstallSnapshot: true, hasEntryToAppend: true},
	}

	for _, c := range replicatingCases {
		t.Run(c.name, func(t *testing.T) {
			raft := makeRaft(c.logs)

			rep := NewReplicator(1, 0, nil, raft, nil, nil, func() {}, c.nextIndex)
			if c.snapshot != nil {
				raft.logs.Snapshot(c.lastIncludeIndex, c.lastIncludeIndex, c.snapshot)
			}
			defer rep.stop()
			rep.status = replicating

			args, installSnapshotArgs, hasEntryToAppend := rep.fillRequest()

			rq.Equal(c.hasEntryToAppend, hasEntryToAppend)

			if c.shouldSendInstallSnapshot {
				rq.NotNil(installSnapshotArgs)
				rq.Equal(0, installSnapshotArgs.LeaderId)
				rq.Equal(c.lastIncludeIndex, installSnapshotArgs.LastIncludeIndex)
				rq.EqualValues(c.snapshot, installSnapshotArgs.data)
				// see makeRaft
				rq.Equal(0, installSnapshotArgs.Term)
				rq.Equal(c.lastIncludeIndex, installSnapshotArgs.LastIncludeTerm)
			} else {
				rq.Nil(installSnapshotArgs)
				rq.Equal(c.start, args.Offset)
				rq.Equal(c.length, len(args.Entries))
				if c.length > 0 {
					rq.Equal(c.start, args.Entries[0].Term)
				}
			}
		})
	}
}

type handleReplyTestCase struct {
	name            string
	nextIndex       int
	start           int
	success         bool
	expectNextIndex int
}

func TestReplicator_handleReply_matching(t *testing.T) {
	rq := require.New(t)

	cases := []handleReplyTestCase{
		{name: "not match", start: 10, success: false, expectNextIndex: 9},
		{name: "match", start: 10, success: true, expectNextIndex: 13},
		{name: "match", start: 0, success: true, expectNextIndex: 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raft := makeRaft(0)
			ch := make(chan received, 1)
			rep := NewReplicator(1, 0, nil, raft, tracer, ch, func() {}, c.nextIndex)
			defer rep.stop()

			rep.handleReply(AppendEntriesRequest{Offset: c.start}, &AppendEntriesReply{Success: c.success, Next: c.expectNextIndex})
			rq.Equal(c.expectNextIndex, rep.nextIndex)

			if c.success == true {
				r := <-ch
				rq.Equal(c.expectNextIndex-1, r.index)
				rq.Equal(replicating, rep.status)
			} else {
				rq.Equal(matching, rep.status)
			}
		})
	}

}

type tryAppendEntriesTestCases struct {
	logs             int
	name             string
	args             AppendEntriesRequest
	expectedReply    AppendEntriesReply
	expectedRaftLogs int
}

func TestRaft_tryAppendEntries(t *testing.T) {
	rq := require.New(t)

	cases := []tryAppendEntriesTestCases{
		{
			name: "init, both empty",
			logs: 0,
			args: AppendEntriesRequest{
				Offset:  0,
				Entries: []Log{}},
			expectedReply: AppendEntriesReply{
				Success: true,
				Next:    0,
			},
			expectedRaftLogs: 0,
		},
		{
			name: "init, leader empty",
			logs: 3,
			args: AppendEntriesRequest{
				Offset:  0,
				Entries: []Log{}},
			expectedReply: AppendEntriesReply{
				Success: true,
				Next:    0,
			},
			expectedRaftLogs: 3,
		},
		{
			name: "init",
			logs: 0,
			args: AppendEntriesRequest{
				Offset: 0,
				Entries: []Log{
					{Command: "0"},
					{Command: "1"},
					{Command: "2"},
					{Command: "3"},
				}},
			expectedReply: AppendEntriesReply{
				Success: true,
				Next:    4,
			},
			expectedRaftLogs: 4,
		},
		{
			name: "empty",
			logs: 5,
			args: AppendEntriesRequest{
				Offset:  0,
				Entries: []Log{}},
			expectedReply: AppendEntriesReply{
				Success: true,
				Next:    0,
			},
			expectedRaftLogs: 5,
		},
		{
			name: "no match",
			logs: 5,
			args: AppendEntriesRequest{
				Offset: 1,
				Entries: []Log{
					{Command: "-1"},
					{Command: "-2"},
					{Command: "-3"},
				}},
			expectedReply: AppendEntriesReply{
				Success: false,
				Next:    0,
			},
			expectedRaftLogs: 5,
		},
		{
			name: "no match, begin with 0",
			logs: 5,
			args: AppendEntriesRequest{
				Offset: 0,
				Entries: []Log{
					{Term: 11, Command: "0"},
					{Command: "-1"},
					{Command: "-2"},
				}},
			expectedReply: AppendEntriesReply{
				Success: true,
				Next:    3,
			},
			expectedRaftLogs: 3,
		},
		{
			name: "match all",
			logs: 5,
			args: AppendEntriesRequest{
				Offset: 2,
				Entries: []Log{
					{Term: 2, Command: "2"},
					{Term: 3, Command: "3"},
				}},
			expectedReply: AppendEntriesReply{
				Success: true,
				Next:    4,
			},
			expectedRaftLogs: 5,
		},
		{
			name: "entries more than logs",
			logs: 5,
			args: AppendEntriesRequest{
				Offset: 3,
				Entries: []Log{
					{Term: 3, Command: "3"},
					{Term: 4, Command: "4"},
					{Term: 5, Command: "5"},
					{Term: 6, Command: "6"},
					{Term: 7, Command: "7"},
				}},
			expectedReply: AppendEntriesReply{
				Success: true,
				Next:    8,
			},
			expectedRaftLogs: 8,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raft := makeRaft(c.logs)

			var reply AppendEntriesReply
			raft.tryAppendEntries(c.args, &reply)

			rq.Equal(c.expectedReply, reply)
			rq.Equal(c.expectedRaftLogs, raft.logs.GetLastLogIndex()+1)
		})
	}
}

func TestRaft_AppendEntries(t *testing.T) {
	rq := require.New(t)
	rf1 := makeRaft(4 * maxLogEntries)
	ch := make(chan received, 10)
	rep := NewReplicator(1, 0, nil, rf1, tracer, ch, func() {}, rf1.logs.GetLastLogIndex())

	rf2 := Raft{
		tracer:    tracer,
		persister: MakePersister(),
		applier:   NewApplier(1, nil, nil),
	}
	rf2.logs = NewLogService(&rf2, DefaultServiceState(), tracer)
	rf2.logs.AddLogs([]Log{
		{Term: 3, Command: "3"},
		{Term: 4, Command: "4"},
		{Term: 5, Command: "5"},
		{Term: 6, Command: "6"},
		{Term: 7, Command: "7"},
	})

	for {
		args, _, ok := rep.fillRequest()
		if !ok {
			break
		}
		var reply AppendEntriesReply

		rf2.tryAppendEntries(args, &reply)
		rep.handleReply(args, &reply)
	}

	rq.True(reflect.DeepEqual(rf1.logs.lastLogIndex, rf2.logs.lastLogIndex))

	// check commit
	for r := range ch {
		if r.index == 4*maxLogEntries-1 {
			return
		}
	}
	close(ch)
}

func TestRaft_AppendEntries_withSnapshot(t *testing.T) {
	rq := require.New(t)
	rf1 := makeRaft(4 * maxLogEntries)
	rf1.Snapshot(maxLogEntries, []byte("test"))
	ch := make(chan received, 10)
	rep := NewReplicator(1, 0, nil, rf1, tracer, ch, func() {}, rf1.logs.GetLastLogIndex())

	rf2 := Raft{
		tracer:    tracer,
		persister: MakePersister(),
		applier:   NewApplier(1, nil, nil),
	}
	rf2.logs = NewLogService(&rf2, DefaultServiceState(), tracer)
	rf2.logs.AddLogs([]Log{
		{Term: 3, Command: "3"},
		{Term: 4, Command: "4"},
		{Term: 5, Command: "5"},
		{Term: 6, Command: "6"},
		{Term: 7, Command: "7"},
	})

	for {
		args, installSnapshotArgs, ok := rep.fillRequest()
		if !ok {
			break
		}

		if installSnapshotArgs == nil {
			var reply AppendEntriesReply
			rf2.tryAppendEntries(args, &reply)
			rep.handleReply(args, &reply)
		} else {
			var reply InstallSnapshotReply
			rf2.logs.Snapshot(installSnapshotArgs.LastIncludeIndex, installSnapshotArgs.LastIncludeTerm, installSnapshotArgs.data)
			reply.Term = rf2.currentTerm
			rep.handleInstallSnapshotReply(*installSnapshotArgs, &reply)
		}
	}

	rq.True(reflect.DeepEqual(rf1.logs.lastLogIndex, rf2.logs.lastLogIndex))

	// check commit
	for r := range ch {
		if r.index == 4*maxLogEntries-1 {
			return
		}
	}
	close(ch)
}
