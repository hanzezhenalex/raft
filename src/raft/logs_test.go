package raft

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func NewEmptyStore() *Store {
	return &Store{lastIndexOfSnapshot: -1}
}

func shouldPanic(fn func(), rq *require.Assertions) {
	hasPanic := false
	defer func() {
		if r := recover(); r != nil {
			hasPanic = true
		}
		rq.Equal(true, hasPanic)
	}()
	fn()
}

func TestStore_Index(t *testing.T) {
	rq := require.New(t)

	t.Run("toLogIndex", func(t *testing.T) {
		t.Run("snapshot", func(t *testing.T) {
			s := NewEmptyStore()
			s.lastIndexOfSnapshot = 4
			s.logs = []Log{
				{Command: "1"}, {Command: "2"}, {Command: "3"},
			}
			rq.Equal(0, s.toLogIndex(5))
			rq.Equal(-1, s.toLogIndex(4))
		})

		t.Run("no snapshot", func(t *testing.T) {
			s := NewEmptyStore()
			s.logs = []Log{
				{Command: "1"}, {Command: "2"}, {Command: "3"}, {Command: "4"}, {Command: "5"}, {Command: "6"},
			}
			rq.Equal(5, s.toLogIndex(5))
			rq.Equal(4, s.toLogIndex(4))
		})
		t.Run("out of range", func(t *testing.T) {
			s := NewEmptyStore()
			shouldPanic(func() {
				s.toLogIndex(5)
			}, rq)
		})
	})

	t.Run("fromLogIndex", func(t *testing.T) {
		t.Run("snapshot", func(t *testing.T) {
			s := NewEmptyStore()
			s.lastIndexOfSnapshot = 4
			rq.Equal(10, s.fromLogIndex(5))
			rq.Equal(5, s.fromLogIndex(0))
		})

		t.Run("no snapshot", func(t *testing.T) {
			s := NewEmptyStore()
			rq.Equal(5, s.fromLogIndex(5))
			rq.Equal(4, s.fromLogIndex(4))
		})
	})
}

func TestStore_Append(t *testing.T) {
	rq := require.New(t)

	t.Run("no snapshot", func(t *testing.T) {
		var (
			index int
			s     = NewEmptyStore()
		)
		index = s.Append(Log{Command: "1"})
		rq.Equal(0, index)
		rq.Equal("1", s.logs[0].Command)

		index = s.Append(Log{Command: noOpCommand})
		rq.Equal(1, index)
		rq.Equal(noOpCommand, s.logs[1].Command)

		index = s.Append(Log{Command: "3"}, Log{Command: "4"})
		rq.Equal(2, index)
		rq.Equal("3", s.logs[2].Command)

		rq.Equal(4, s.Length())
		rq.Equal(4, len(s.logs))
	})

	t.Run("snapshot", func(t *testing.T) {
		var (
			index int
			s     = NewEmptyStore()
		)

		s.lastIndexOfSnapshot = 5

		index = s.Append(Log{Command: "1"})
		rq.Equal(6, index)
		rq.Equal("1", s.logs[0].Command)

		rq.Equal(1, len(s.logs))
		rq.Equal(7, s.Length())
	})
}

func TestStore_Get(t *testing.T) {
	rq := require.New(t)

	t.Run("no snapshot", func(t *testing.T) {
		s := NewEmptyStore()
		s.logs = []Log{
			{Command: "0"}, {Command: "1"}, {Command: "2"}, {Command: "3"}, {Command: "4"}, {Command: "5"}, {Command: "6"},
		}

		t.Run("get one", func(t *testing.T) {
			ret := s.Get(1, 1)
			rq.Equal(1, ret.Start)
			rq.Equal(1, len(ret.Logs))
			rq.Equal(0, len(ret.Snapshot))
			rq.Equal("1", ret.Logs[0].Command)
		})

		t.Run("get many", func(t *testing.T) {
			ret := s.Get(1, 3)
			rq.Equal(1, ret.Start)
			rq.Equal(3, len(ret.Logs))
			rq.Equal(0, len(ret.Snapshot))
			rq.Equal("1", ret.Logs[0].Command)
		})

		t.Run("left boundary", func(t *testing.T) {
			ret := s.Get(0, 3)
			rq.Equal(0, ret.Start)
			rq.Equal(4, len(ret.Logs))
			rq.Equal(0, len(ret.Snapshot))
			rq.Equal("0", ret.Logs[0].Command)
		})

		t.Run("right boundary", func(t *testing.T) {
			ret := s.Get(3, 6)
			rq.Equal(3, ret.Start)
			rq.Equal(4, len(ret.Logs))
			rq.Equal(0, len(ret.Snapshot))
			rq.Equal("6", ret.Logs[3].Command)
		})
	})

	t.Run("snapshot", func(t *testing.T) {
		s := NewEmptyStore()
		s.logs = []Log{
			{Command: "3"}, {Command: "4"}, {Command: "5"}, {Command: "6"},
		}
		s.lastIndexOfSnapshot = 2
		s.snapshot = []byte("snapshot")

		t.Run("get one", func(t *testing.T) {
			t.Run("in snapshot", func(t *testing.T) {
				ret := s.Get(1, 1)
				rq.Equal(-1, ret.Start)
				rq.Equal(0, len(ret.Logs))
				rq.EqualValues(s.snapshot, ret.Snapshot)
			})

			t.Run("not in snapshot", func(t *testing.T) {
				ret := s.Get(3, 3)
				rq.Equal(3, ret.Start)
				rq.Equal(1, len(ret.Logs))
				rq.Equal(0, len(ret.Snapshot))
				rq.Equal("3", ret.Logs[0].Command)
			})
		})

		t.Run("get many", func(t *testing.T) {
			t.Run("in snapshot", func(t *testing.T) {
				ret := s.Get(0, 2)
				rq.Equal(-1, ret.Start)
				rq.Equal(0, len(ret.Logs))
				rq.EqualValues(s.snapshot, ret.Snapshot)
			})

			t.Run("not in snapshot", func(t *testing.T) {
				ret := s.Get(3, 5)
				rq.Equal(3, ret.Start)
				rq.Equal(3, len(ret.Logs))
				rq.Equal(0, len(ret.Snapshot))
				rq.Equal("3", ret.Logs[0].Command)
			})

			t.Run("part in snapshot", func(t *testing.T) {
				ret := s.Get(1, 5)
				rq.Equal(3, ret.Start)
				rq.Equal(3, len(ret.Logs))
				rq.EqualValues(s.snapshot, ret.Snapshot)
				rq.Equal("3", ret.Logs[0].Command)
			})
		})
	})
}

func TestStore_Trim(t *testing.T) {
	rq := require.New(t)
	s := NewEmptyStore()

	t.Run("no snapshot", func(t *testing.T) {
		s.logs = []Log{
			{Command: "0"}, {Command: "1"}, {Command: "2"}, {Command: "3"}, {Command: "4"}, {Command: "5"}, {Command: "6"},
		}
		s.Trim(1)
		rq.Equal(2, s.Length())
		rq.Equal("0", s.logs[0].Command)
		rq.Equal("1", s.logs[1].Command)
	})

	t.Run("end not in snapshot", func(t *testing.T) {
		s.logs = []Log{
			{Command: "2"}, {Command: "3"},
		}
		s.lastIndexOfSnapshot = 1
		s.Trim(2)
		rq.Equal(3, s.Length())
		rq.Equal("2", s.logs[0].Command)
	})

	t.Run("end in snapshot", func(t *testing.T) {
		s.logs = []Log{
			{Command: "2"}, {Command: "3"},
		}
		s.lastIndexOfSnapshot = 1
		s.Trim(1)
		rq.Equal(2, s.Length())
		rq.Equal(0, len(s.logs))
	})
}

func TestStore_BuildSnapshot(t *testing.T) {
	rq := require.New(t)
	s := NewEmptyStore()

	s.logs = []Log{
		{Command: "0"}, {Command: "1"}, {Command: "2"}, {Command: "3"}, {Command: "4"}, {Command: "5"}, {Command: "6"},
	}

	// build snapshot
	s.BuildSnapshot(2, []byte("1"))
	rq.EqualValues([]byte("1"), s.snapshot)
	rq.Equal(4, len(s.logs))
	rq.Equal(7, s.Length())
	rq.Equal("3", s.logs[0].Command)
	rq.Equal("4", s.logs[1].Command)
	rq.Equal("5", s.logs[2].Command)
	rq.Equal("6", s.logs[3].Command)

	// index lower than snapshot, do not build
	s.BuildSnapshot(2, []byte("2"))
	rq.EqualValues([]byte("1"), s.snapshot)
	rq.Equal(4, len(s.logs))
	rq.Equal(7, s.Length())
	rq.Equal("3", s.logs[0].Command)
	rq.Equal("4", s.logs[1].Command)
	rq.Equal("5", s.logs[2].Command)
	rq.Equal("6", s.logs[3].Command)

	// build all
	s.BuildSnapshot(6, []byte("3"))
	rq.EqualValues([]byte("3"), s.snapshot)
	rq.Equal(0, len(s.logs))
	rq.Equal(7, s.Length())
}

func NewEmptyLogService() *LogService {
	return &LogService{
		raft:         &Raft{},
		lastLogIndex: -1,
	}
}

func TestLogService_AddCommand(t *testing.T) {
	rq := require.New(t)
	mockStore := NewMockLogStore(gomock.NewController(t))
	ls := NewEmptyLogService()
	ls.store = mockStore

	// test 1: Add non-noop log
	mockStore.EXPECT().Append(Log{
		Command: 0,
	}).Times(1).Return(0)

	ls.AddCommand(0)
	rq.Equal(0, ls.lastLogIndex)
	rq.Equal(0, ls.lastLog.Command)
	rq.Equal(0, ls.noOp)

	// test 2: Add noop log
	mockStore.EXPECT().Append(Log{
		Command: noOpCommand,
	}).Times(1).Return(0)

	ls.AddCommand(noOpCommand)
	rq.Equal(1, ls.lastLogIndex)
	rq.Equal(noOpCommand, ls.lastLog.Command)
	rq.Equal(1, ls.noOp)
}

func TestLogService_AddLogs(t *testing.T) {
	rq := require.New(t)
	mockStore := NewMockLogStore(gomock.NewController(t))
	ls := NewEmptyLogService()
	ls.store = mockStore
	logs := []Log{
		{Command: "0"}, {Command: "1"}, {Command: "2"}, {Command: "3"}, {Command: noOpCommand}, {Command: "5"}, {Command: "6"},
	}
	mockStore.EXPECT().Append(logs).Times(1).Return(0)

	ls.AddLogs(logs)
	rq.Equal(6, ls.lastLogIndex)
	rq.Equal("6", ls.lastLog.Command)
	rq.Equal(1, ls.noOp)
}

func TestLogService_Retrieve(t *testing.T) {
	mockStore := NewMockLogStore(gomock.NewController(t))
	ls := NewEmptyLogService()
	ls.store = mockStore

	// test 1: backward
	mockStore.EXPECT().Get(2, 4).Times(1)
	mockStore.EXPECT().Length().Return(100).Times(1)
	ls.RetrieveBackward(4, 3)

	// test 2: forward
	mockStore.EXPECT().Get(1, 3).Times(1)
	mockStore.EXPECT().Length().Return(100).Times(1)
	ls.RetrieveForward(1, 3)

	// test 2: forward, not enough
	mockStore.EXPECT().Get(1, 1).Times(1)
	mockStore.EXPECT().Length().Return(2).Times(1)
	ls.RetrieveForward(1, 3)
}

func TestLogService_Trim(t *testing.T) {
	rq := require.New(t)
	mockStore := NewMockLogStore(gomock.NewController(t))
	ls := NewEmptyLogService()
	ls.store = mockStore

	// test 1: removed contains noop
	mockStore.EXPECT().Length().Return(13).Times(3)
	mockStore.EXPECT().Get(11, 12).Return(GetLogsResult{
		Start: 11,
		Logs: []Log{
			{Command: 11}, {Command: noOpCommand},
		},
		Snapshot: nil,
	})
	mockStore.EXPECT().Get(10, 10).Return(GetLogsResult{
		Start:    10,
		Logs:     []Log{{Command: 10}},
		Snapshot: nil,
	})
	mockStore.EXPECT().Trim(10)
	ls.noOp = 1

	ls.Trim(10)
	rq.Equal(10, ls.lastLogIndex)
	rq.Equal(10, ls.lastLog.Command)
	rq.Equal(0, ls.noOp)

	// test 2: last log in snapshot
	mockStore.EXPECT().Length().Return(3).Times(3)
	mockStore.EXPECT().Get(1, 2).Return(GetLogsResult{
		Start: 1,
		Logs: []Log{
			{Command: 1}, {Command: 2},
		},
		Snapshot: nil,
	})
	mockStore.EXPECT().Get(0, 0).Return(GetLogsResult{
		Start:    -1,
		Snapshot: []byte("snapshot"),
	})
	mockStore.EXPECT().Trim(0)
	ls.noOp = 1
	ls.lastSnapshotLog.Command = "snapshot log"
	ls.lastSnapshotLogIndex = 0

	ls.Trim(0)
	rq.Equal(0, ls.lastLogIndex)
	rq.Equal("snapshot log", ls.lastLog.Command)
	rq.Equal(1, ls.noOp)
}
