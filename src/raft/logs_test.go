package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func NewEmptyStore() *Store {
	return &Store{lastIndexOfSnapshot: -1}
}

func TestLogs_Index(t *testing.T) {
	logs := NewLogs(&Raft{})
	rq := require.New(t)

	t.Run("toLogIndex", func(t *testing.T) {
		t.Run("snapshot", func(t *testing.T) {
			logs.lastIndexOfSnapshot = 4
			rq.Equal(0, logs.toLogIndex(5))
			rq.Equal(-1, logs.toLogIndex(4))
		})

		t.Run("no snapshot", func(t *testing.T) {
			logs.lastIndexOfSnapshot = -1
			rq.Equal(5, logs.toLogIndex(5))
			rq.Equal(4, logs.toLogIndex(4))
		})
	})

	t.Run("fromLogIndex", func(t *testing.T) {
		t.Run("snapshot", func(t *testing.T) {
			logs.lastIndexOfSnapshot = 4
			rq.Equal(10, logs.fromLogIndex(5))
			rq.Equal(5, logs.fromLogIndex(0))
		})

		t.Run("no snapshot", func(t *testing.T) {
			logs.lastIndexOfSnapshot = -1
			rq.Equal(5, logs.fromLogIndex(5))
			rq.Equal(4, logs.fromLogIndex(4))
		})
	})

	t.Run("ToNoOpIndex", func(t *testing.T) {
		t.Run("snapshot", func(t *testing.T) {
			logs.noOp = 1
			logs.snapshotNoOp = 1
			rq.Equal(3, logs.ToNoOpIndex(5))
			rq.Equal(2, logs.ToNoOpIndex(4))
		})

		t.Run("no snapshot", func(t *testing.T) {
			logs.noOp = 1
			logs.snapshotNoOp = 0
			rq.Equal(4, logs.ToNoOpIndex(5))
			rq.Equal(3, logs.ToNoOpIndex(4))
		})
	})

	t.Run("FromNoOpIndex", func(t *testing.T) {
		t.Run("snapshot", func(t *testing.T) {
			logs.noOp = 1
			logs.snapshotNoOp = 1
			rq.Equal(5, logs.FromNoOpIndex(3))
			rq.Equal(4, logs.FromNoOpIndex(2))
		})

		t.Run("no snapshot", func(t *testing.T) {
			logs.noOp = 1
			logs.snapshotNoOp = 0
			rq.Equal(6, logs.FromNoOpIndex(5))
			rq.Equal(5, logs.FromNoOpIndex(4))
		})
	})
}

func TestLogs_Append(t *testing.T) {
	rq := require.New(t)

	t.Run("append one", func(t *testing.T) {
		t.Run("no snapshot", func(t *testing.T) {
			var (
				index int
				logs  = NewLogs(&Raft{})
			)
			index = logs.AppendOne("1")
			rq.Equal(0, index)
			rq.Equal("1", logs.lastLog.Command)

			index = logs.AppendOne(noOpCommand)
			rq.Equal(1, index)
			rq.Equal(noOpCommand, logs.lastLog.Command)

			index = logs.AppendOne("2")
			rq.Equal(2, index)
			rq.Equal("2", logs.lastLog.Command)

			rq.Equal(3, logs.Length())
			rq.Equal(3, len(logs.logs))
			rq.Equal(1, logs.noOp)
		})

		t.Run("snapshot", func(t *testing.T) {
			var (
				index int
				logs  = NewLogs(&Raft{})
			)

			logs.lastIndexOfSnapshot = 5
			logs.snapshotNoOp = 1

			index = logs.AppendOne("1")
			rq.Equal(6, index)
			rq.Equal("1", logs.lastLog.Command)

			index = logs.AppendOne(noOpCommand)
			rq.Equal(7, index)
			rq.Equal(noOpCommand, logs.lastLog.Command)

			index = logs.AppendOne("2")
			rq.Equal(8, index)
			rq.Equal("2", logs.lastLog.Command)

			rq.Equal(3, len(logs.logs))
			rq.Equal(1, logs.noOp)
			rq.Equal(9, logs.Length())
		})
	})

	t.Run("append many", func(t *testing.T) {
		var (
			index int
			logs  = NewLogs(&Raft{})
		)
		logs.AppendMany([]Log{
			{
				Command: "1",
			},
			{
				Command: "2",
			},
			{
				Command: "3",
			},
			{
				Command: "4",
			},
		})
		rq.Equal(4, len(logs.logs))
		rq.Equal("4", logs.lastLog.Command)

		logs.AppendMany([]Log{
			{
				Command: "5",
			},
			{
				Command: "6",
			},
			{
				Command: noOpCommand,
			},
			{
				Command: "8",
			},
		})
		rq.Equal(8, len(logs.logs))
		rq.Equal("8", logs.lastLog.Command)

		index = logs.AppendOne("9")
		rq.Equal(8, index)
	})
}

func TestLogs_RangeOperation(t *testing.T) {

}
