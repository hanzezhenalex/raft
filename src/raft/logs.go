package raft

import (
	"fmt"
)

type LogService struct {
	raft  *Raft
	store *Store

	lastLog      Log
	lastLogIndex int
	noOp         int
}

func NewLogService(raft *Raft, lastLog Log, lastLogIndex int, noOp int,
	logs []Log, lastIndexOfSnapshot int, snapshot []byte) *LogService {
	return &LogService{
		raft:         raft,
		store:        NewStore(logs, lastIndexOfSnapshot, snapshot),
		lastLogIndex: lastLogIndex,
		lastLog:      lastLog,
		noOp:         noOp,
	}
}

func (ls *LogService) AddCommand(command interface{}) int {
	if command == noOpCommand {
		ls.noOp++
	}
	newLog := Log{
		Term:    ls.raft.currentTerm,
		Command: command,
	}
	ls.lastLogIndex++
	ls.lastLog = newLog
	return ls.store.Append(newLog)
}

func (ls *LogService) AddLogs(logs []Log) {
	if len(logs) == 0 {
		return
	}
	for _, log := range logs {
		if log.Command == noOpCommand {
			ls.noOp++
		}
	}
	ls.lastLog = logs[len(logs)-1]
	ls.lastLogIndex += len(logs)
	ls.store.Append(logs...)
}

// #######################################
//
// #######################################

type Store struct {
	logs                []Log
	lastIndexOfSnapshot int
	snapshot            []byte
}

func NewStore(logs []Log, lastIndexOfSnapshot int, snapshot []byte) *Store {
	return &Store{
		logs:                logs,
		lastIndexOfSnapshot: lastIndexOfSnapshot,
		snapshot:            snapshot,
	}
}

func (s *Store) Length() int {
	return s.lastIndexOfSnapshot + 1 + len(s.logs)
}

func (s *Store) Append(logs ...Log) int {
	first := len(s.logs)
	s.logs = append(s.logs, logs...)
	return first
}

type GetLogsResult struct {
	Start    int
	Logs     []Log
	Snapshot []byte
}

// Get retrieve data from `left` to `right`, both included
// In return, Start is the first index of Logs
// Snapshot will be filled if there is data in snapshot
func (s *Store) Get(left, right int) GetLogsResult {
	if left < right {
		panic("left should be lower than right")
	}
	if right == -1 {
		right = s.fromLogIndex(len(s.logs) - 1)
	}
	if left < 0 || s.toLogIndex(right) >= len(s.logs) {
		panic("out of range")
	}
	var ret GetLogsResult
	left, right = s.toLogIndex(left), s.toLogIndex(right)
	if left < 0 {
		left = 0
		ret.Start = -1
		ret.Snapshot = s.snapshot
	}
	if right >= 0 {
		ret.Start = s.fromLogIndex(left)
		ret.Logs = s.logs[left : right+1]
	}
	return ret
}

// Trim the log, `end` included
func (s *Store) Trim(end int) {
	end = s.toLogIndex(end)
	if end < 0 {
		// todo log waring
		return
	}
	s.logs = s.logs[:end+1]
}

func (s *Store) BuildSnapshot(index int, snapshot []byte) {
	logsIndex := s.toLogIndex(index)
	if logsIndex < 0 {
		// todo log waring
		return
	}
	if logsIndex == len(s.logs)-1 {
		s.logs = s.logs[:0]
	} else {
		s.logs = s.logs[logsIndex+1:] // todo test
	}
	s.snapshot = snapshot
	s.lastIndexOfSnapshot = index
}

func (s *Store) toLogIndex(index int) int {
	index = index - s.lastIndexOfSnapshot - 1
	if index >= len(s.logs) {
		panic("out of range")
	}
	return index
}

func (s *Store) fromLogIndex(index int) int {
	if index < 0 {
		panic("out of range")
	}
	return index + s.lastIndexOfSnapshot + 1
}

type Logs struct {
	raft *Raft

	lastLog Log
	logs    []Log
	noOp    int

	lastIndexOfSnapshot int
	snapshot            []byte
	snapshotNoOp        int
}

func NewLogs(raft *Raft) *Logs {
	return &Logs{raft: raft, lastIndexOfSnapshot: -1}
}

func (l *Logs) AppendOne(command interface{}) int {
	newEntry := Log{
		Term:    l.raft.currentTerm,
		Command: command,
	}
	l.logs = append(l.logs, newEntry)
	if command == noOpCommand {
		l.noOp++
	}
	l.lastLog = newEntry
	return l.fromLogIndex(len(l.logs) - 1)
}

func (l *Logs) toLogIndex(index int) int {
	return index - l.lastIndexOfSnapshot - 1
}

func (l *Logs) fromLogIndex(index int) int {
	if index < 0 {
		panic(fmt.Sprintf("log index should upper than 0, actual=%d", index))
	}
	return index + l.lastIndexOfSnapshot + 1
}

func (l *Logs) AppendMany(commands []Log) {
	if len(commands) <= 0 {
		return
	}
	for _, log := range commands {
		if log.Command == noOpCommand {
			l.noOp++
		}
	}
	l.logs = append(l.logs, commands...)
	l.lastLog = l.logs[len(l.logs)-1]
}

func (l *Logs) ToNoOpIndex(index int) int {
	index = index - l.noOp - l.snapshotNoOp
	if index < 0 {
		panic(fmt.Sprintf("out of range, expected=%d", index))
	}
	return index
}

func (l *Logs) FromNoOpIndex(index int) int {
	return index + l.noOp + l.snapshotNoOp
}

func (l *Logs) Length() int {
	return l.lastIndexOfSnapshot + 1 + len(l.logs)
}

func (l *Logs) GetLastLog() (bool, int, Log) {
	if len(l.logs) == 0 && l.lastIndexOfSnapshot == -1 {
		return false, -1, Log{}
	}
	if len(l.logs) == 0 {
		return true, l.lastIndexOfSnapshot, l.lastLog
	}
	return true, l.fromLogIndex(len(l.logs) - 1), l.lastLog
}

func (l *Logs) Trim(end int) *Logs {
	end = l.toLogIndex(end)
	if end > len(l.logs)-1 {
		panic(fmt.Sprintf("out of range, expected: %d, actual: %d", end, len(l.logs)-1))
	}
	if end >= 0 {
		remove := l.logs[end:]
		for _, log := range remove {
			if log.Command == noOpCommand {
				l.noOp--
			}
		}
		l.logs = l.logs[:end]
	}
	return l
}

// RetrieveBackward try to fetch data from Logs, begin at `from`, trace backward, length=`expectedLength` (`from` is included)
// return value: start of the logs fetched
// if all the entries needed in snapshot, will return an empty slice
func (l *Logs) RetrieveBackward(from int, expectedLength int) (int, []Log) {
	if from == -1 {
		from = l.fromLogIndex(len(l.logs) - 1)
	}
	from = l.toLogIndex(from)
	if from > len(l.logs)-1 {
		panic(fmt.Sprintf("out of range, expected=%d, actual=%d", from, len(l.logs)-1))
	}
	if from < 0 {
		return -1, []Log{}
	}
	from++
	start := max(from-expectedLength, 0)
	return l.fromLogIndex(start), l.logs[start:from]
}

// RetrieveForward try to fetch data from Logs, begin at `from`, length=`expectedLength` (`from` is included)
// return value: start of the logs fetched, if it does not equal to `from`, that means at least some logs are in snapshot
// if all the entries needed in snapshot, will return an empty slice
func (l *Logs) RetrieveForward(from int, expectedLength int) (int, []Log) {
	if from >= len(l.logs) {
		panic(fmt.Sprintf("out of range, expected=%d, actual=%d", from, len(l.logs)))
	}
	end := min(from+expectedLength, len(l.logs))
	if from < 0 {
		from = 0
	}
	if end <= 0 {
		return -1, []Log{}
	}
	return from, l.logs[from:end]
}

func (l *Logs) Snapshot(index int, snapshot []byte) {
	i := l.toLogIndex(index)
	if i >= len(l.logs) || i < 0 {
		panic(fmt.Sprintf("out of range, expected:%d, actual:%d", i, len(l.logs)-1))
	}
	remove := l.logs[:i+1]
	for _, log := range remove {
		if log.Command == noOpCommand {
			l.noOp--
			l.snapshotNoOp++
		}
	}
	l.logs = l.logs[i+1:]
	l.snapshot = snapshot
	l.lastIndexOfSnapshot = i
}
