package raft

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
// Store supports snapshot
// #######################################

type Store struct {
	logs []Log

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
	return s.fromLogIndex(first)
}

type GetLogsResult struct {
	Start    int
	Logs     []Log
	Snapshot []byte
}

// Get retrieve data from `left` to `right`, both included;
// In return, `Start` is the first index of `Logs`, -1 if all data in snapshot;
// `Snapshot` will be filled if data in snapshot
func (s *Store) Get(left, right int) GetLogsResult {
	if left > right {
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
