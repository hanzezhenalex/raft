package raft

import "github.com/sirupsen/logrus"

type GetLogsResult struct {
	Start    int
	Logs     []Log
	Snapshot []byte
}

type LogState struct {
	Logs                []Log
	LastIndexOfSnapshot int
	Snapshot            []byte
}

type ServiceState struct {
	LogState
	LastLog      Log
	LastLogIndex int
	NoOp         int
}

func DefaultServiceState() ServiceState {
	return ServiceState{
		LogState: LogState{
			LastIndexOfSnapshot: -1,
		},
		LastLogIndex: -1,
	}
}

type LogStore interface {
	Length() int
	Append(logs ...Log) int
	Get(left, right int) GetLogsResult
	Trim(end int)
	BuildSnapshot(index int, snapshot []byte)
	GetState() LogState
}

type LogService struct {
	raft   *Raft
	store  LogStore
	tracer *logrus.Entry

	lastLog              Log
	lastLogIndex         int
	lastSnapshotLog      Log
	lastSnapshotLogIndex int
	noOp                 int
}

func NewLogService(raft *Raft, state ServiceState, tracer *logrus.Entry) *LogService {
	return &LogService{
		raft:         raft,
		store:        NewStore(state.LogState),
		lastLogIndex: state.LastLogIndex,
		lastLog:      state.LastLog,
		noOp:         state.NoOp,
		tracer:       tracer,
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

func (ls *LogService) RetrieveForward(start int, length int) GetLogsResult {
	end := min(start+length-1, ls.store.Length()-1)
	return ls.store.Get(start, end)
}

func (ls *LogService) RetrieveBackward(end int, length int) GetLogsResult {
	start := end - length + 1
	if start < 0 {
		panic("out of range")
	}
	return ls.store.Get(start, end)
}

func (ls *LogService) GetState() ServiceState {
	return ServiceState{
		LogState:     ls.store.GetState(),
		LastLog:      ls.lastLog,
		LastLogIndex: ls.lastLogIndex,
		NoOp:         ls.noOp,
	}
}

func (ls *LogService) Trim(end int) {
	if end > ls.store.Length() {
		panic("out of range")
	}
	// update noOp
	if end+1 < ls.store.Length() {
		toRemove := ls.store.Get(end+1, -1)
		if toRemove.Snapshot != nil {
			// todo log warning
			panic("should not trim committed logs")
		}
		for _, log := range toRemove.Logs {
			if log.Command == noOpCommand {
				ls.noOp--
			}
		}
	}
	// update lastLog
	ret := ls.store.Get(end, end)
	if ret.Snapshot == nil {
		ls.lastLogIndex = ret.Start
		ls.lastLog = ret.Logs[0]
	} else {
		ls.lastLogIndex = ls.lastSnapshotLogIndex
		ls.lastLog = ls.lastSnapshotLog
	}
	// do trim
	ls.store.Trim(end)
}

func (ls *LogService) Snapshot(index int, snapshot []byte) {
	ret := ls.store.Get(index, index)
	if ret.Snapshot != nil {
		return
	}
	ls.lastSnapshotLogIndex = ret.Start
	ls.lastSnapshotLog = ret.Logs[0]
	ls.store.BuildSnapshot(index, snapshot)
}

func (ls *LogService) FromNoOpIndex(index int) int {
	return index + ls.noOp
}

func (ls *LogService) ToNoOpIndex(index int) int {
	index = index - ls.noOp
	if index < 0 {
		panic("out of range")
	}
	return index
}

func (ls *LogService) IsPeerLogAhead(args RequestVoteArgs) bool {
	if ls.lastLogIndex == -1 {
		return true
	}
	return args.LastLogTerm > ls.lastLog.Term ||
		(args.LastLogTerm == ls.lastLog.Term && args.LastLogIndex >= ls.lastLogIndex)
}

// #######################################
// Store supports snapshot
// #######################################

type Store struct {
	logs []Log

	lastIndexOfSnapshot int
	snapshot            []byte
}

func NewStore(state LogState) *Store {
	return &Store{
		logs:                state.Logs,
		lastIndexOfSnapshot: state.LastIndexOfSnapshot,
		snapshot:            state.Snapshot,
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

func (s *Store) GetState() LogState {
	return LogState{
		Logs:                s.logs,
		LastIndexOfSnapshot: s.lastIndexOfSnapshot,
		Snapshot:            s.snapshot,
	}
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
