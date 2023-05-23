package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   Offset agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"

	"github.com/sirupsen/logrus"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int

	Peer int
}

type Config struct {
	electionTimeout time.Duration
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu           sync.RWMutex        // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	resetTimerCh chan struct{}
	electionCnt  int
	applier      *Applier

	tracer             *logrus.Entry
	config             Config
	replicationService *ReplicationService
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all server
	currentTerm int
	voteFor     int
	logs        *LogService
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	// Volatile state on leaders
	isLeader   bool
	matchIndex int
}

type Log struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm) // assume no error
	e.Encode(rf.voteFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.logs.GetState())

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

	rf.printStatus("persist")
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	state := DefaultServiceState()
	if data == nil || len(data) < 1 {
		// bootstrap without any state?
		rf.logs = NewLogService(rf, state, rf.tracer.WithField("role", "log service"))
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.commitIndex)
	d.Decode(&state)

	rf.logs = NewLogService(rf, state, rf.tracer.WithField("role", "log service"))
	if rf.commitIndex >= 0 {
		ret := rf.logs.Get(0, rf.commitIndex)
		if ret.Snapshot != nil {
			// handle snapshot
		}
		rf.applier.Apply(ApplyRequest{
			Start: ret.Start,
			Logs:  ret.Logs,
		})
	}
	rf.printStatus("recover")
}

func (rf *Raft) printStatus(prefix string) {
	status := fmt.Sprintf("term=%d, last log=%d, dcommitIndex=%d, lastApplied=%d, leader=%t, snapshot=%d",
		rf.currentTerm, rf.logs.lastLogIndex, rf.commitIndex, rf.applier.lastApplied, rf.isLeader, rf.logs.lastSnapshotLogIndex)
	rf.tracer.Debugf("[%s]raft status: %s", prefix, status)
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.tracer.Debugf("install snapshot, index=%d", index)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	assert(index > rf.logs.lastSnapshotLogIndex, "new snapshot index should be newer")
	assert(index <= rf.logs.lastLogIndex, "snapshot index out of range")

	ret := rf.logs.Get(index, index)

	rf.logs.Snapshot(index, ret.Logs[0].Term, snapshot)
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must Offset with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must Offset with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		return
	}

	// update currentTerm
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term, true)
		rf.voteFor = -1
		rf.stopLeader()
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		rf.logs.IsPeerLogAhead(args) {
		rf.voteFor = args.CandidateId
		rf.stopLeader()

		reply.VoteGranted = true
		rf.resetTimer()
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) updateTerm(new int, locked bool) {
	if !locked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	if rf.currentTerm < new {
		rf.currentTerm = new
		rf.persist()
	}
}

func (rf *Raft) commit(newCommitIndex int, locked bool) {
	if !locked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	if rf.commitIndex < newCommitIndex {
		if newCommitIndex > rf.logs.lastLogIndex {
			newCommitIndex = rf.logs.lastLogIndex
		}
		ret := rf.logs.Get(rf.commitIndex+1, newCommitIndex)
		lastIncludeTerm := rf.logs.lastSnapshotLogTerm
		lastIncludeIndex := rf.logs.lastSnapshotLogIndex
		go func() {
			if ret.Snapshot != nil {
				rf.applier.ApplySnapshot(ApplySnapshotRequest{
					Term:             lastIncludeTerm,
					LastIncludeIndex: lastIncludeIndex,
					Data:             ret.Snapshot,
				})
			}
			if len(ret.Logs) > 0 {
				rf.applier.Apply(ApplyRequest{
					Start: ret.Start,
					Logs:  ret.Logs, //[rf.commitIndex+1 : newCommitIndex+1]
				})
			}
		}()
		rf.commitIndex = newCommitIndex
		rf.persist()
	}
}

func (rf *Raft) commitSnapshot(term, lastIncludeIndex int, data []byte) {
	rf.applier.ApplySnapshot(ApplySnapshotRequest{
		Term:             term,
		LastIncludeIndex: lastIncludeIndex,
		Data:             data,
	})
	rf.commitIndex = max(lastIncludeIndex, rf.commitIndex)
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	Offset       int
	FirstLogTerm int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Next    int
}

func (rf *Raft) AppendEntries(args AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.tracer.Debugf("receive AppendEntries from %d, offset=%d, logs=%#v, term=%d",
		args.LeaderId, args.Offset, args.Entries, args.Term)

	rf.mu.Lock()
	defer func() {
		rf.updateTerm(args.Term, true)
		rf.mu.Unlock()
		reply.Term = rf.currentTerm
	}()

	if args.Term < rf.currentTerm {
		rf.tracer.Debugf("AppendEntry rejected, argsTerm=%d, currentTerm=%d", args.Term, rf.currentTerm)
		reply.Next = args.Offset + len(args.Entries) - 1
		return
	}

	rf.resetTimer()
	rf.stopLeader()
	rf.voteFor = -1

	rf.tryAppendEntries(args, reply)
}

func (rf *Raft) tryAppendEntries(args AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.tracer.Debug("try append entries")

	if len(args.Entries) == 0 {
		rf.tracer.Debug("receive heartbeat message")
		reply.Success = true
		reply.Next = args.Offset
		rf.commit(args.LeaderCommit, true)
		return
	}

	argsLogIndexToLogServiceIndex := func(i int) int {
		return args.Offset + i
	}
	logServiceIndexToArgsLogIndex := func(i int) int {
		return i - args.Offset
	}

	ret := rf.logs.RetrieveForward(args.Offset, len(args.Entries))
	rf.tracer.Debugf("correspnding logs: %#v", ret)

	retLogIndexToArgsLogIndex := func(i int) int {
		ahead := ret.Start - args.Offset
		return ahead + i
	}
	retLogIndexToLogServiceIndex := func(i int) int {
		return ret.Start + i
	}

	match := len(ret.Logs) - 1 // the first index of log that match

	if len(ret.Logs) > 0 {
		// try to march log from end
		for ; match >= 0; match-- {
			myLog := ret.Logs[match]
			leaderLog := args.Entries[retLogIndexToArgsLogIndex(match)]
			if myLog.Term == leaderLog.Term {
				break
			}
		}
	}

	if ret.Snapshot != nil && match == -1 {
		lastIncludeIndex := logServiceIndexToArgsLogIndex(rf.logs.lastSnapshotLogIndex)
		if rf.logs.lastSnapshotLogTerm == args.Entries[lastIncludeIndex].Term {
			match = lastIncludeIndex
		}
	}

	// possibility when match >= 0:
	// 1) log matches, index = match, append at match+1
	// 2) snapshot exists, match = last snapshot index, append at match+1
	if match >= 0 {
		rf.tracer.Debugf("log matched, index=%d", retLogIndexToLogServiceIndex(match))

		offset := retLogIndexToArgsLogIndex(match) + 1
		if offset < len(args.Entries) { // in case args.Entries are empty
			rf.logs.Trim(retLogIndexToLogServiceIndex(match)).AddLogs(args.Entries[offset:])
			rf.persist()
		}

		reply.Success = true
		reply.Next = argsLogIndexToLogServiceIndex(len(args.Entries))

		rf.commit(min(args.LeaderCommit, argsLogIndexToLogServiceIndex(len(args.Entries)-1)), true)
	} else if args.Offset == 0 && ret.Snapshot == nil {
		rf.tracer.Debugf("no log matched, still append, args offset=0")

		rf.logs.Trim(-1).AddLogs(args.Entries)
		rf.persist()

		reply.Success = true
		reply.Next = len(args.Entries)

		rf.commit(min(args.LeaderCommit, len(args.Entries)-1), true)
	} else {
		// no log matched, no snapshot
		reply.Next = max(args.Offset-1, 0)
	}
}

// the service using Raft (e.g. a k/v server) wants to Offset
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise Offset the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()
	if !rf.isLeader {
		return -1, -1, rf.isLeader
	}
	rf.tracer.Debugf("receive a message, command=%#v", command)
	index := rf.logs.AddCommand(command)
	return rf.logs.ToNoOpIndex(index), rf.currentTerm, rf.isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.stopLeader()
	rf.applier.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	timer := time.NewTimer(rf.config.electionTimeout)

	rf.tracer.Debugf("raft start with election timeout = %s", rf.config.electionTimeout.String())

	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.printStatus("loop")
		select {
		case <-timer.C:
			go rf.handleTimeout()
		case <-rf.resetTimerCh:
			rf.tracer.Debug("reset timer")
		}
		timer.Reset(rf.config.electionTimeout)
	}
}

func (rf *Raft) handleTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader {
		rf.tracer.Debug("leader ignore the heartbeat, will be done by replicator")
	} else {
		rf.tracer.Debug("timeout, start election")
		go rf.election()
	}
}

func (rf *Raft) resetTimer() {
	rf.resetTimerCh <- struct{}{}
}

func (rf *Raft) fillRequestVotesArgs() (int, RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.voteFor = rf.me
	rf.electionCnt++
	rf.currentTerm++

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logs.GetLastLogIndex(),
		LastLogTerm:  rf.logs.GetLastLogTerm(),
	}
	return rf.electionCnt, args
}

func (rf *Raft) handleRequestVote(peer int, tracer *logrus.Entry, args RequestVoteArgs,
	voteCh chan struct{}, wg *sync.WaitGroup) {
	var (
		ok        = false
		reply     RequestVoteReply
		subTracer = tracer.WithField("peer", peer)
	)
	defer func() { wg.Done() }()

	DoWithTimeout(func() {
		subTracer.Debugf("send rpc requestVote")
		ok = rf.sendRequestVote(peer, args, &reply)
	}, rf.config.electionTimeout)

	if !ok {
		subTracer.Debugf("timeout or peer disconnect when waiting for vote")
		return
	}

	defer rf.updateTerm(reply.Term, false)

	if reply.VoteGranted {
		subTracer.Debug("election granted")
		voteCh <- struct{}{}
	} else {
		subTracer.Debug("grant forbidden")
	}
}

func (rf *Raft) election() {
	currentElectionCnt, args := rf.fillRequestVotesArgs()

	var (
		votes     = 1
		peers     = len(rf.peers)
		wg        sync.WaitGroup
		replyCh   = make(chan struct{}, peers)
		subTracer = rf.tracer.WithField("Term", args.Term).WithField("cnt", currentElectionCnt)
	)

	wg.Add(peers)

	go func() { wg.Wait(); close(replyCh) }()

	for idx, _ := range rf.peers {
		idx := idx
		if idx == rf.me {
			continue
		}
		go rf.handleRequestVote(idx, subTracer, args, replyCh, &wg)
	}

	for {
		_, ok := <-replyCh
		if !ok {
			break
		}
		if votes++; votes > peers/2 {
			subTracer.Debug("win the election")
			rf.mu.Lock()
			if rf.electionCnt == currentElectionCnt {
				rf.transferToLeader()
			}
			rf.mu.Unlock()
			break
		}
	}
}

type InstallSnapshotRequest struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Offset           int
	Data             []byte
	Done             bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || args.LastIncludeIndex < rf.logs.lastSnapshotLogIndex {
		return
	}
	rf.resetTimer()
	rf.logs.Snapshot(args.LastIncludeIndex, args.LastIncludeTerm, args.Data)
	rf.commitSnapshot(args.Term, args.LastIncludeIndex, args.Data)
	rf.persist()
}

func (rf *Raft) transferToLeader() {
	subTracer := rf.tracer.WithField("Term", rf.currentTerm)
	subTracer.Debug("change to leader, start heart beat")

	rf.isLeader = true
	rf.voteFor = -1
	rf.logs.AddCommand(noOpCommand)
	rf.replicationService = NewReplicationService(rf, rf.logs.GetLastLogIndex())

	rf.persist()
	rf.resetTimer()
}

func (rf *Raft) stopLeader() {
	rf.isLeader = false

	if rf.replicationService != nil {
		go rf.replicationService.stop()
		rf.replicationService = nil
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should Offset goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	cfg := Config{
		electionTimeout: genElectionTimeout(),
	}

	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.config = cfg
	rf.voteFor = -1
	rf.tracer = logrus.WithField("id", me)
	rf.resetTimerCh = make(chan struct{})
	rf.applier = NewApplier(rf.me, applyCh, rf.tracer)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to Offset elections
	go rf.ticker()
	go rf.applier.Daemon()
	return rf
}

func genElectionTimeout() time.Duration {
	interval := rand.Int()%150 + 150
	timeout, err := time.ParseDuration(fmt.Sprintf("%dms", interval))
	if err != nil {
		panic(err)
	}
	return timeout
}
