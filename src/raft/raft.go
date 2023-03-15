package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

type commits struct {
	start, end int
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
	commitCh     chan commits
	applyCh      chan ApplyMsg

	tracer             *logrus.Entry
	config             Config
	replicationService *ReplicationService
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all server
	currentTerm int
	voteFor     int
	logs        []Log
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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
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
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		rf.canBeLeader(args) {
		rf.voteFor = args.CandidateId
		rf.stopLeader()

		reply.VoteGranted = true
	}
}

// with lock
func (rf *Raft) canBeLeader(args RequestVoteArgs) bool {
	lastEntryTerm := -1
	if len(rf.logs) > 0 {
		lastEntryTerm = rf.logs[len(rf.logs)-1].Term
	}
	if lastEntryTerm > args.LastLogTerm {
		return false
	} else if lastEntryTerm == args.LastLogTerm {
		return args.LastLogIndex >= len(rf.logs)-1
	} else {
		return true
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

func (rf *Raft) applyMsg(newCommitIndex int) {
	if rf.commitIndex < newCommitIndex {
		if newCommitIndex > len(rf.logs)-1 {
			newCommitIndex = len(rf.logs) - 1
		}

		old := rf.commitIndex
		rf.commitIndex = newCommitIndex

		go func() {
			rf.tracer.Debugf("try to send commits, old=%d, end=%d", old+1, newCommitIndex)
			rf.commitCh <- commits{
				start: old + 1,
				end:   newCommitIndex,
			}
		}()
	}
}

func (rf *Raft) handleApplyMsg() {
	rf.commitCh = make(chan commits)
	lastCommitted := -1
	var newCommits []commits
	for c := range rf.commitCh {
		if c.start <= lastCommitted {
			continue
		}
		if c.start-lastCommitted != 1 { // todo
			newCommits = append(newCommits, c)
			sort.Slice(newCommits, func(i, j int) bool {
				return newCommits[i].start < newCommits[j].start
			})
			rf.tracer.Debugf("inqueue: %#v", c)
			continue
		} else {
			cnt := -1
			for c.start-lastCommitted == 1 {
				for i := c.start; i <= c.end; i++ {
					lastCommitted++
					command := rf.logs[i].Command
					if command == noOpCommand {
						continue
					}
					rf.lastApplied++
					rf.tracer.Debugf("commit: command=%#v, index=%d", command, rf.lastApplied)
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						CommandIndex: rf.lastApplied,
						Command:      command,
						Peer:         rf.me,
					}
				}
				cnt++
				if cnt < len(newCommits) {
					c = newCommits[cnt]
				} else {
					break
				}
			}
			newCommits = newCommits[:cnt]
		}
	}
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.tracer.Debugf("receive AppendEntries from %d, args=%v", args.LeaderId, args)

	rf.mu.Lock()

	defer func() {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm ||
		args.LeaderCommit < rf.commitIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.resetTimer()
	rf.currentTerm = args.Term

	rf.stopLeader()
	rf.voteFor = -1

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.PreLogIndex < 0 {
		reply.Success = true
		rf.logs = append(rf.logs[:0], args.Entries...)
	} else if args.PreLogIndex < len(rf.logs) && rf.logs[args.PreLogIndex].Term == args.PreLogTerm {
		rf.logs = append(rf.logs[:args.PreLogIndex+1], args.Entries...)
		reply.Success = true
	}

	if reply.Success {
		rf.applyMsg(args.LeaderCommit)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
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
	defer rf.mu.Unlock()

	if !rf.isLeader {
		return -1, -1, rf.isLeader
	}

	rf.tracer.Debugf("receive a message, command=%#v", command)

	rf.logs = append(rf.logs, Log{
		Term:    rf.currentTerm,
		Command: command,
	})

	index := 0
	for _, log := range rf.logs {
		if log.Command != noOpCommand {
			index++
		}
	}

	return index - 1, rf.currentTerm, rf.isLeader
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
		rf.tracer.Debugf("raft status: logs=%d, committed=%d, term=%d, leader=%t",
			len(rf.logs), rf.commitIndex, rf.currentTerm, rf.isLeader)
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
		LastLogIndex: len(rf.logs) - 1,
	}
	if len(rf.logs) <= 0 {
		args.LastLogTerm = -1
	} else {
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	return rf.electionCnt, args
}

var noOpCommand = "no-op"

func (rf *Raft) election() {
	currentElectionCnt, args := rf.fillRequestVotesArgs()

	var (
		votes     = 1
		peers     = len(rf.peers)
		wg        sync.WaitGroup
		replyCh   = make(chan struct{}, peers)
		subTracer = rf.tracer.WithField("Term", args.Term).WithField("cnt", currentElectionCnt)
		timer     = time.NewTimer(rf.config.electionTimeout)
	)

	wg.Add(peers)
	go func() { wg.Wait(); close(replyCh) }()

	subTracer.Debug("send requestVote rpc to peers")
	for idx, _ := range rf.peers {
		idx := idx
		if idx == rf.me {
			continue
		}

		go func(server int) {
			defer func() { wg.Done() }()
			subTracer := subTracer.WithField("peer", server)
			subTracer.Debug("send rpc requestVote")
			var reply RequestVoteReply
			if ok := rf.sendRequestVote(idx, args, &reply); ok {
				if reply.VoteGranted == true {
					subTracer.Debug("election granted")
					replyCh <- struct{}{}
				} else {
					subTracer.Debug("grant forbidden")
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
				}
			}
		}(idx)
	}

	for {
		select {
		case <-replyCh:
			if votes++; votes > peers/2 {
				subTracer.Debug("win the election")
				rf.mu.Lock()
				if rf.electionCnt == currentElectionCnt {
					rf.transferToLeader()
				}
				rf.mu.Unlock()
				return
			}
		case <-timer.C:
			return
		}
	}
}

func (rf *Raft) transferToLeader() {
	rf.isLeader = true
	rf.voteFor = -1
	rf.logs = append(rf.logs, Log{
		Term:    rf.currentTerm,
		Command: noOpCommand,
	})

	go func() {
		subTracer := rf.tracer.WithField("Term", rf.currentTerm)
		subTracer.Debug("change to leader, start heart beat")

		rf.replicationService = NewReplicationService(rf)
	}()

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
// Make() must return quickly, so it should start goroutines
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
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.handleApplyMsg()
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
