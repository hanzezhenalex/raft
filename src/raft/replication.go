package raft

import (
	"context"
	"sort"
	"sync"
	"time"

	"6.5840/labrpc"

	"github.com/sirupsen/logrus"
)

type received struct {
	peer  int
	index int
}

type ReplicationService struct {
	raft           *Raft
	tracer         *logrus.Entry
	commitCh       chan received
	replicators    []*Replicator
	wg             sync.WaitGroup
	committedIndex []int
	committed      int

	stopped bool
	mu      sync.Mutex
}

func NewReplicationService(raft *Raft) *ReplicationService {
	rs := &ReplicationService{
		raft:           raft,
		tracer:         raft.tracer.WithField("role", "replication service"),
		commitCh:       make(chan received),
		replicators:    make([]*Replicator, 0, len(raft.peers)-1), // peers contains self
		committedIndex: make([]int, 0, len(raft.peers)),
		committed:      -1,
	}

	rs.wg.Add(len(raft.peers) - 1)
	for i := 0; i < len(raft.peers); i++ {
		rs.committedIndex = append(rs.committedIndex, -1)
	}
	subTracer := raft.tracer.WithField("role", "replicator")
	for idx := 0; idx < len(raft.peers); idx++ {
		if idx == raft.me {
			continue
		}
		rs.replicators = append(rs.replicators, NewReplicator(idx, raft.me, raft.peers[idx], raft,
			subTracer.WithField("peer", idx), rs.commitCh, func() { rs.wg.Done() }))
	}

	go rs.daemon()
	return rs
}

func (rs *ReplicationService) daemon() {
	rs.tracer.Debug("ReplicationService start working")

	for peerCommit := range rs.commitCh {
		rs.tracer.Debugf("peer %d last log index=%d", peerCommit.peer, peerCommit.index)
		rs.committedIndex[peerCommit.peer] = peerCommit.index

		index := rs.updateCommittedIndex()
		if index > rs.committed {
			rs.committed = index
			rs.tracer.Debugf("commit message, index=%d", index)
			go rs.raft.applyMsg(index) // apply message even if we are not leader
		}
	}
}

func (rs *ReplicationService) updateCommittedIndex() int {
	committed := make([]int, len(rs.committedIndex))
	copy(committed, rs.committedIndex) // contains me
	sort.Slice(committed, func(i, j int) bool {
		return committed[i] < committed[j]
	})
	// me always be -1
	// [-1, a, b], should be b, 3/2=1 => 2
	// [-1, a, b, c, d], should be c, 5/2=2 => 3
	mid := len(committed)/2 + 1
	return committed[mid]
}

func (rs *ReplicationService) stop() {
	rs.mu.Lock()
	if rs.stopped {
		rs.mu.Unlock()
		return
	}
	rs.stopped = true
	rs.mu.Unlock()

	rs.tracer.Debug("ReplicationService stop working")
	for _, rep := range rs.replicators {
		rep.stop()
	}
	rs.wg.Wait() // safe stop
	close(rs.commitCh)
}

type replicatorStatus string

const (
	maxLogEntries = 100

	matching    replicatorStatus = "matching"
	replicating replicatorStatus = "replicating"
)

func DoWithTimeout(fn func(), timeout time.Duration) {
	ch := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	go func() {
		defer close(ch)
		fn()
	}()

	select {
	case <-ch:
	case <-ctx.Done():
	}
}

type Replicator struct {
	me     int
	i      int // append logs to whom
	peer   *labrpc.ClientEnd
	raft   *Raft
	tracer *logrus.Entry

	status    replicatorStatus
	nextIndex int
	logs      []Log

	callback        func()
	stopped         chan struct{}
	reportSendIndex chan received
}

func NewReplicator(i, me int, peer *labrpc.ClientEnd, raft *Raft, tracer *logrus.Entry, reportSendIndex chan received, callback func()) *Replicator {
	replicator := &Replicator{
		me:              me,
		i:               i,
		peer:            peer,
		raft:            raft,
		tracer:          tracer,
		status:          matching,
		stopped:         make(chan struct{}),
		reportSendIndex: reportSendIndex,
		callback:        callback,
	}
	replicator.nextIndex = len(replicator.logs) - 1
	if replicator.nextIndex < 0 {
		replicator.nextIndex = 0
	}
	go replicator.daemon()
	return replicator
}

// init nextIndex = len(rf.logs) - 1 => try to append the last log
func (rep *Replicator) fillAppendEntries() (AppendEntriesRequest, bool) {
	rep.raft.mu.Lock()
	currentTerm := rep.raft.currentTerm
	commitIndex := rep.raft.commitIndex
	rep.logs = rep.raft.logs // update logs
	rep.raft.mu.Unlock()

	args := AppendEntriesRequest{
		Term:         currentTerm,
		LeaderId:     rep.me,
		LeaderCommit: commitIndex,
	}

	if rep.nextIndex < 0 { // if reply false in AppendEntries
		rep.nextIndex = 0
	}

	// fill PreLogIndex
	args.PreLogIndex = rep.nextIndex - 1

	// fill PreLogTerm
	// set -1 if rf.logs is empty
	if args.PreLogIndex < 0 {
		args.PreLogTerm = -1
	} else {
		args.PreLogTerm = rep.logs[args.PreLogIndex].Term
	}

	// return false if there is no nextIndex to append
	if rep.nextIndex < len(rep.logs) && rep.nextIndex >= 0 {
		args.Entries = rep.logs[rep.nextIndex : rep.nextIndex+1]
		return args, true
	}

	return args, false
}

func (rep *Replicator) update() {
	rep.tracer.Debug("replicator start update")

	for {
		select {
		case <-rep.stopped:
			return
		default:
		}

		args, hasEntryToAppend := rep.fillAppendEntries()
		if !hasEntryToAppend { // nothing to append, stop update
			rep.tracer.Debug("has no new entry, stop update")
			return
		}

		var (
			reply AppendEntriesReply
			ok    = false
		)

		// try to append logs
		DoWithTimeout(func() {
			rep.tracer.Debugf("try to append %d", args.PreLogIndex+1)
			ok = rep.peer.Call("Raft.AppendEntries", args, &reply) // may timeout
		}, rep.raft.config.electionTimeout)

		if !ok { // timeout or network unavailable
			rep.tracer.Debugf("peer disconnected")
			return
		}

		rep.handleReply(&args, &reply)
	}
}

func (rep *Replicator) commit(last int) {
	select {
	case <-rep.stopped:
	case rep.reportSendIndex <- received{
		peer:  rep.i,
		index: last,
	}:
	}
}

func (rep *Replicator) handleReply(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rep.tracer.Debugf("got reply reply=%#v, index=%d", reply, args.PreLogIndex)
	if reply.Success {
		go rep.commit(args.PreLogIndex + 1)
		rep.nextIndex++
	} else {
		rep.nextIndex--
	}

	{
		// update term if needed
		rep.raft.mu.Lock()
		if reply.Term > rep.raft.currentTerm {
			rep.raft.currentTerm = reply.Term
		}
		rep.raft.mu.Unlock()
	}
}

func (rep *Replicator) stop() {
	close(rep.stopped)
}

func (rep *Replicator) daemon() {
	defer func() {
		rep.callback()
		rep.tracer.Debug("replicator stop working")
	}()
	rep.update()

	interval := time.Millisecond * 100
	timer := time.NewTimer(interval)

	for {
		select {
		case <-timer.C:
			rep.update()
			timer.Reset(interval)
		case <-rep.stopped:
			return
		}
	}
}
