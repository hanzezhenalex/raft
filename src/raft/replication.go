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

func NewReplicationService(raft *Raft, nextIndex int) *ReplicationService {
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
		rep := NewReplicator(idx, raft.me, raft.peers[idx], raft,
			subTracer.WithField("peer", idx), rs.commitCh, func() { rs.wg.Done() }, nextIndex)
		go rep.daemon()
		rs.replicators = append(rs.replicators, rep)
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
			go func() {
				rs.raft.commit(index, false) // apply message even if we are not leader
			}()
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

	status replicatorStatus
	// matching:    next log entry to append
	// replicating: next log entry try to match
	nextIndex int

	callback        func()
	stopped         chan struct{}
	reportSendIndex chan received
}

func NewReplicator(i, me int, peer *labrpc.ClientEnd, raft *Raft, tracer *logrus.Entry, reportSendIndex chan received,
	callback func(), nextIndex int) *Replicator {
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
		nextIndex:       nextIndex,
	}
	return replicator
}

// init nextIndex = len(rf.logs) - 1 => try to append the last log
func (rep *Replicator) fillAppendEntries() (AppendEntriesRequest, bool) {
	rep.raft.mu.Lock()
	defer rep.raft.mu.Unlock()

	if rep.status == matching {
		return rep.fillRequestsMatching()
	}
	return rep.fillRequestsReplication()
}

func (rep *Replicator) fillRequestsMatching() (AppendEntriesRequest, bool) {
	assert(rep.status == matching, "should in matching stage")

	rep.nextIndex = max(rep.nextIndex, 0)
	ret := rep.raft.logs.RetrieveBackward(rep.nextIndex, maxLogEntries)

	if len(ret.Logs) == 0 && ret.Snapshot != nil {
		// handle snapshot
	}

	args := AppendEntriesRequest{
		Term:         rep.raft.currentTerm,
		LeaderId:     rep.me,
		LeaderCommit: rep.raft.commitIndex,
		Offset:       ret.Start,
		Entries:      ret.Logs,
	}
	return args, true
}

func (rep *Replicator) fillRequestsReplication() (AppendEntriesRequest, bool) {
	assert(rep.status == replicating, "should in replicating stage")

	args := AppendEntriesRequest{
		Term:         rep.raft.currentTerm,
		LeaderId:     rep.me,
		LeaderCommit: rep.raft.commitIndex,
	}

	if rep.nextIndex > rep.raft.logs.GetLastLogIndex() {
		args.Offset = rep.nextIndex
		return args, false // no entry to append
	}

	nextIndex := max(rep.nextIndex-1, 0)
	ret := rep.raft.logs.RetrieveForward(nextIndex, maxLogEntries)

	if ret.Snapshot != nil {
		// handle snapshot
	}

	args.Offset = ret.Start
	args.Entries = ret.Logs

	return args, true
}

func (rep *Replicator) update() {
	rep.tracer.Debug("replicator start update")

	for {
		select {
		case <-rep.stopped:
			return
		default:
		}

		// hasEntryToAppend == false means no entry to append
		// but still need to send a heartbeat message
		args, hasEntryToAppend := rep.fillAppendEntries()
		var (
			reply AppendEntriesReply
			ok    = false
		)

		// try to append logs
		DoWithTimeout(func() {
			rep.tracer.Debugf("try to append entries, start=%d", args.Offset)
			ok = rep.peer.Call("Raft.AppendEntries", args, &reply) // may timeout
		}, rep.raft.config.electionTimeout)

		if !hasEntryToAppend { // nothing to append, stop update
			rep.tracer.Debug("has no new entry, stop update")
			return
		}

		if !ok { // timeout or network unavailable
			rep.tracer.Debugf("peer disconnected")
			return
		}

		rep.raft.mu.Lock()
		if rep.raft.currentTerm < args.Term {
			rep.tracer.Debugf("term behind peer, convert to follower, current term=%d, args term=%d", rep.raft.currentTerm, args.Term)
			rep.raft.stopLeader()
			rep.raft.voteFor = -1
			rep.raft.currentTerm = args.Term
			rep.raft.mu.Unlock()
			return
		}
		rep.raft.mu.Unlock()

		rep.handleReply(args, &reply)
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

func (rep *Replicator) handleReply(args AppendEntriesRequest, reply *AppendEntriesReply) {
	rep.tracer.Debugf("got reply reply=%#v", reply)
	rep.nextIndex = reply.Next

	if reply.Success {
		if rep.status == matching {
			rep.status = replicating
		}
		rep.commit(rep.nextIndex - 1)
	} else {
		if rep.status == replicating {
			rep.status = matching
		}
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
