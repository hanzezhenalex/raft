package raft

import (
	"time"

	"6.5840/labrpc"
	"github.com/sirupsen/logrus"
)

type Replicator struct {
	me     int
	i      int
	peer   *labrpc.ClientEnd
	raft   *Raft
	tracer *logrus.Entry

	nextIndex int
	logs      []Log

	stopped         chan struct{}
	reportSendIndex chan commit
}

// init nextIndex = len(rf.logs) - 1 => try to append the last log
func (rep *Replicator) fillAppendEntries() (AppendEntriesRequest, bool) {
	rep.raft.mu.Lock()

	currentTerm := rep.raft.currentTerm
	commitIndex := rep.raft.commitIndex

	rep.logs = rep.raft.logs

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

func (rep *Replicator) update() (stopOnLeaderChange bool) {
	rep.tracer.Debug("start update")

	for {
		select {
		case <-rep.stopped:
			stopOnLeaderChange = true
			return
		default:
		}

		args, hasEntryToAppend := rep.fillAppendEntries()
		rep.tracer.Debugf("try to append %d", args.PreLogIndex+1)

		var reply AppendEntriesReply
		ok := false
		ch := make(chan struct{}, 1)
		timer := time.NewTimer(rep.raft.config.electionTimeout)

		go func() {
			defer func() { ch <- struct{}{}; close(ch) }()
			ok = rep.peer.Call("Raft.AppendEntries", args, &reply) // may timeout
		}()

		select {
		case <-ch:
		case <-timer.C:
		}

		if !ok {
			rep.tracer.Debugf("peer disconnected")
			stopOnLeaderChange = false
			return
		}

		rep.tracer.Debugf("got reply reply=%#v, index=%d", reply, args.PreLogIndex)

		if !hasEntryToAppend {
			rep.tracer.Debug("has no new entry, stop update")
			stopOnLeaderChange = false
			return
		}

		if reply.Success {
			go func() {
				select {
				case <-rep.stopped:
					stopOnLeaderChange = true
					return
				case rep.reportSendIndex <- commit{
					peer:  rep.i,
					index: args.PreLogIndex + 1,
				}:
				}
			}()
			rep.nextIndex++
		} else {
			rep.nextIndex--
		}

		{
			// update term if needed
			rep.raft.mu.Lock()
			if reply.Term > rep.raft.currentTerm {
				rep.raft.currentTerm = reply.Term
				rep.raft.persist()
			}
			rep.raft.mu.Unlock()
		}
	}
}

func (rep *Replicator) start(stop chan struct{}) {
	defer func() {
		rep.tracer.Debug("replicator stop working")
	}()

	rep.nextIndex = len(rep.logs) - 1
	if rep.nextIndex < 0 {
		rep.nextIndex = 0
	}
	rep.stopped = stop

	rep.tracer.Debugf("replicator start work, next index=%d", rep.nextIndex)

	if stopOnLeaderChange := rep.update(); stopOnLeaderChange {
		return
	}

	interval := time.Millisecond * 100
	timer := time.NewTimer(interval)

	for {
		select {
		case <-timer.C:
			if stopOnLeaderChange := rep.update(); stopOnLeaderChange {
				return
			}
			timer.Reset(interval)
		case <-rep.stopped:
			return
		}
	}
}
