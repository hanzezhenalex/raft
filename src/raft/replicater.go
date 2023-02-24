package raft

import (
	"6.5840/labrpc"
	"time"
)

type Replicator struct {
	me   int
	i    int
	peer *labrpc.ClientEnd
	raft *Raft

	nextIndex int
	logs      []Log

	stopped chan struct{}
}

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

	args.PreLogIndex = rep.nextIndex - 1
	if args.PreLogIndex < 0 || len(rep.logs) == 0 {
		args.PreLogTerm = -1
	} else {
		args.PreLogTerm = rep.logs[args.PreLogIndex].Term
	}

	if rep.nextIndex < len(rep.logs) {
		args.Entries = rep.logs[rep.nextIndex : rep.nextIndex+1]
	} else {
		return args, false
	}

	return args, true
}

func (rep *Replicator) tryAppendEntry(args AppendEntriesRequest) bool {
	var reply AppendEntriesReply

	ok := rep.peer.Call("Raft.AppendEntries", args, &reply)
	if !ok {
		return false
	}
	if reply.Success {
		rep.nextIndex++
	} else {
		rep.nextIndex--
	}

	rep.raft.mu.Lock()
	defer rep.raft.mu.Unlock()

	if reply.Term > rep.raft.currentTerm {
		rep.raft.currentTerm = reply.Term
	}

	return true
}

func (rep *Replicator) update() bool {
	for {
		select {
		case <-rep.stopped:
			return false
		default:
		}

		args, hasEntryToAppend := rep.fillAppendEntries()
		if hasEntryToAppend {
			if ok := rep.tryAppendEntry(args); !ok {
				time.Sleep(rep.raft.config.electionTimeout / 2)
			}
		} else {
			return true
		}
	}
}

func (rep *Replicator) start(stop chan struct{}) {
	rep.nextIndex = len(rep.logs)
	rep.stopped = stop

	if ok := rep.update(); !ok {
		return
	}

	timer := time.NewTimer(rep.raft.config.electionTimeout)

	for {
		select {
		case <-timer.C:
			if ok := rep.update(); !ok {
				return
			}
		case <-rep.stopped:
			return
		}
	}
}
