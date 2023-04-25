package raft

import (
	"sort"
	"sync"

	"github.com/sirupsen/logrus"
)

type ApplyRequest struct {
	Start int
	Logs  []Log
}

type ApplySnapshotRequest struct {
	Term             int
	LastIncludeIndex int
	Data             []byte
}

type Applier struct {
	tracer        *logrus.Entry
	me            int
	applyCh       chan ApplyMsg
	nextIndex     int
	lastApplied   int
	repCh         chan ApplyRequest
	snapshotReqCh chan ApplySnapshotRequest
	toApply       []ApplyRequest
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

func NewApplier(me int, applyCh chan ApplyMsg, tracer *logrus.Entry) *Applier {
	return &Applier{
		tracer:        tracer,
		nextIndex:     0,
		lastApplied:   -1,
		me:            me,
		applyCh:       applyCh,
		stopCh:        make(chan struct{}),
		repCh:         make(chan ApplyRequest),
		snapshotReqCh: make(chan ApplySnapshotRequest),
	}
}

func (apl *Applier) Stop() {
	apl.tracer.Debugf("stop the applier")
	close(apl.stopCh)
	apl.wg.Wait()
}

func (apl *Applier) Daemon() {
	apl.wg.Add(1)
	defer apl.wg.Done()

	apl.tracer.Debugf("applier daemon started")

	for {
		select {
		case <-apl.stopCh:
			return
		case req := <-apl.repCh:
			apl.tracer.Debugf("apply req: start=%d", req.Start)
			if req.Start == apl.nextIndex {
				apl.doApply(req)
			} else {
				apl.tracer.Debug("cache req")
				apl.toApply = append(apl.toApply, req)
				sort.Slice(apl.toApply, func(i, j int) bool {
					return apl.toApply[i].Start < apl.toApply[j].Start
				})
			}
		case req := <-apl.snapshotReqCh:
			apl.doApplySnapshot(req)
		}
	}
}

func (apl *Applier) doApply(req ApplyRequest) {
	apl.tracer.Debug("apply logs req now")
	apl.handleApplyReq(req)
	apl.applyReqInCache()
}

func (apl *Applier) doApplySnapshot(req ApplySnapshotRequest) {
	apl.tracer.Debugf("apply snapshot req: lastInclude=%d", req.LastIncludeIndex)
	apl.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotTerm:  req.Term,
		SnapshotIndex: req.LastIncludeIndex,
	}

	toRemove := -1
	noOpLogsToRemove := 0

	for i := 0; i < len(apl.toApply); i++ {
		logReq := apl.toApply[i]
		start := logReq.Start
		end := logReq.Start + len(logReq.Logs)

		if start > req.LastIncludeIndex {
			break
		}
		if end > req.LastIncludeIndex {
			n := req.LastIncludeIndex + 1 - logReq.Start
			for i := 0; i < n; i++ {
				log := logReq.Logs[i]
				if log.Command == noOpCommand {
					continue
				}
				noOpLogsToRemove++
			}

			logReq.Start = req.LastIncludeIndex + 1
			logReq.Logs = logReq.Logs[n:]
			apl.toApply[i] = logReq
			break
		}
		toRemove = i
		for _, log := range logReq.Logs {
			if log.Command == noOpCommand {
				continue
			}
			noOpLogsToRemove++
		}
	}

	if toRemove >= 0 {
		apl.toApply = apl.toApply[toRemove+1:]
		apl.lastApplied += noOpLogsToRemove
		apl.nextIndex = req.LastIncludeIndex + 1
	}

	apl.applyReqInCache()
}

func (apl *Applier) Apply(req ApplyRequest) {
	go func() {
		select {
		case <-apl.stopCh:
		case apl.repCh <- req:
		}
	}()
}

func (apl *Applier) ApplySnapshot(req ApplySnapshotRequest) {
	go func() {
		select {
		case <-apl.stopCh:
		case apl.snapshotReqCh <- req:
		}
	}()
}

func (apl *Applier) handleApplyReq(req ApplyRequest) {
	for _, log := range req.Logs {
		apl.nextIndex++
		if log.Command != noOpCommand {
			apl.lastApplied++
			apl.tracer.Debugf("apply message: index=%d, command=%#v", apl.lastApplied, log.Command)
			apl.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: apl.lastApplied,
				Command:      log.Command,
				Peer:         apl.me,
			}
		}
	}
}

func (apl *Applier) applyReqInCache() {
	cleaned := -1
	for i, req := range apl.toApply {
		if req.Start == apl.nextIndex {
			apl.handleApplyReq(req)
			cleaned = i
		} else {
			break
		}
	}

	if cleaned >= 0 {
		apl.toApply = apl.toApply[:cleaned+1]
	}
}
