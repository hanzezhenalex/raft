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

type Applier struct {
	tracer      *logrus.Entry
	me          int
	applyCh     chan ApplyMsg
	nextIndex   int
	lastApplied int
	repCh       chan ApplyRequest
	toApply     []ApplyRequest
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

func NewApplier(me int, applyCh chan ApplyMsg, tracer *logrus.Entry) *Applier {
	return &Applier{
		tracer:      tracer,
		nextIndex:   0,
		lastApplied: -1,
		me:          me,
		applyCh:     applyCh,
		stopCh:      make(chan struct{}),
		repCh:       make(chan ApplyRequest),
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
		}
	}
}

func (apl *Applier) doApply(req ApplyRequest) {
	apl.tracer.Debug("apply logs req now")
	apl.handleApplyReq(req)
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
