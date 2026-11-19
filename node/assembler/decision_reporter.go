/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"context"
	"sync"

	"github.com/Chaguar/common/protoutil/identity"
	"github.com/Chaguar/orderer/common/controlevent"
	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/consensus/state"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
)

// DecisionReporter reports to every consenter the highest decision whose batches are all in the assembler's
// ledger, once that decision is at least step decisions above the last one it reported. A report that is lost
// or rejected is not resent: the next report replaces it.
//
// Each consenter has its own goroutine and a mailbox that holds only the latest report, so a consenter that
// is down or slow delays only its own reports, and gets the latest one once its send returns.
//
// Start and Appended must be called from one goroutine at a time, Start first.
type DecisionReporter struct {
	logger     *flogging.FabricLogger
	party      types.PartyID
	configSeq  types.ConfigSequence
	signer     identity.SignerSerializer
	step       types.DecisionNum
	consenters []*consenterMailbox
	committed  types.DecisionNum // the highest decision whose batches are all in the ledger
	lastSent   types.DecisionNum

	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stopOnce sync.Once
}

type consenterMailbox struct {
	sender  controlevent.Sender
	mailbox chan state.ControlEvent // holds at most the latest report not yet sent
}

// NewDecisionReporter creates a reporter that signs its reports with signer, for the given party and config
// sequence, and sends them through senders, one per consenter. The reporter owns the senders and closes them
// in Stop. A step below 1 counts as 1. last is the ordering information of the last block in the ledger, or
// nil if there is none.
func NewDecisionReporter(
	logger *flogging.FabricLogger,
	party types.PartyID,
	configSeq types.ConfigSequence,
	signer identity.SignerSerializer,
	senders []controlevent.Sender,
	step types.DecisionNum,
	last *state.OrderingInformation,
) *DecisionReporter {
	ctx, cancel := context.WithCancel(context.Background())
	r := &DecisionReporter{
		logger:    logger,
		party:     party,
		configSeq: configSeq,
		signer:    signer,
		step:      max(step, 1),
		ctx:       ctx,
		cancel:    cancel,
	}
	if last != nil {
		r.committed = committedDecision(last)
	}
	for _, sender := range senders {
		r.consenters = append(r.consenters, &consenterMailbox{sender: sender, mailbox: make(chan state.ControlEvent, 1)})
	}
	return r
}

// Start starts a sending goroutine per consenter, and reports the decision the ledger ends with if it is at
// least step.
func (r *DecisionReporter) Start() {
	r.wg.Add(len(r.consenters))
	for _, c := range r.consenters {
		go r.send(c)
	}
	r.maybeReport()
}

// Stop stops the sending goroutines and closes the senders, which aborts a send in progress. It returns once
// every goroutine of the reporter has returned. Stop can be called more than once, and before Start.
func (r *DecisionReporter) Stop() {
	r.stopOnce.Do(func() {
		r.cancel()
		for _, c := range r.consenters {
			c.sender.Close()
		}
		r.wg.Wait()
	})
}

// Appended records the ordering information of a block the assembler appended to its ledger, and reports the
// decision it completes if it is at least step decisions above the last report. It ignores nil. It does not
// wait for any send, but it signs the report on the caller's goroutine, once every step decisions.
func (r *DecisionReporter) Appended(oi *state.OrderingInformation) {
	if oi == nil {
		return
	}
	r.committed = committedDecision(oi)
	r.maybeReport()
}

func (r *DecisionReporter) maybeReport() {
	if r.committed < r.lastSent+r.step {
		return
	}

	report := &state.AssemblerDecisionReport{Party: r.party, DecisionNum: r.committed, ConfigSeq: r.configSeq}
	sig, err := r.signer.Sign(report.ToBeSigned())
	if err != nil {
		r.logger.Errorf("Failed signing the report of decision %d: %v", r.committed, err)
		return
	}
	report.Signature = sig
	ce := state.ControlEvent{AssemblerReport: report}

	for _, c := range r.consenters {
		// A report this consenter has not taken yet is replaced by the new one. That is safe because the new
		// report is of a higher decision, and consensus keeps only the highest decision reported by each party.
		// Only this method puts into a mailbox, so once the old report is taken out, the put cannot block.
		select {
		case <-c.mailbox:
		default:
		}
		c.mailbox <- ce
	}
	r.lastSent = r.committed
	r.logger.Infof("Queued the report of decision %d for %d consenters", r.committed, len(r.consenters))
}

func (r *DecisionReporter) send(c *consenterMailbox) {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case ce := <-c.mailbox:
			if err := c.sender.SendControlEvent(ce); err != nil {
				if r.ctx.Err() != nil {
					return
				}
				r.logger.Warnf("Failed sending the report of decision %d: %v", ce.AssemblerReport.DecisionNum, err)
			}
		}
	}
}

// committedDecision returns the highest decision whose batches are all in the ledger, given the ordering
// information of the last block in it. A block that is not the last of its decision means the decision before
// it is the last complete one. A decision with no batches has no block in the ledger, so the decision of the
// next block covers it.
func committedDecision(oi *state.OrderingInformation) types.DecisionNum {
	if oi.BatchIndex == oi.BatchCount-1 || oi.DecisionNum == 0 {
		return oi.DecisionNum
	}
	return oi.DecisionNum - 1
}
