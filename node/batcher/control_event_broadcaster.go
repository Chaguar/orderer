/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Chaguar/orderer/common/controlevent"
	"github.com/Chaguar/orderer/node/consensus/state"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
)

// ControlEventBroadcaster sends control events (BAFs and complaints) to the consenters. A broadcast returns once a
// quorum of n − f consenters accepted the event, without waiting for the others. At most one send per consenter is in
// flight: a consenter whose earlier send hasn't returned yet, because it is slow or unreachable, is skipped.
type ControlEventBroadcaster struct {
	senders          []controlevent.Sender
	busy             []atomic.Bool // busy[i] tells whether a send to senders[i] is in flight
	threshold        int
	n                int
	f                int
	minRetryInterval time.Duration
	maxRetryDelay    time.Duration
	logger           *flogging.FabricLogger
	ctx              context.Context
	cancelFunc       context.CancelFunc
}

// sendResult is the outcome of sending an event to the consenter of index sender.
type sendResult struct {
	sender int
	ok     bool
}

// BroadcastControlEvent sends ce to every consenter that isn't busy, and returns once threshold consenters accepted it,
// or with an error once ctx or the broadcaster ends. Sends still in flight then finish in the background. A consenter
// whose send failed, or that was busy, is tried again after a backoff, until the event has its quorum. The backoff
// grows only on a retry that starts sends: while every missing consenter is busy, the broadcast checks again at the
// same interval, so a stuck send that fails is retried soon.
func (b *ControlEventBroadcaster) BroadcastControlEvent(ce state.ControlEvent, ctx context.Context) error {
	results := make(chan sendResult)
	done := make(chan struct{}) // closed when the broadcast returns: a send that ends later doesn't report
	defer close(done)
	accepted := make([]bool, len(b.senders))
	acceptedCount := 0

	b.sendToIdle(ce, accepted, results, done)
	delay := b.minRetryInterval
	retry := time.NewTimer(delay)
	defer retry.Stop()

	for {
		select {
		case result := <-results:
			if !result.ok {
				continue
			}
			accepted[result.sender] = true
			acceptedCount++
			if acceptedCount >= b.threshold {
				b.logger.Debugf("Control event accepted by a quorum (%d of %d consenters)", acceptedCount, len(b.senders))
				return nil
			}
		case <-retry.C:
			if started := b.sendToIdle(ce, accepted, results, done); started > 0 {
				b.logger.Warnf("Control event accepted by %d of %d consenters (need %d); sending to %d", acceptedCount, len(b.senders), b.threshold, started)
				delay = min(2*delay, b.maxRetryDelay)
			} else {
				b.logger.Debugf("Control event accepted by %d of %d consenters (need %d); the others are busy", acceptedCount, len(b.senders), b.threshold)
			}
			retry.Reset(delay)
		case <-ctx.Done():
			return fmt.Errorf("broadcast cancelled by input context: %w", ctx.Err())
		case <-b.ctx.Done():
			return fmt.Errorf("broadcast cancelled by object context: %w", b.ctx.Err())
		}
	}
}

// sendToIdle sends ce, each on its own goroutine, to the consenters that didn't accept it yet and have no send in flight.
// A send reports its result on results, unless the broadcast is done by then. It returns how many sends it started.
func (b *ControlEventBroadcaster) sendToIdle(ce state.ControlEvent, accepted []bool, results chan<- sendResult, done <-chan struct{}) int {
	started := 0
	for i, sender := range b.senders {
		if accepted[i] || !b.busy[i].CompareAndSwap(false, true) {
			continue
		}
		started++
		go func() {
			// Free i only after the broadcast read this result or returned: until then it must not send to i again.
			defer b.busy[i].Store(false)
			err := sender.SendControlEvent(ce)
			if err != nil && b.ctx.Err() == nil { // once the broadcaster stopped, Close ends the sends in flight
				b.logger.Warnf("Failed sending control event %s: %v", ce.String(), err)
			}
			select {
			case results <- sendResult{sender: i, ok: err == nil}:
			case <-done:
			}
		}()
	}
	return started
}

func NewControlEventBroadcaster(senders []controlevent.Sender, n int, f int, minRetryInterval time.Duration, maxRetryDelay time.Duration, logger *flogging.FabricLogger, ctx context.Context, cancelFunc context.CancelFunc) *ControlEventBroadcaster {
	return &ControlEventBroadcaster{
		senders:          senders,
		busy:             make([]atomic.Bool, len(senders)),
		n:                n,
		f:                f,
		threshold:        n - f,
		minRetryInterval: minRetryInterval,
		maxRetryDelay:    maxRetryDelay,
		logger:           logger,
		ctx:              ctx,
		cancelFunc:       cancelFunc,
	}
}

func (b *ControlEventBroadcaster) Stop() {
	b.cancelFunc()
}

// Close closes the connections of the senders, which also ends a send in flight. Call it after Stop, once nothing
// produces control events.
func (b *ControlEventBroadcaster) Close() {
	for _, s := range b.senders {
		s.Close()
	}
}
