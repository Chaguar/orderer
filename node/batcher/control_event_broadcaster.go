/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Chaguar/orderer/common/controlevent"
	"github.com/Chaguar/orderer/node/consensus/state"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
)

type ControlEventBroadcaster struct {
	senders          []controlevent.Sender
	threshold        int
	n                int
	f                int
	minRetryInterval time.Duration
	maxRetryDelay    time.Duration
	logger           *flogging.FabricLogger
	ctx              context.Context
	cancelFunc       context.CancelFunc
}

func (b *ControlEventBroadcaster) BroadcastControlEvent(ce state.ControlEvent, ctx context.Context) error {
	retrySenders := b.senders
	delay := b.minRetryInterval

	for {
		var failed []controlevent.Sender
		var failedMu sync.Mutex
		var wg sync.WaitGroup

		for _, sender := range retrySenders {
			wg.Add(1)
			go func(s controlevent.Sender) {
				defer wg.Done()
				if err := b.sendControlEvent(ce, s); err != nil {
					failedMu.Lock()
					failed = append(failed, s)
					failedMu.Unlock()
				}
			}(sender)
		}

		wg.Wait()

		if len(b.senders)-len(failed) >= b.threshold {
			b.logger.Infof("Control event sent to quorum (%d out of %d)", len(b.senders)-len(failed), len(b.senders))
			return nil
		}

		delay *= 2
		if delay > b.maxRetryDelay {
			delay = b.maxRetryDelay
		}

		b.logger.Warnf("Only %d successful sends (need %d); going to retry %d failed after %s", len(b.senders)-len(failed), b.threshold, len(failed), delay)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			err := fmt.Errorf("broadcast cancelled by input context: %w", ctx.Err())
			b.logger.Info(err)
			return err
		case <-b.ctx.Done():
			timer.Stop()
			err := fmt.Errorf("broadcast cancelled by object context: %w", b.ctx.Err())
			b.logger.Info(err)
			return err
		case <-timer.C:
		}

		retrySenders = failed
	}
}

func (b *ControlEventBroadcaster) sendControlEvent(ce state.ControlEvent, sender controlevent.Sender) error {
	t1 := time.Now()

	defer func() {
		b.logger.Infof("Sending control event took %v", time.Since(t1))
	}()

	if err := sender.SendControlEvent(ce); err != nil {
		b.logger.Errorf("Failed sending control event; err: %v", err)
		return err
	}

	return nil
}

func NewControlEventBroadcaster(senders []controlevent.Sender, n int, f int, minRetryInterval time.Duration, maxRetryDelay time.Duration, logger *flogging.FabricLogger, ctx context.Context, cancelFunc context.CancelFunc) *ControlEventBroadcaster {
	return &ControlEventBroadcaster{
		senders:          senders,
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

// Close closes the connections of the senders. Call it after Stop, once nothing produces control events.
func (b *ControlEventBroadcaster) Close() {
	for _, s := range b.senders {
		s.Close()
	}
}
