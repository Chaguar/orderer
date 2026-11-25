/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	arma_types "github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/batcher"
	"github.com/Chaguar/orderer/node/batcher/mocks"
	"github.com/Chaguar/orderer/node/consensus/state"
	"github.com/Chaguar/orderer/request"
	"github.com/Chaguar/orderer/testutil"
	"github.com/stretchr/testify/require"
)

// complainingSecondary is batcher 3 of 4, a secondary in term 0, whose pulled batches fail verification and whose
// complaints block until their context ends, as when the consenters do not accept them.
type complainingSecondary struct {
	role         *batcher.BatcherRole
	termProvider *testTermProvider
	puller       *mocks.FakeBatchesPuller
	complaining  chan struct{} // closed when the first complaint starts
	complained   chan struct{} // closed when the first complaint ends
}

func newComplainingSecondary(t *testing.T) *complainingSecondary {
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	s := &complainingSecondary{
		role:         createBatcher(t, 3, 0, batchers, 4, testutil.CreateLogger(t, 3)),
		termProvider: newTestTermProvider(batchers, 0, 4),
		puller:       &mocks.FakeBatchesPuller{},
		complaining:  make(chan struct{}),
		complained:   make(chan struct{}),
	}
	s.role.TermProvider = s.termProvider
	verifier := &mocks.FakeBatchedRequestsVerifier{}
	verifier.VerifyBatchedRequestsReturns(nil, errors.New("invalid request"))
	s.role.BatchedRequestsVerifier = verifier
	var first sync.Once
	complainer := &mocks.FakeComplainer{}
	complainer.ComplainCalls(func(ctx context.Context, _ uint64, _ string) {
		isFirst := false
		first.Do(func() { isFirst = true })
		if !isFirst {
			return
		}
		close(s.complaining)
		select {
		case <-ctx.Done():
			close(s.complained)
		case <-time.After(10 * time.Second): // the test fails waiting for complained
		}
	})
	s.role.Complainer = complainer
	batches := make(chan arma_types.Batch, 1)
	batches <- arma_types.NewSimpleBatch(0, 1, 0, arma_types.BatchedRequests{[]byte("req")}, 0, nil)
	s.puller.PullBatchesReturns(batches)
	s.role.BatchPuller = s.puller
	return s
}

func (s *complainingSecondary) startAndWaitForComplaint(t *testing.T) {
	s.role.Start()
	select {
	case <-s.complaining:
	case <-time.After(10 * time.Second):
		t.Fatal("the secondary did not complain")
	}
}

// TestSecondaryComplaintEnds checks that a secondary's complaint about a batch that failed verification ends once
// the term changes, so the secondary moves on to the new primary, and once the role soft stops.
func TestSecondaryComplaintEnds(t *testing.T) {
	t.Run("term change", func(t *testing.T) {
		s := newComplainingSecondary(t)
		defer s.role.Stop()
		s.startAndWaitForComplaint(t)

		s.termProvider.setState(s.role, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 1}}})

		select {
		case <-s.complained:
		case <-time.After(10 * time.Second):
			t.Fatal("the complaint did not end on the term change")
		}
		require.Eventually(t, func() bool {
			n := s.puller.PullBatchesCallCount()
			return n > 1 && s.puller.PullBatchesArgsForCall(n-1) == 2
		}, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("soft stop", func(t *testing.T) {
		s := newComplainingSecondary(t)
		s.startAndWaitForComplaint(t)

		stopped := make(chan struct{})
		go func() {
			s.role.SoftStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Fatal("the role did not soft stop")
		}
	})
}

// newIdleSecondary starts batcher 2 of 4, a secondary in term 0 whose primary sends no batches, and waits until it
// acts in term 0.
func newIdleSecondary(t *testing.T) *batcher.BatcherRole {
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	role := createBatcher(t, 2, 0, batchers, 4, testutil.CreateLogger(t, 2))
	role.TermProvider = newTestTermProvider(batchers, 0, 4)
	puller := &mocks.FakeBatchesPuller{}
	puller.PullBatchesReturns(make(chan arma_types.Batch))
	role.BatchPuller = puller
	role.Start()
	t.Cleanup(role.Stop)
	require.Eventually(t, func() bool {
		_, ctx := role.TermContext()
		return ctx.Err() == nil
	}, 10*time.Second, 10*time.Millisecond)
	return role
}

// TestSameTermNotificationKeepsRunContext checks that a notification that leaves the term as it is does not end the
// context of the role's run.
func TestSameTermNotificationKeepsRunContext(t *testing.T) {
	role := newIdleSecondary(t)
	term, ctx := role.TermContext()
	require.Equal(t, uint64(0), term)

	role.NotifyTermChange()

	require.Never(t, func() bool { return ctx.Err() != nil }, 200*time.Millisecond, 10*time.Millisecond)
}

// strikeComplainer is a pool striker whose second-strike complaint, like the batcher's, is sent in the role's term
// and blocks until the role's run context ends, as when the consenters do not accept it.
type strikeComplainer struct {
	role       *batcher.BatcherRole
	complained chan struct{}
	once       sync.Once
}

func (s *strikeComplainer) OnFirstStrikeTimeout([]byte) {}

func (s *strikeComplainer) OnSecondStrikeTimeout() {
	_, ctx := s.role.TermContext()
	s.once.Do(func() { close(s.complained) })
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second): // the pool restarts late, and the test fails waiting for the batch
	}
}

// TestTermChangeDoesNotWaitForSecondStrikeComplaint checks that a term change restarts the pool although a
// second-strike complaint of the previous term waits for its quorum: the pool's restart waits for the pending
// store's goroutine, which sends the complaint.
func TestTermChangeDoesNotWaitForSecondStrikeComplaint(t *testing.T) {
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	logger := testutil.CreateLogger(t, 2)
	role := createBatcher(t, 2, 0, batchers, 4, logger)
	role.BatchTimeout = 50 * time.Millisecond
	termProvider := newTestTermProvider(batchers, 0, 4)
	role.TermProvider = termProvider
	puller := &mocks.FakeBatchesPuller{}
	puller.PullBatchesReturns(make(chan arma_types.Batch))
	role.BatchPuller = puller
	ledger := &mocks.FakeBatchLedger{}
	role.Ledger = ledger
	striker := &strikeComplainer{role: role, complained: make(chan struct{})}
	pool := request.NewPool(logger, func(req []byte) string { return string(req) }, request.PoolOptions{
		MaxSize:               10,
		BatchMaxSize:          10,
		BatchMaxSizeBytes:     100000,
		RequestMaxBytes:       1024,
		SubmitTimeout:         200 * time.Millisecond,
		FirstStrikeThreshold:  100 * time.Millisecond,
		SecondStrikeThreshold: 100 * time.Millisecond,
		AutoRemoveTimeout:     10 * time.Second,
	}, striker)
	t.Cleanup(pool.Close)
	role.MemPool = pool

	role.Start()
	defer role.Stop()
	require.NoError(t, role.Submit([]byte("req")))
	select {
	case <-striker.complained:
	case <-time.After(10 * time.Second):
		t.Fatal("the pending request did not strike twice")
	}

	// Batcher 2 is the primary of term 1, so it batches the request once the pool restarts.
	termProvider.setState(role, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 1}}})

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() > 0
	}, 10*time.Second, 10*time.Millisecond)
}

// TestSecondaryKeepsRequestsWhenBAFSendFails checks that a secondary whose BAF send fails, which happens only when
// its run ends, as on a stop or a term change, keeps the batch's requests and does not ack the batch, since the BAF
// may not have reached the consenters.
func TestSecondaryKeepsRequestsWhenBAFSendFails(t *testing.T) {
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	role := createBatcher(t, 3, 0, batchers, 4, testutil.CreateLogger(t, 3))
	role.TermProvider = newTestTermProvider(batchers, 0, 4)
	pool := &mocks.FakeMemPool{}
	role.MemPool = pool
	acker := &mocks.FakeBatchAcker{}
	role.BatchAcker = acker
	bafSender := &mocks.FakeBAFSender{}
	bafSender.SendBAFCalls(func(arma_types.BatchAttestationFragment, context.Context) error {
		return errors.New("broadcast cancelled by input context")
	})
	role.BAFSender = bafSender
	puller := &mocks.FakeBatchesPuller{}
	batches := make(chan arma_types.Batch, 1)
	batches <- arma_types.NewSimpleBatch(0, 1, 0, arma_types.BatchedRequests{[]byte("req")}, 0, nil)
	puller.PullBatchesReturns(batches)
	role.BatchPuller = puller

	role.Start()
	defer role.Stop()

	require.Eventually(t, func() bool {
		return bafSender.SendBAFCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool {
		return pool.RemoveRequestsCallCount() > 0 || acker.AckCallCount() > 0
	}, 300*time.Millisecond, 10*time.Millisecond)
}
