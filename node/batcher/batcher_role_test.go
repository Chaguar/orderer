/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaguar/orderer/common/operations"
	arma_types "github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/batcher"
	"github.com/Chaguar/orderer/node/batcher/mocks"
	"github.com/Chaguar/orderer/node/config"
	"github.com/Chaguar/orderer/node/consensus/state"
	node_ledger "github.com/Chaguar/orderer/node/ledger"
	"github.com/Chaguar/orderer/request"
	"github.com/Chaguar/orderer/testutil"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/stretchr/testify/require"
)

func TestPrimaryBatcherSimple(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	reqIDs := []string{"reused-req-id-1"}
	pool.NextRequestsReturnsOnCall(1, reqs, reqIDs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	batcher.Start()

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.RemoveRequestsCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()
	batcher.Stop()

	require.True(t, pool.RestartArgsForCall(0))
	require.NotZero(t, pool.NextRequestsCallCount())
	require.Equal(t, reqIDs, pool.RemoveRequestsArgsForCall(0))
}

func TestSecondaryBatcherSimple(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := arma_types.NewSimpleBatch(0, 1, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	batcher.Start()

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batch = arma_types.NewSimpleBatch(0, 1, 1, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.RemoveRequestsCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.Zero(t, pool.NextRequestsCallCount())
}

// Scenario: a secondary batcher, running at config sequence K, pulls a batch that the primary stamped with the
// previous config sequence K-1. This models the timing skew where pulling batches from a
// primary and pulling configs from consensus are asynchronous. The secondary logs a warning about the config
// sequence mismatch but still verifies and appends the batch, and the BAF it emits
// carries the secondary's current config sequence K.
func TestSecondaryBatcherPullsLowerConfigSeqBatch(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := arma_types.PartyID(2)
	shardID := arma_types.ShardID(0)

	// the batcher's current config sequence (K) and the older config sequence the pulled batch is stamped with (K-1)
	const currentConfigSeq = arma_types.ConfigSequence(1)
	const batchConfigSeq = arma_types.ConfigSequence(0)

	logger := testutil.CreateLogger(t, int(batcherID))
	batcher := createBatcher(t, batcherID, shardID, batchers, N, logger)

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(currentConfigSeq)
	batcher.ConfigSequenceGetter = configSeqGet

	// the BAF creator stamps the batcher's current config sequence, mirroring the production;
	// this is what lets a fragment for a lower-seq batch still be counted at the current config sequence.
	bafCreator := &mocks.FakeBAFCreator{}
	bafCreator.CreateBAFCalls(func(seq arma_types.BatchSequence, primary arma_types.PartyID, si arma_types.ShardID, digest []byte, txCount uint64, primarySignature []byte) arma_types.BatchAttestationFragment {
		return arma_types.NewSimpleBatchAttestationFragment(si, primary, seq, digest, batcherID, configSeqGet.ConfigSequence(), txCount, primarySignature)
	})
	batcher.BAFCreator = bafCreator

	bafSender := &mocks.FakeBAFSender{}
	batcher.BAFSender = bafSender

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	// the pulled batch: primary 1, seq 0 (matching the secondary's initial ledger height), stamped with the older
	// config sequence K-1
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := arma_types.BatchedRequests{req}
	batch := arma_types.NewSimpleBatch(shardID, 1, 0, reqs, batchConfigSeq, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()
	defer batcher.Stop()

	batchChan <- batch

	// verifyBatch logs a warning about the config-sequence mismatch but does not reject the batch; a successful
	// append is proof that the batch was verified and accepted despite the skew
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	// the batch is recorded under the secondary's current config sequence K, not the batch's K-1
	_, _, appendedConfigSeq, _, _, _ := ledger.AppendArgsForCall(0)
	require.Equal(t, currentConfigSeq, appendedConfigSeq)

	// the BAF the secondary emits carries the current config sequence K
	require.Eventually(t, func() bool {
		return bafSender.SendBAFCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	sentBAF, _ := bafSender.SendBAFArgsForCall(0)
	require.Equal(t, currentConfigSeq, sentBAF.ConfigSequence())
}

func TestPrimaryChangeToSecondary(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batch := arma_types.NewSimpleBatch(0, 2, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	})

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.False(t, pool.RestartArgsForCall(1))

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.True(t, pool.RestartArgsForCall(0))
	require.False(t, pool.RestartArgsForCall(1))
}

func TestSecondaryChangeToPrimary(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batch := arma_types.NewSimpleBatch(0, 1, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Zero(t, pool.NextRequestsCallCount())

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	})

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.True(t, pool.RestartArgsForCall(1))

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return batchPuller.StopCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.True(t, pool.RestartArgsForCall(1))

	require.Equal(t, arma_types.PartyID(1), ledger.HeightArgsForCall(0))
	require.Equal(t, arma_types.PartyID(2), ledger.HeightArgsForCall(1))
}

func TestSecondaryChangeToSecondary(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 3
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := arma_types.NewSimpleBatch(0, 1, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return pool.RemoveRequestsCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	})

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.False(t, pool.RestartArgsForCall(1))

	require.Eventually(t, func() bool {
		return batchPuller.StopCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batch = arma_types.NewSimpleBatch(0, 2, 0, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.RemoveRequestsCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.False(t, pool.RestartArgsForCall(1))
	require.Zero(t, pool.NextRequestsCallCount())

	require.Eventually(t, func() bool {
		return batchPuller.StopCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)
}

func TestPrimaryChangeToPrimary(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  4,
			},
		},
	})

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.True(t, pool.RestartArgsForCall(1))

	batcher.Stop()

	require.True(t, pool.RestartArgsForCall(0))
	require.True(t, pool.RestartArgsForCall(1))
}

func TestPrimaryWaiting(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturns(reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	batcher.Start()

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.NextRequestsCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.Equal(t, 10, ledger.AppendCallCount())
	require.Equal(t, 10, pool.NextRequestsCallCount())
}

func TestPrimaryWaitingAndTermChange(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturns(reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batch := arma_types.NewSimpleBatch(0, 2, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.NextRequestsCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	})

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.False(t, pool.RestartArgsForCall(1))

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 11
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.Equal(t, 11, ledger.AppendCallCount())
	require.Equal(t, 10, pool.NextRequestsCallCount())
}

func TestResubmitPending(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batch := arma_types.NewSimpleBatch(0, 1, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Zero(t, pool.NextRequestsCallCount())

	require.Zero(t, pool.SubmitCallCount())

	ledger.RetrieveBatchByNumberReturns(batch)

	myBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), arma_types.PartyID(batcherID), 0, 0, nil)
	notMyBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), arma_types.PartyID(batcherID+1), 0, 0, nil)
	myBAFWithOtherPrimary := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary()+1, batch.Seq(), batch.Digest(), arma_types.PartyID(batcherID), 0, 0, nil)

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
		Pending: []arma_types.BatchAttestationFragment{myBAF, notMyBAF, myBAFWithOtherPrimary},
	})

	require.Eventually(t, func() bool {
		return pool.SubmitCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.True(t, pool.RestartArgsForCall(1))

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return batchPuller.StopCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.True(t, pool.RestartArgsForCall(1))

	require.Equal(t, arma_types.PartyID(1), ledger.HeightArgsForCall(0))
	require.Equal(t, arma_types.PartyID(2), ledger.HeightArgsForCall(1))
}

func TestVerifyBatch(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0
	logger := testutil.CreateLogger(t, batcherID)
	secondaryBatcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)
	verifier := &mocks.FakeBatchedRequestsVerifier{}
	reqIDs := []string{"reused-req-id-1"}
	verifier.VerifyBatchedRequestsReturns(reqIDs, nil)
	secondaryBatcher.BatchedRequestsVerifier = verifier
	complainer := &mocks.FakeComplainer{}
	secondaryBatcher.Complainer = complainer

	pool := &mocks.FakeMemPool{}
	secondaryBatcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := arma_types.NewSimpleBatch(0, 1, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	secondaryBatcher.BatchPuller = batchPuller

	ledger := &mocks.FakeBatchLedger{}
	secondaryBatcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	secondaryBatcher.ConfigSequenceGetter = configSeqGet

	sigVerifier := &mocks.FakeSigVerifier{}
	sigVerifier.VerifySignatureReturns(nil)
	secondaryBatcher.SigVerifier = sigVerifier

	secondaryBatcher.Start()
	defer secondaryBatcher.Stop()

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.RemoveRequestsCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, reqIDs, pool.RemoveRequestsArgsForCall(0))

	batch = arma_types.NewSimpleBatch(0, 2, 0, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batch = arma_types.NewSimpleBatch(1, 1, 0, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batch = arma_types.NewSimpleBatch(0, 1, 2, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 3
	}, 10*time.Second, 10*time.Millisecond)

	batch = arma_types.NewSimpleBatch(0, 1, 0, nil, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 4
	}, 10*time.Second, 10*time.Millisecond)

	verifier.VerifyBatchedRequestsReturns(nil, errors.New(""))
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 5
	}, 10*time.Second, 10*time.Millisecond)
	verifier.VerifyBatchedRequestsReturns(nil, nil)

	batch = arma_types.NewSimpleBatch(0, 1, 1, reqs, 1, nil) // config seq mismatch, log as warning but append anyway
	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	sigVerifier.VerifySignatureReturns(errors.New(""))
	batch = arma_types.NewSimpleBatch(0, 1, 2, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 6
	}, 10*time.Second, 10*time.Millisecond)
	sigVerifier.VerifySignatureReturns(nil)

	require.Equal(t, 2, ledger.AppendCallCount())
}

// TestComplainInTermFaultSeenIn checks that a secondary complains in the term it saw the faulty batch
// in, even if the term changes before the complaint is sent.
func TestComplainInTermFaultSeenIn(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 3
	shardID := 0
	logger := testutil.CreateLogger(t, batcherID)
	secondaryBatcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	secondaryBatcher.TermProvider = termProvider

	// the verification of the pulled batch blocks until the term changed, and then fails
	verifying := make(chan struct{})
	termChanged := make(chan struct{})
	verifier := &mocks.FakeBatchedRequestsVerifier{}
	verifier.VerifyBatchedRequestsCalls(func(arma_types.BatchedRequests) ([]string, error) {
		close(verifying)
		<-termChanged
		return nil, errors.New("invalid request")
	})
	secondaryBatcher.BatchedRequestsVerifier = verifier
	complainer := &mocks.FakeComplainer{}
	secondaryBatcher.Complainer = complainer

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	batch := arma_types.NewSimpleBatch(0, 1, 0, arma_types.BatchedRequests{req}, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch, 1)
	batchPuller.PullBatchesReturns(batchChan)
	secondaryBatcher.BatchPuller = batchPuller

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	secondaryBatcher.ConfigSequenceGetter = configSeqGet

	secondaryBatcher.Start()
	defer secondaryBatcher.Stop()

	// pull a batch from the primary of term 0
	batchChan <- batch
	<-verifying

	// the term changes while the batch is verified
	termProvider.setState(secondaryBatcher, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 1}}})
	close(termChanged)

	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	_, term, _ := complainer.ComplainArgsForCall(0)
	require.Equal(t, uint64(0), term)
}

// TestRoleTermIsTheTermItActsIn checks that the role's term, which the second-strike complaint is sent in,
// is the term the role acts in: it does not move with the batcher's term until the role restarts in it.
func TestRoleTermIsTheTermItActsIn(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 3
	shardID := 0
	logger := testutil.CreateLogger(t, batcherID)
	secondaryBatcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	secondaryBatcher.TermProvider = termProvider

	batchPuller := &mocks.FakeBatchesPuller{}
	batchPuller.PullBatchesReturns(make(chan arma_types.Batch))
	secondaryBatcher.BatchPuller = batchPuller

	pool := &mocks.FakeMemPool{}
	secondaryBatcher.MemPool = pool

	secondaryBatcher.Start()
	defer secondaryBatcher.Stop()

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(0), secondaryBatcher.Term())

	// the batcher's term moves on, but the role is not told yet, so it still acts in term 0
	termProvider.setTerm(1)
	require.Never(t, func() bool {
		return secondaryBatcher.Term() != 0
	}, 500*time.Millisecond, 10*time.Millisecond)

	// once the role is told of a change to term 2, it restarts in term 2
	termProvider.setState(secondaryBatcher, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 2}}})
	require.Eventually(t, func() bool {
		return secondaryBatcher.Term() == 2 && pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)
}

// TestRoleTermIsSetAfterThePoolRestarts checks that the role sets its term only once the pool is restarted
// in it, so that a second-strike complaint about a request of the previous term is not sent in the new term.
func TestRoleTermIsSetAfterThePoolRestarts(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 3
	shardID := 0
	logger := testutil.CreateLogger(t, batcherID)
	secondaryBatcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	secondaryBatcher.TermProvider = termProvider

	batchPuller := &mocks.FakeBatchesPuller{}
	batchPuller.PullBatchesReturns(make(chan arma_types.Batch))
	secondaryBatcher.BatchPuller = batchPuller

	// the second restart of the pool, in term 1, blocks until released, and records the role's term then
	restarting := make(chan struct{})
	release := make(chan struct{})
	var termDuringRestart atomic.Uint64
	pool := &mocks.FakeMemPool{}
	pool.RestartCalls(func(bool) {
		if pool.RestartCallCount() == 2 {
			termDuringRestart.Store(secondaryBatcher.Term())
			close(restarting)
			<-release
		}
	})
	secondaryBatcher.MemPool = pool

	secondaryBatcher.Start()
	defer secondaryBatcher.Stop()
	// release the blocked restart before stopping, also if an assertion below fails
	var releaseOnce sync.Once
	releaseRestart := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseRestart()

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 1 && secondaryBatcher.Term() == 0
	}, 10*time.Second, 10*time.Millisecond)

	termProvider.setState(secondaryBatcher, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 1}}})

	// while the pool is restarting in term 1, the role still acts in term 0
	<-restarting
	require.Equal(t, uint64(0), termDuringRestart.Load())
	require.Equal(t, uint64(0), secondaryBatcher.Term())
	releaseRestart()

	require.Eventually(t, func() bool {
		return secondaryBatcher.Term() == 1
	}, 10*time.Second, 10*time.Millisecond)
}

// TestRoleDoesNotRestartInTheSameTerm checks that a notification of a term change that the role already
// acts in does not restart the role.
func TestRoleDoesNotRestartInTheSameTerm(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 3
	shardID := 0
	logger := testutil.CreateLogger(t, batcherID)
	secondaryBatcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	secondaryBatcher.TermProvider = termProvider

	batchPuller := &mocks.FakeBatchesPuller{}
	batchPuller.PullBatchesReturns(make(chan arma_types.Batch))
	secondaryBatcher.BatchPuller = batchPuller

	pool := &mocks.FakeMemPool{}
	secondaryBatcher.MemPool = pool

	secondaryBatcher.Start()
	defer secondaryBatcher.Stop()

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	// a notification for the term the role already acts in, as when the role read the new term before
	// the batcher sent the notification
	secondaryBatcher.NotifyTermChange()
	require.Never(t, func() bool {
		return pool.RestartCallCount() != 1 || batchPuller.StopCallCount() != 0
	}, 500*time.Millisecond, 10*time.Millisecond)

	// a real term change still restarts the role
	termProvider.setState(secondaryBatcher, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 1}}})
	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2 && secondaryBatcher.Term() == 1
	}, 10*time.Second, 10*time.Millisecond)
}

// TestResubmitOnTermChangeDoesNotBlockTheCaller checks that the requests of pending BAFs are resubmitted
// on the role's goroutine, so a slow resubmission does not delay the batcher's decision loop.
func TestResubmitOnTermChangeDoesNotBlockTheCaller(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0
	logger := testutil.CreateLogger(t, batcherID)
	secondaryBatcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	secondaryBatcher.TermProvider = termProvider

	batchPuller := &mocks.FakeBatchesPuller{}
	batchPuller.PullBatchesReturns(make(chan arma_types.Batch))
	secondaryBatcher.BatchPuller = batchPuller

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	batch := arma_types.NewSimpleBatch(0, 1, 0, arma_types.BatchedRequests{req}, 0, nil)
	ledger := &mocks.FakeBatchLedger{}
	ledger.RetrieveBatchByNumberReturns(batch)
	secondaryBatcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	secondaryBatcher.ConfigSequenceGetter = configSeqGet

	// submitting the resubmitted request blocks, as on a full pool, until released
	release := make(chan struct{})
	pool := &mocks.FakeMemPool{}
	pool.SubmitCalls(func([]byte) error {
		<-release
		return nil
	})
	secondaryBatcher.MemPool = pool

	secondaryBatcher.Start()
	defer secondaryBatcher.Stop()
	defer close(release)

	myBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(),
		arma_types.PartyID(batcherID), 0, 0, nil)
	resubmitted := make(chan struct{})
	go func() {
		secondaryBatcher.ResubmitOnTermChange(&state.State{Pending: []arma_types.BatchAttestationFragment{myBAF}}, 1)
		close(resubmitted)
	}()

	select {
	case <-resubmitted:
	case <-time.After(5 * time.Second):
		t.Fatal("ResubmitOnTermChange blocked on the resubmission")
	}
	require.Eventually(t, func() bool {
		return pool.SubmitCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
}

// TestResubmitPendingBAFsReverifiesOnConfigChange covers the reconfiguration case:
// requests taken from a BAF created under an older config are re-verified under the
// current policy before being resubmitted to the pool, and any request that fails
// the current policy is dropped rather than reinserted (so it never becomes eligible
// for the secondary pull-path verification shortcut).
func TestResubmitPendingBAFsReverifiesOnConfigChange(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := arma_types.PartyID(2)
	shardID := arma_types.ShardID(0)
	logger := testutil.CreateLogger(t, int(batcherID))

	b := createBatcher(t, batcherID, shardID, batchers, N, logger)

	validReq := []byte("valid-request")
	invalidReq := []byte("invalid-request")
	reqs := arma_types.BatchedRequests{validReq, invalidReq}

	// The pending BAF and its ledger batch were created under config seq 0.
	batch := arma_types.NewSimpleBatch(shardID, 1, 0, reqs, 0, nil)
	ledger := &mocks.FakeBatchLedger{}
	ledger.RetrieveBatchByNumberReturns(batch)
	b.Ledger = ledger

	// The batcher's current config seq is newer than the BAF's, so its requests must
	// be re-verified under the current policy.
	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(1)
	b.ConfigSequenceGetter = configSeqGet

	// Under the current policy invalidReq no longer verifies.
	verifier := &mocks.FakeBatchedRequestsVerifier{}
	verifier.VerifyRequestStub = func(req []byte) error {
		if string(req) == string(invalidReq) {
			return errors.New("fails current policy")
		}
		return nil
	}
	b.BatchedRequestsVerifier = verifier

	pool := &mocks.FakeMemPool{}
	b.MemPool = pool

	myBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), batcherID, 0, 0, nil)
	st := &state.State{
		Shards:  []state.ShardTerm{{Shard: shardID, Term: 1}},
		Pending: []arma_types.BatchAttestationFragment{myBAF},
	}

	b.ResubmitPendingBAFs(st, 0, true)

	// Both requests were re-verified, but only the valid one was resubmitted to the pool.
	require.Equal(t, 2, verifier.VerifyRequestCallCount())
	require.Equal(t, 1, pool.SubmitCallCount())
	require.Equal(t, validReq, pool.SubmitArgsForCall(0))
}

// TestResubmitStaleConfigBAFs covers the stale-config revive path: a BAF that consensus surfaced as
// behind the current config (in State.StaleConfigBAFs) has its batch's requests re-read from the ledger,
// re-verified under the current config, and resubmitted to the pool. BAFs signed by other batchers
// are ignored.
func TestResubmitStaleConfigBAFs(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := arma_types.PartyID(2)
	shardID := arma_types.ShardID(0)
	logger := testutil.CreateLogger(t, int(batcherID))

	b := createBatcher(t, batcherID, shardID, batchers, N, logger)

	validReq := []byte("valid-request")
	invalidReq := []byte("invalid-request")
	reqs := arma_types.BatchedRequests{validReq, invalidReq}

	// The stale BAF and its ledger batch were created under config seq 0.
	batch := arma_types.NewSimpleBatch(shardID, 1, 0, reqs, 0, nil)
	ledger := &mocks.FakeBatchLedger{}
	ledger.RetrieveBatchByNumberReturns(batch)
	b.Ledger = ledger

	// Current config seq (1) is newer than the BAF's (0), so its requests are re-verified.
	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(1)
	b.ConfigSequenceGetter = configSeqGet

	verifier := &mocks.FakeBatchedRequestsVerifier{}
	verifier.VerifyRequestStub = func(req []byte) error {
		if string(req) == string(invalidReq) {
			return errors.New("fails current policy")
		}
		return nil
	}
	b.BatchedRequestsVerifier = verifier

	pool := &mocks.FakeMemPool{}
	b.MemPool = pool

	myBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), batcherID, 0, 0, nil)
	notMyBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), batcherID+1, 0, 0, nil)
	st := &state.State{
		Shards:          []state.ShardTerm{{Shard: shardID, Term: 1}},
		StaleConfigBAFs: []arma_types.BatchAttestationFragment{myBAF, notMyBAF},
	}

	b.ResubmitStaleConfigBAFs(st)

	// Only my BAF's requests are revived: both re-verified, only the valid one resubmitted; the other
	// batcher's BAF is ignored (no extra ledger read / verify).
	require.Equal(t, 2, verifier.VerifyRequestCallCount())
	require.Equal(t, 1, pool.SubmitCallCount())
	require.Equal(t, validReq, pool.SubmitArgsForCall(0))
}

func createBatcher(t *testing.T, batcherID arma_types.PartyID, shardID arma_types.ShardID, batchers []arma_types.PartyID, N uint16, logger *flogging.FabricLogger) *batcher.BatcherRole {
	bafCreator := &mocks.FakeBAFCreator{}
	bafCreator.CreateBAFCalls(func(seq arma_types.BatchSequence, primary arma_types.PartyID, si arma_types.ShardID, digest []byte, txCount uint64, primarySignature []byte) arma_types.BatchAttestationFragment {
		return arma_types.NewSimpleBatchAttestationFragment(shardID, primary, seq, digest, batcherID, 0, txCount, nil)
	})

	batchersInfo := make([]config.BatcherInfo, len(batchers))
	for i, id := range batchers {
		batchersInfo[i] = config.BatcherInfo{
			PartyID: id,
		}
	}

	ledger := &mocks.FakeBatchLedger{}

	batcher := &batcher.BatcherRole{
		BatchTimeout:            time.Millisecond * 500,
		RequestInspector:        &mocks.FakeRequestInspector{},
		ID:                      batcherID,
		Shard:                   shardID,
		Threshold:               2,
		N:                       N,
		Logger:                  logger,
		Ledger:                  ledger,
		BatchPuller:             &mocks.FakeBatchesPuller{},
		TermProvider:            newTestTermProvider(batchers, shardID, N),
		BAFCreator:              bafCreator,
		BAFSender:               &mocks.FakeBAFSender{},
		BatchAcker:              &mocks.FakeBatchAcker{},
		MemPool:                 &mocks.FakeMemPool{},
		ConfigSequenceGetter:    &mocks.FakeConfigSequenceGetter{},
		BatchedRequestsVerifier: &mocks.FakeBatchedRequestsVerifier{},
		SigVerifier:             &mocks.FakeSigVerifier{},
		BatchSequenceGap:        arma_types.BatchSequence(10),
		Metrics: batcher.NewBatcherMetrics(&config.BatcherNodeConfig{
			PartyId: batcherID,
			ShardId: shardID,
			Operations: &operations.Operations{
				ListenAddress: testutil.AllocateLocalhostAddress(t),
			},
			Metrics: &operations.Metrics{
				Provider:           "disabled",
				MetricsLogInterval: 10 * time.Second,
			},
		}, batchersInfo, &node_ledger.BatchLedgerMetrics{}, ledger, logger),
	}

	return batcher
}

// testTermProvider stands in for the batcher as the source of the shard's primary and term.
type testTermProvider struct {
	lock     sync.Mutex
	batchers []arma_types.PartyID
	shard    arma_types.ShardID
	n        uint16
	term     uint64
}

func newTestTermProvider(batchers []arma_types.PartyID, shard arma_types.ShardID, n uint16) *testTermProvider {
	return &testTermProvider{batchers: batchers, shard: shard, n: n}
}

func (tp *testTermProvider) CurrentPrimaryAndTerm() (arma_types.PartyID, uint64) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	return tp.primary(tp.term), tp.term
}

// setState moves the provider to the term of its shard in s and, if the term changed, notifies role, as
// the batcher's decision loop does.
func (tp *testTermProvider) setState(role *batcher.BatcherRole, s *state.State) {
	var term uint64
	for _, shard := range s.Shards {
		if shard.Shard == tp.shard {
			term = shard.Term
		}
	}
	if prevPrimary, changed := tp.setTerm(term); changed {
		role.NotifyTermChange()
		role.ResubmitOnTermChange(s, prevPrimary)
	}
}

// setTerm moves the provider to term without notifying the role, and returns the primary of the
// previous term and whether the term changed.
func (tp *testTermProvider) setTerm(term uint64) (arma_types.PartyID, bool) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	prevPrimary, prevTerm := tp.primary(tp.term), tp.term
	tp.term = term
	return prevPrimary, term != prevTerm
}

func (tp *testTermProvider) primary(term uint64) arma_types.PartyID {
	return tp.batchers[(uint64(tp.shard)+term)%uint64(tp.n)]
}

// TestPrimaryBAFContextPerRole checks that the primary reuses one BAF context and cancels it on a term change.
func TestPrimaryBAFContextPerRole(t *testing.T) {
	b := createBatcher(t, 1, 0, []arma_types.PartyID{1, 2, 3, 4}, 4, testutil.CreateLogger(t, 1))
	pool := &mocks.FakeMemPool{}
	pool.NextRequestsReturns(arma_types.BatchedRequests{[]byte{1}}, nil)
	b.MemPool = pool
	bafSender := &mocks.FakeBAFSender{}
	// The last BAF blocks until its context is cancelled.
	inFlightDone := make(chan struct{})
	bafSender.SendBAFCalls(func(baf arma_types.BatchAttestationFragment, ctx context.Context) error {
		if baf.Seq() == 9 {
			<-ctx.Done()
			close(inFlightDone)
			return ctx.Err()
		}
		return nil
	})
	b.BAFSender = bafSender
	b.ConfigSequenceGetter = &mocks.FakeConfigSequenceGetter{}
	termProvider := newTestTermProvider([]arma_types.PartyID{1, 2, 3, 4}, 0, 4)
	b.TermProvider = termProvider

	b.Start()
	defer b.Stop()

	// With no acks, the primary sends BatchSequenceGap (10) batches and then waits.
	require.Eventually(t, func() bool {
		return bafSender.SendBAFCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	_, ctx := bafSender.SendBAFArgsForCall(0)
	for i := 1; i < 10; i++ {
		_, next := bafSender.SendBAFArgsForCall(i)
		require.Truef(t, next == ctx, "batch %d got a new BAF context", i)
	}
	require.NoError(t, ctx.Err())

	termProvider.setState(b, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 1}}})
	select {
	case <-inFlightDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the in-flight SendBAF did not get a cancelled context")
	}
}

// TestSecondaryBAFContextPerRole checks that the secondary reuses one BAF context and cancels it on a term change.
func TestSecondaryBAFContextPerRole(t *testing.T) {
	b := createBatcher(t, 2, 0, []arma_types.PartyID{1, 2, 3, 4}, 4, testutil.CreateLogger(t, 2))
	batchChan := make(chan arma_types.Batch)
	puller := &mocks.FakeBatchesPuller{}
	puller.PullBatchesReturns(batchChan)
	b.BatchPuller = puller
	bafSender := &mocks.FakeBAFSender{}
	// The last BAF blocks until its context is cancelled.
	inFlightDone := make(chan struct{})
	bafSender.SendBAFCalls(func(baf arma_types.BatchAttestationFragment, ctx context.Context) error {
		if baf.Seq() == 9 {
			<-ctx.Done()
			close(inFlightDone)
			return ctx.Err()
		}
		return nil
	})
	b.BAFSender = bafSender
	b.ConfigSequenceGetter = &mocks.FakeConfigSequenceGetter{}
	termProvider := newTestTermProvider([]arma_types.PartyID{1, 2, 3, 4}, 0, 4)
	b.TermProvider = termProvider

	b.Start()
	defer b.Stop()

	for seq := 0; seq < 10; seq++ {
		batchChan <- arma_types.NewSimpleBatch(0, 1, arma_types.BatchSequence(seq), arma_types.BatchedRequests{[]byte{1}}, 0, nil)
	}
	require.Eventually(t, func() bool {
		return bafSender.SendBAFCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	_, ctx := bafSender.SendBAFArgsForCall(0)
	for i := 1; i < 10; i++ {
		_, next := bafSender.SendBAFArgsForCall(i)
		require.Truef(t, next == ctx, "batch %d got a new BAF context", i)
	}
	require.NoError(t, ctx.Err())

	termProvider.setState(b, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 1}}})
	select {
	case <-inFlightDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the in-flight SendBAF did not get a cancelled context")
	}
}

var strandedRequestIDs = []string{"req-0", "req-1", "req-2"}

// testPrimary is batcher 1 of 4 in shard 0, the primary in term 0, with a fake ledger and BAF sender.
type testPrimary struct {
	role         *batcher.BatcherRole
	termProvider *testTermProvider
	ledger       *mocks.FakeBatchLedger
	bafSender    *mocks.FakeBAFSender
	puller       *mocks.FakeBatchesPuller
}

func newTestPrimary(t *testing.T, pool batcher.MemPool, batchTimeout time.Duration) *testPrimary {
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	role := createBatcher(t, 1, 0, batchers, 4, testutil.CreateLogger(t, 1))
	role.BatchTimeout = batchTimeout
	role.MemPool = pool
	termProvider := newTestTermProvider(batchers, 0, 4)
	role.TermProvider = termProvider
	ledger := &mocks.FakeBatchLedger{}
	// A party's height is the number of batches stored for it, so a role that restarts continues at the next seq.
	ledger.HeightCalls(func(party arma_types.PartyID) uint64 {
		var height uint64
		for i := 0; i < ledger.AppendCallCount(); i++ {
			if appended, _, _, _, _, _ := ledger.AppendArgsForCall(i); appended == party {
				height++
			}
		}
		return height
	})
	role.Ledger = ledger
	bafSender := &mocks.FakeBAFSender{}
	role.BAFSender = bafSender
	batchPuller := &mocks.FakeBatchesPuller{}
	batchPuller.PullBatchesReturns(make(chan arma_types.Batch))
	role.BatchPuller = batchPuller
	return &testPrimary{role: role, termProvider: termProvider, ledger: ledger, bafSender: bafSender, puller: batchPuller}
}

// softStopWithin soft stops the role, and fails the test if that takes longer than timeout, as when the role waits
// for a BAF send that the stop does not cancel.
func (p *testPrimary) softStopWithin(t *testing.T, timeout time.Duration) {
	stopped := make(chan struct{})
	go func() {
		p.role.SoftStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(timeout):
		t.Fatal("the role did not soft stop")
	}
}

// setTerm moves the shard to term, notifying the role as the batcher's decision loop does. With 4 batchers, the
// primary of term 4 is the primary of term 0.
func (p *testPrimary) setTerm(term uint64) {
	p.termProvider.setState(p.role, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: term}}})
}

// requireAppendedStranded checks that the i-th batch the primary stored holds strandedRequestIDs.
func (p *testPrimary) requireAppendedStranded(t *testing.T, i int) {
	_, _, _, reqs, _, _ := p.ledger.AppendArgsForCall(i)
	ids := make([]string, len(reqs))
	for j, req := range reqs {
		ids[j] = string(req)
	}
	require.ElementsMatch(t, strandedRequestIDs, ids)
}

// newStrandedTestPool returns a pool, holding strandedRequestIDs, whose request ids are the requests themselves.
func newStrandedTestPool(t *testing.T) *request.Pool {
	requestID := func(req []byte) string { return string(req) }
	pool := request.NewPool(testutil.CreateLogger(t, 1), requestID, request.PoolOptions{
		MaxSize:               10,
		BatchMaxSize:          10,
		BatchMaxSizeBytes:     100000,
		RequestMaxBytes:       1024,
		SubmitTimeout:         200 * time.Millisecond,
		FirstStrikeThreshold:  time.Minute,
		SecondStrikeThreshold: time.Minute,
		AutoRemoveTimeout:     10 * time.Second,
	}, noopStriker{})
	t.Cleanup(pool.Close)
	for _, id := range strandedRequestIDs {
		require.NoError(t, pool.Submit([]byte(id)))
	}
	return pool
}

// containsAll reports whether the pool's pending store holds all of strandedRequestIDs.
func containsAll(pool *request.Pool) bool {
	for _, id := range strandedRequestIDs {
		if !pool.Contains(id) {
			return false
		}
	}
	return true
}

// First BAF sends of a stranded batch: one that blocks until cancelled, as when the BAF does not reach enough
// consenters, and one that fails, as when the batcher stopped sending control events.
func blockUntilCancelled(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func failSend(context.Context) error {
	return errors.New("broadcast cancelled by object context")
}

// acceptedAsCancelled is a first BAF send that a quorum accepts as the role cancels it, so the send reports the
// acceptance: it closes cancelled once its context is cancelled, and succeeds once release is closed.
func acceptedAsCancelled(cancelled chan<- struct{}, release <-chan struct{}) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		<-ctx.Done()
		close(cancelled)
		<-release
		return nil
	}
}

// strandedBatch is a primary whose first batch holds strandedRequestIDs and whose first BAF send is firstSend.
type strandedBatch struct {
	*testPrimary
	pool *request.Pool
}

// newStrandedBatch starts the primary and returns once it has stored the batch and started sending its BAF.
func newStrandedBatch(t *testing.T, firstSend func(ctx context.Context) error) *strandedBatch {
	pool := newStrandedTestPool(t)
	p := newTestPrimary(t, pool, 50*time.Millisecond)
	inFlight := make(chan struct{})
	var sends atomic.Int32
	p.bafSender.SendBAFCalls(func(_ arma_types.BatchAttestationFragment, ctx context.Context) error {
		if sends.Add(1) > 1 {
			return nil
		}
		close(inFlight)
		return firstSend(ctx)
	})

	p.role.Start()
	select {
	case <-inFlight:
	case <-time.After(10 * time.Second):
		t.Fatal("the primary did not send a BAF")
	}
	require.Equal(t, 1, p.ledger.AppendCallCount())
	p.requireAppendedStranded(t, 0)
	return &strandedBatch{testPrimary: p, pool: pool}
}

// requireBatchedAfterRestart starts a new primary on pool, as the batcher does after a reconfiguration, and checks
// that it batches strandedRequestIDs.
func requireBatchedAfterRestart(t *testing.T, pool *request.Pool) {
	p := newTestPrimary(t, pool, 50*time.Millisecond)
	p.role.Start()
	defer p.role.Stop()

	require.Eventually(t, func() bool {
		return p.ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	p.requireAppendedStranded(t, 0)
	require.Eventually(t, func() bool {
		return pool.RequestCount() == 0
	}, 10*time.Second, 10*time.Millisecond)
}

// TestStrandedBatchReturnedOnReconfig checks that the requests of a batch whose BAF was not sent when the primary
// soft stops for a reconfiguration stay in the pool, are pruned under the new config, and are batched again.
func TestStrandedBatchReturnedOnReconfig(t *testing.T) {
	s := newStrandedBatch(t, blockUntilCancelled)
	s.softStopWithin(t, 5*time.Second)

	s.pool.Prune(func([]byte) error { return nil })
	require.Equal(t, int64(3), s.pool.RequestCount())
	requireBatchedAfterRestart(t, s.pool)
}

// TestStrandedBatchReturnedWhenBAFSendFails checks that the requests of a batch whose BAF send fails stay in the
// pool. The batcher stops sending control events before it stops the role, so on a soft stop the send can fail
// before the role learns that it stops. The role must not batch the requests again until it is restarted.
func TestStrandedBatchReturnedWhenBAFSendFails(t *testing.T) {
	s := newStrandedBatch(t, failSend)

	require.Never(t, func() bool {
		return s.ledger.AppendCallCount() > 1
	}, 300*time.Millisecond, 10*time.Millisecond)

	s.softStopWithin(t, 5*time.Second)
	s.pool.Prune(func([]byte) error { return nil })
	require.Equal(t, int64(3), s.pool.RequestCount())
	requireBatchedAfterRestart(t, s.pool)
}

// TestStrandedBatchReturnedOnTermChangeToSecondary checks that the requests of a batch whose BAF was not sent when
// the primary becomes a secondary are held in its pending store.
func TestStrandedBatchReturnedOnTermChangeToSecondary(t *testing.T) {
	s := newStrandedBatch(t, blockUntilCancelled)
	defer s.role.Stop()

	s.setTerm(1)

	require.Eventually(t, func() bool { return containsAll(s.pool) }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(3), s.pool.RequestCount())
}

// TestStrandedBatchReturnedOnTermChangeSamePrimary checks that the requests of a batch whose BAF was not sent, or
// whose send failed, are batched again when the term changes and the primary stays the same.
func TestStrandedBatchReturnedOnTermChangeSamePrimary(t *testing.T) {
	for name, firstSend := range map[string]func(context.Context) error{
		"send blocks": blockUntilCancelled,
		"send fails":  failSend,
	} {
		t.Run(name, func(t *testing.T) {
			s := newStrandedBatch(t, firstSend)
			defer s.role.Stop()

			s.setTerm(4)

			require.Eventually(t, func() bool {
				return s.ledger.AppendCallCount() == 2
			}, 10*time.Second, 10*time.Millisecond)
			s.requireAppendedStranded(t, 1)
			// The stranded batch stays in the ledger at seq 0, so the requests are stored again at seq 1, and both
			// batches can be ordered.
			_, seq, _, _, _, _ := s.ledger.AppendArgsForCall(1)
			require.Equal(t, arma_types.BatchSequence(1), seq)
		})
	}
}

// TestStrandedBatchRemovedWhenBAFAcceptedAsTermChanges checks that a primary whose term changes while its BAF is in
// flight cancels the send, and removes the batch's requests when the send reports that a quorum accepted the BAF.
func TestStrandedBatchRemovedWhenBAFAcceptedAsTermChanges(t *testing.T) {
	cancelled, release := make(chan struct{}), make(chan struct{})
	s := newStrandedBatch(t, acceptedAsCancelled(cancelled, release))
	defer s.role.Stop()

	s.setTerm(1)
	<-cancelled
	close(release)

	require.Eventually(t, func() bool {
		return s.puller.PullBatchesCallCount() > 0
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, s.ledger.AppendCallCount())
	require.Equal(t, int64(0), s.pool.RequestCount())
	for _, id := range strandedRequestIDs {
		require.False(t, s.pool.Contains(id))
	}
}

// TestStrandedBatchFailedSendWaitsThroughSameTermNotification checks that a primary whose BAF send failed keeps
// waiting when it is notified of a term change that leaves the term as it is, rather than batching the requests
// again in a BAF it cannot send.
func TestStrandedBatchFailedSendWaitsThroughSameTermNotification(t *testing.T) {
	s := newStrandedBatch(t, failSend)
	defer s.role.Stop()

	s.role.NotifyTermChange()

	require.Never(t, func() bool {
		return s.ledger.AppendCallCount() > 1
	}, 300*time.Millisecond, 10*time.Millisecond)
	require.Equal(t, int64(3), s.pool.RequestCount())
}

// TestStrandedBatchRemovedWhenBAFAcceptedAsStopping checks that a primary that stops while its BAF is in flight
// waits for the send, which the stop cancels, and removes the batch's requests when the send reports that a quorum
// accepted the BAF.
func TestStrandedBatchRemovedWhenBAFAcceptedAsStopping(t *testing.T) {
	cancelled, release := make(chan struct{}), make(chan struct{})
	s := newStrandedBatch(t, acceptedAsCancelled(cancelled, release))
	go func() {
		<-cancelled
		close(release)
	}()

	s.softStopWithin(t, 5*time.Second)

	require.Equal(t, int64(0), s.pool.RequestCount())
}

// enteringPool is a pool that closes entered when NextRequests is first called.
type enteringPool struct {
	*request.Pool
	entered chan struct{}
	once    sync.Once
}

func (p *enteringPool) NextRequests(ctx context.Context) ([][]byte, []string) {
	p.once.Do(func() { close(p.entered) })
	return p.Pool.NextRequests(ctx)
}

// newWaitingPrimary returns a primary on a pool that holds strandedRequestIDs, and that waits up to batchTimeout in
// NextRequests for a batch, and the pool, whose entered is closed once the primary waits there.
func newWaitingPrimary(t *testing.T, batchTimeout time.Duration) (*testPrimary, *enteringPool) {
	pool := &enteringPool{Pool: newStrandedTestPool(t), entered: make(chan struct{})}
	return newTestPrimary(t, pool, batchTimeout), pool
}

// TestSoftStopDoesNotCutABatch checks that a primary waiting for a batch when it soft stops does not cut, store and
// send a batch of the requests it holds, since it also keeps them in the pool, to batch them again once it restarts.
func TestSoftStopDoesNotCutABatch(t *testing.T) {
	p, pool := newWaitingPrimary(t, 10*time.Second)

	p.role.Start()
	<-pool.entered
	p.softStopWithin(t, 5*time.Second)

	require.Equal(t, 0, p.ledger.AppendCallCount())
	require.Equal(t, 0, p.bafSender.SendBAFCallCount())
	require.Equal(t, int64(3), pool.RequestCount())
	requireBatchedAfterRestart(t, pool.Pool)
}

// TestTermChangeDuringNextRequestsDoesNotCutABatch checks that a primary whose term changes while it waits for a
// batch does not cut, store and send that batch in the term it left, but keeps its requests in the pool.
func TestTermChangeDuringNextRequestsDoesNotCutABatch(t *testing.T) {
	p, pool := newWaitingPrimary(t, time.Second)

	p.role.Start()
	defer p.role.Stop()
	<-pool.entered
	p.setTerm(1)

	require.Eventually(t, func() bool { return containsAll(pool.Pool) }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, 0, p.ledger.AppendCallCount())
	require.Equal(t, 0, p.bafSender.SendBAFCallCount())
}

// TestTermChangeBeforeNotificationDoesNotCutABatch checks that a primary whose term moved on, but which was not yet
// notified of it, does not cut, store and send a batch in the term it left. The batcher updates the term before it
// notifies the role.
func TestTermChangeBeforeNotificationDoesNotCutABatch(t *testing.T) {
	p, pool := newWaitingPrimary(t, 200*time.Millisecond)

	p.role.Start()
	defer p.role.Stop()
	<-pool.entered
	p.termProvider.setTerm(1)

	require.Eventually(t, func() bool { return containsAll(pool.Pool) }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, 0, p.ledger.AppendCallCount())
	require.Equal(t, 0, p.bafSender.SendBAFCallCount())
}
