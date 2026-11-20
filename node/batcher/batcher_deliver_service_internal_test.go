/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"testing"
	"time"

	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/delivery"
	node_ledger "github.com/Chaguar/orderer/node/ledger"
	"github.com/Chaguar/orderer/testutil"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Create the batch ledger of a shard with two parties, with no batch in either part.
//  2. Iterate the empty part of party 1 for a request whose stream is open, and expect the iterator not to be
//     returned while the part stays empty.
//  3. Append a batch to that part, and expect the iterator to be returned.
//  4. Iterate the empty part of party 2 for a request whose stream is open, and expect the iterator not to be
//     returned while the part stays empty.
//  5. Close the request's stream, and expect the iterator to be returned although the part is still empty.
func TestDelayedReaderWaitsForTheFirstBatchUntilTheStreamCloses(t *testing.T) {
	ledgerMetrics := &node_ledger.BatchLedgerMetrics{}
	ledgerMetrics.NewBatchLedgerMetrics(&disabled.Provider{}, "1", "1")
	ledgerArray, err := node_ledger.NewBatchLedgerArray(1, 1, []types.PartyID{1, 2}, "arma", t.TempDir(), testutil.CreateLogger(t, 1), ledgerMetrics)
	require.NoError(t, err)
	t.Cleanup(ledgerArray.Close)

	iterate := func(ctx context.Context, partyID types.PartyID) chan struct{} {
		returned := make(chan struct{})
		go func() {
			iterator, _ := (&delayedReader{Reader: ledgerArray.Part(partyID).Ledger(), ctx: ctx}).Iterator(delivery.SingleSpecifiedSeekInfo(0).Start)
			iterator.Close()
			close(returned)
		}()
		return returned
	}
	isClosed := func(c chan struct{}) func() bool {
		return func() bool {
			select {
			case <-c:
				return true
			default:
				return false
			}
		}
	}

	t.Run("a batch is appended", func(t *testing.T) {
		returned := iterate(context.Background(), 1)
		require.Never(t, isClosed(returned), 200*time.Millisecond, 10*time.Millisecond)

		requests := types.BatchedRequests{[]byte("tx")}
		ledgerArray.Append(1, types.BatchSequence(0), types.ConfigSequence(0), requests, requests.Digest(), nil)
		require.Eventually(t, isClosed(returned), 10*time.Second, 10*time.Millisecond)
	})

	t.Run("the stream closes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		returned := iterate(ctx, 2)
		require.Never(t, isClosed(returned), 200*time.Millisecond, 10*time.Millisecond)

		cancel()
		require.Eventually(t, isClosed(returned), 10*time.Second, 10*time.Millisecond)
	})
}
