/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"testing"
	"time"

	"github.com/Chaguar/orderer/common/types"
)

func createTestBatch(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, req []byte) types.Batch {
	return types.NewSimpleBatch(shard, primary, seq, types.BatchedRequests{req}, 0, nil)
}

// createTestBatchWithSize creates a simple batch including requests with given size
//
// Example:
//
//	// Returns a simple batch with:
//	// shard id = 1
//	// primary party id = 2
//	// sequence = 3
//	// digest = calculated according to the generated requests
//	// 2 requests, the first of size 4 bytes, the second with 5 bytes
//	b := createTestBatchWithSize(1, 2, 3, []int{4, 5})
func createTestBatchWithSize(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, requestsBytesSize []int) types.Batch {
	requests := types.BatchedRequests{}
	for _, requestSize := range requestsBytesSize {
		requests = append(requests, make([]byte, requestSize))
	}
	return types.NewSimpleBatch(shard, primary, seq, requests, 0, nil)
}

func createTestBatchId(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, req []byte) types.BatchID {
	return types.NewSimpleBatch(shard, primary, seq, types.BatchedRequests{req}, 0, nil)
}

// receiveWithin returns the next value from ch, or a zero value if ch is closed, and fails the test
// if neither happens within eventuallyTimeout, so that a test of a hang fails instead of hanging.
func receiveWithin[T any](t *testing.T, ch <-chan T, msg string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(eventuallyTimeout):
		t.Fatal(msg)
		return *new(T)
	}
}
