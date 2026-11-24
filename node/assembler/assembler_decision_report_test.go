/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"testing"
	"time"

	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/comm/tlsgen"
	"github.com/Chaguar/orderer/node/config"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Start an assembler of party 1 with stub batchers, the stub consenter of party 1, which it pulls decisions
//     from, and the stub consenter of party 2.
//  2. Wait for the genesis block, then send decisions 1 to 100, each with one batch.
//  3. Expect the assembler to append all of them, and both consenters to get a report of decision 100 from
//     party 1 under config sequence 0.
//  4. Stop the assembler, and expect each consenter to have got exactly that report.
func TestAssemblerReportsTheCommittedDecisionToEveryConsenter(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	partyID := types.PartyID(1)
	shardID := types.ShardID(1)

	batchersStub, batcherInfos, cleanup := createStubBatchersAndInfos(t, 4, shardID, ca)
	defer cleanup()

	ownConsenter := NewStubConsenter(t, partyID, ca)
	defer ownConsenter.Stop()
	otherConsenter := NewStubConsenter(t, 2, ca)
	defer otherConsenter.Stop()

	shards := []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}}
	assembler, _ := newAssemblerTest(t, partyID, ca, shards, ownConsenter.consenterInfo, 20*time.Second, false, nil, otherConsenter.consenterInfo)

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()
	for d := types.DecisionNum(1); d <= 100; d++ {
		batch := types.NewSimpleBatch(shardID, 1, types.BatchSequence(d-1), types.BatchedRequests{[]byte{byte(d)}}, 0, nil)
		batchersStub[0].SetNextBatch(batch)
		ownConsenter.SetNextDecision(obaCreator.Append(batch, d, 0, 1))
		select {
		case <-ownConsenter.decisionSentCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("the stub consenter did not send decision %d", d)
		}
	}

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 101
	}, 30*time.Second, 100*time.Millisecond)

	consenters := []*stubConsenter{ownConsenter, otherConsenter}
	require.Eventually(t, func() bool {
		for _, c := range consenters {
			if len(c.receivedReports()) == 0 {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)
	assembler.Stop()

	for _, c := range consenters {
		reports := c.receivedReports()
		require.Len(t, reports, 1)
		require.Equal(t, partyID, reports[0].Party)
		require.Equal(t, types.DecisionNum(100), reports[0].DecisionNum)
		require.Equal(t, types.ConfigSequence(0), reports[0].ConfigSeq)
	}
}
