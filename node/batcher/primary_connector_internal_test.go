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
	node_config "github.com/Chaguar/orderer/node/config"
	"github.com/Chaguar/orderer/testutil"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Create a connector with different timings for forwards and for acks. Nothing is sent, so nothing connects.
//  2. Expect each lane's link to have its own lane's send timeout and retry delays, and SendReq to wait the forward
//     send timeout.
func TestCreatePrimaryConnectorGivesEachLaneItsTiming(t *testing.T) {
	forwards := LinkTiming{SendTimeout: 10 * time.Second, MinRetryInterval: 200 * time.Millisecond, MaxRetryDelay: 2 * time.Second}
	acks := LinkTiming{SendTimeout: time.Second, MinRetryInterval: 50 * time.Millisecond, MaxRetryDelay: 500 * time.Millisecond}
	conf := &node_config.BatcherNodeConfig{PartyId: 2, ShardId: 3}
	c := CreatePrimaryConnector(1, testutil.CreateLogger(t, 2), conf, map[types.PartyID]*EndpointAndCerts{}, context.Background(), forwards, acks)
	t.Cleanup(c.Stop)

	require.Equal(t, forwards, c.forwards.link.timing)
	require.Equal(t, acks, c.acks.link.timing)
	require.Equal(t, forwards.SendTimeout, c.forwardWait)
}
