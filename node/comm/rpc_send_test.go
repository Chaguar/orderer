/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/stretchr/testify/require"
)

// newBufferedTestStream returns a stream whose sends go to a send buffer of
// the given capacity, with nothing draining it.
func newBufferedTestStream(id uint64, capacity int) *Stream {
	return &Stream{
		ID:        id,
		abortChan: make(chan struct{}),
		sendBuff: make(chan struct {
			request *orderer.StepRequest
			report  func(error)
		}, capacity),
		commShutdown: make(chan struct{}),
		abortReason:  &atomic.Value{},
		canceled:     new(uint32),
		Cancel:       func(error) {},
		Logger:       flogging.MustGetLogger("test"),
	}
}

func newSendTestRPC(destination uint64, stream *Stream) *RPC {
	rpc := &RPC{
		Logger:        flogging.MustGetLogger("test"),
		Timeout:       time.Minute,
		StreamsByType: NewStreamsByType(),
	}
	rpc.StreamsByType[ConsensusOperation][destination] = stream
	return rpc
}

func TestRPCSendConsensus(t *testing.T) {
	t.Run("a successful send returns no error", func(t *testing.T) {
		stream := newBufferedTestStream(7, 1)
		rpc := newSendTestRPC(1, stream)

		require.NoError(t, rpc.SendConsensus(1, &orderer.ConsensusRequest{Channel: "arma"}))
		require.Len(t, stream.sendBuff, 1)
		require.Same(t, stream, rpc.StreamsByType[ConsensusOperation][1])
	})

	t.Run("a failed send returns the error and unmaps the stream", func(t *testing.T) {
		// A full send buffer makes the consensus request overflow.
		stream := newBufferedTestStream(7, 1)
		stream.sendBuff <- struct {
			request *orderer.StepRequest
			report  func(error)
		}{}
		rpc := newSendTestRPC(1, stream)

		err := rpc.SendConsensus(1, &orderer.ConsensusRequest{Channel: "arma"})
		require.ErrorIs(t, err, errOverflow)
		require.NotContains(t, rpc.StreamsByType[ConsensusOperation], uint64(1))
	})
}
