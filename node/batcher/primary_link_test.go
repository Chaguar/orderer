/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/batcher"
	"github.com/Chaguar/orderer/node/comm/tlsgen"
	node_config "github.com/Chaguar/orderer/node/config"
	protos "github.com/Chaguar/orderer/node/protos/comm"
	"github.com/Chaguar/orderer/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// Scenario:
//  1. Start primary 1, which reads the stream of forwarded requests.
//  2. Create a link to it, and expect no stream to be opened before a request needs it.
//  3. Deliver requests marked 1, 2 and 3, one after another.
//  4. Expect primary 1 to receive all three, on one stream.
func TestPrimaryLinkConnectsOnTheFirstDeliverAndReusesTheStream(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	primary := newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, primary)
	link := newLink(t, nodes, batchers, 2, 1, 30*time.Second, 100*time.Millisecond)

	require.Never(t, func() bool { return primary.streams.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond)
	for mark := range byte(3) {
		link.Deliver(context.Background(), 1, &protos.FwdRequest{Request: []byte{mark + 1}})
	}
	require.Eventually(t, func() bool {
		return primary.byMark[1].Load() == 1 && primary.byMark[2].Load() == 1 && primary.byMark[3].Load() == 1
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), primary.streams.Load())
}

// Scenario:
//  1. Start primary 1, which ends the first stream of forwarded requests after one request.
//  2. Deliver a request, and wait until primary 1 has received it and ended the stream.
//  3. Keep delivering requests marked 2, and expect primary 1 to receive one on a second stream. The ones sent before
//     the link notices that the stream ended are lost, as with any gRPC stream that ends.
func TestPrimaryLinkReconnectsAfterTheStreamEnds(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	primary := newFakePrimary(true)
	primary.endAfter = 1
	nodes, batchers := serveFakePrimaries(t, ca, primary)
	link := newLink(t, nodes, batchers, 2, 1, 30*time.Second, 100*time.Millisecond)

	link.Deliver(context.Background(), 1, &protos.FwdRequest{Request: []byte{1}})
	require.Eventually(t, func() bool { return primary.ended.Load() == 1 }, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		link.Deliver(context.Background(), 1, &protos.FwdRequest{Request: []byte{2}})
		return primary.byMark[2].Load() > 0
	}, 10*time.Second, 20*time.Millisecond)
	require.Equal(t, int32(2), primary.streams.Load())
}

// Scenario:
//  1. Start primary 1, which accepts the stream of forwarded requests but never reads from it.
//  2. Create a link with a send timeout of 300 ms, and deliver requests until a send blocks on the full stream.
//  3. Expect the link to cut the blocked send and open a second stream, no earlier than the send timeout after the
//     first delivery.
func TestPrimaryLinkCutsASendThatHangs(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	stalled := newFakePrimary(false)
	nodes, batchers := serveFakePrimaries(t, ca, stalled)
	link := newLink(t, nodes, batchers, 2, 1, 300*time.Millisecond, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	start := time.Now()
	go func() {
		request := make([]byte, 64*1024)
		for ctx.Err() == nil {
			link.Deliver(ctx, 1, &protos.FwdRequest{Request: request})
		}
	}()
	for i := range 2 {
		select {
		case <-stalled.accepted:
		case <-time.After(10 * time.Second):
			t.Fatalf("the link opened %d streams, want 2: the hung send was not cut", i)
		}
	}
	require.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
}

// Scenario:
//  1. Create a link to primary 1, whose server is down, with a retry delay of an hour.
//  2. Deliver a request: connecting is refused at once, and expect the call not to return while the link waits to
//     retry.
//  3. Retarget the link to primary 2, and expect the call to return.
func TestPrimaryLinkRetargetEndsTheBackoff(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	nodes, batchers := serveFakePrimaries(t, ca, newFakePrimary(true), newFakePrimary(true))
	nodes[0].Stop()
	link := newLink(t, nodes, batchers, 3, 1, 30*time.Second, time.Hour)

	returned := deliverInBackground(link, context.Background(), 1, []byte{1})
	require.Never(t, func() bool { return closed(returned) }, 300*time.Millisecond, 10*time.Millisecond)
	link.Retarget(2)
	requireClosed(t, returned, "the call waiting to retry did not return after the retarget")
}

// Scenario:
//  1. Listen for primary 1 on a port that accepts TCP connections but never completes the TLS handshake, as a primary on
//     a host that stopped responding. Start primary 2, which reads the stream of forwarded requests.
//  2. Deliver a request to primary 1, and wait until the link is connecting to it.
//  3. Retarget the link to primary 2 and back to primary 1, while the first attempt is still connecting.
//  4. Expect the call to return well before the attempt to connect fails (20 s): its target moved, even though it is
//     the target again. Expect no stream to primary 2.
//  5. Deliver another request to primary 1, and expect a new connection rather than the abandoned one.
func TestPrimaryLinkRetargetStopsTheConnecting(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	reading := newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, newFakePrimary(true), reading)
	var accepted func() int
	batchers[1].Endpoint, accepted = silentEndpoint(t)
	link := newLink(t, nodes, batchers, 3, 1, 30*time.Second, 100*time.Millisecond)

	returned := deliverInBackground(link, context.Background(), 1, []byte{1})
	require.Eventually(t, func() bool { return accepted() == 1 }, 10*time.Second, 5*time.Millisecond)
	link.Retarget(2)
	link.Retarget(1)
	requireClosed(t, returned, "the call connecting to primary 1 did not return after its target moved")
	require.Zero(t, reading.streams.Load())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	deliverInBackground(link, ctx, 1, []byte{1})
	require.Eventually(t, func() bool { return accepted() == 2 }, 10*time.Second, 5*time.Millisecond)
}

// Scenario:
//  1. Start primary 1, which accepts the stream of forwarded requests but never reads from it, and primary 2.
//  2. Create a link with a send timeout of an hour, and deliver requests until a send blocks on the full stream.
//  3. Retarget the link to primary 2.
//  4. Expect the blocked call to return, the stream to primary 1 to end, and no stream to primary 2.
func TestPrimaryLinkRetargetEndsABlockedSend(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	stalled, other := newFakePrimary(false), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, stalled, other)
	link := newLink(t, nodes, batchers, 3, 1, time.Hour, 100*time.Millisecond)

	done := deliverUntilBlocked(t, link, context.Background(), 1)
	link.Retarget(2)
	requireClosed(t, done, "the blocked send did not return after the retarget")
	require.Eventually(t, func() bool { return stalled.ended.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
	require.Zero(t, other.streams.Load())
}

// Scenario:
//  1. Start primaries 1 and 2, both reading the stream of forwarded requests, and create a link of party 2 to
//     primary 1.
//  2. Deliver a request to primary 2, which is not the target, and expect the call to return.
//  3. Retarget the link to party 2, as when this batcher becomes the primary, and deliver a request to it.
//  4. Expect the call to return, and no stream to be opened to either party.
func TestPrimaryLinkSkipsAStaleTargetAndItself(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	other, self := newFakePrimary(true), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, other, self)
	link := newLink(t, nodes, batchers, 2, 1, 30*time.Second, 100*time.Millisecond)

	requireClosed(t, deliverInBackground(link, context.Background(), 2, []byte{1}), "a delivery to a stale target did not return")
	link.Retarget(2)
	requireClosed(t, deliverInBackground(link, context.Background(), 2, []byte{1}), "a delivery to this batcher did not return")
	require.Never(t, func() bool { return other.streams.Load()+self.streams.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond)
}

// Scenario:
//  1. In each case, block a delivery to primary 1 and then end its context:
//     - while connecting: primary 1 listens on a port that never completes the TLS handshake;
//     - while waiting to retry: primary 1's server is down, and the retry delay is an hour;
//     - while sending: primary 1 never reads its stream, the send timeout is an hour, and requests are delivered
//     until a send blocks.
//  2. Expect the call to return, and after Disconnect, the stream to primary 1 (if any) to have ended.
func TestPrimaryLinkDeliverEndsWithItsContext(t *testing.T) {
	t.Run("while connecting", func(t *testing.T) {
		ca, err := tlsgen.NewCA()
		require.NoError(t, err)
		nodes, batchers := serveFakePrimaries(t, ca, newFakePrimary(true))
		var accepted func() int
		batchers[1].Endpoint, accepted = silentEndpoint(t)
		link := newLink(t, nodes, batchers, 2, 1, 30*time.Second, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		returned := deliverInBackground(link, ctx, 1, []byte{1})
		require.Eventually(t, func() bool { return accepted() == 1 }, 10*time.Second, 5*time.Millisecond)
		cancel()
		requireClosed(t, returned, "the call connecting did not return with its context")
	})

	t.Run("while waiting to retry", func(t *testing.T) {
		ca, err := tlsgen.NewCA()
		require.NoError(t, err)
		nodes, batchers := serveFakePrimaries(t, ca, newFakePrimary(true))
		nodes[0].Stop()
		link := newLink(t, nodes, batchers, 2, 1, 30*time.Second, time.Hour)

		ctx, cancel := context.WithCancel(context.Background())
		returned := deliverInBackground(link, ctx, 1, []byte{1})
		require.Never(t, func() bool { return closed(returned) }, 300*time.Millisecond, 10*time.Millisecond)
		cancel()
		requireClosed(t, returned, "the call waiting to retry did not return with its context")
	})

	t.Run("while sending", func(t *testing.T) {
		ca, err := tlsgen.NewCA()
		require.NoError(t, err)
		stalled := newFakePrimary(false)
		nodes, batchers := serveFakePrimaries(t, ca, stalled)
		link := newLink(t, nodes, batchers, 2, 1, time.Hour, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		done := deliverUntilBlocked(t, link, ctx, 1)
		cancel()
		requireClosed(t, done, "the blocked send did not return with its context")
		link.Disconnect()
		require.Eventually(t, func() bool { return stalled.ended.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
	})
}

// newLink creates a link of party self in shard 0 to primary, with the given send timeout and first retry delay
// (doubling up to a minute), and disconnects it at the end of the test.
func newLink(t *testing.T, nodes []*node, batchers map[types.PartyID]*batcher.EndpointAndCerts, self, primary types.PartyID, sendTimeout, minRetry time.Duration) *batcher.PrimaryLink {
	conf := &node_config.BatcherNodeConfig{
		PartyId:            self,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[0].TLSKey,
		TLSCertificateFile: nodes[0].TLSCert,
	}
	link := batcher.NewPrimaryLink(testutil.CreateLogger(t, int(self)), conf, batchers, "forwarded requests", openFwdRequestStream, primary,
		batcher.LinkTiming{SendTimeout: sendTimeout, MinRetryInterval: minRetry, MaxRetryDelay: max(minRetry, time.Minute)})
	t.Cleanup(link.Disconnect)
	return link
}

func openFwdRequestStream(ctx context.Context, conn *grpc.ClientConn) (grpc.ClientStream, error) {
	return protos.NewBatcherControlServiceClient(conn).FwdRequestStream(ctx)
}

// deliverInBackground delivers req to target from a new goroutine, and returns a channel closed when the call returns.
func deliverInBackground(link *batcher.PrimaryLink, ctx context.Context, target types.PartyID, req []byte) chan struct{} {
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		link.Deliver(ctx, target, &protos.FwdRequest{Request: req})
	}()
	return returned
}

// deliverUntilBlocked delivers requests to target from a new goroutine, one after another, until one blocks because
// the stream is full; the goroutine stops after that call returns. It returns a channel closed when the goroutine ends.
func deliverUntilBlocked(t *testing.T, link *batcher.PrimaryLink, ctx context.Context, target types.PartyID) chan struct{} {
	var returned atomic.Int32
	var blocked atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		request := make([]byte, 64*1024)
		for !blocked.Load() {
			link.Deliver(ctx, target, &protos.FwdRequest{Request: request})
			returned.Add(1)
		}
	}()
	waitUntilSendsBlock(t, &returned, math.MaxInt32)
	blocked.Store(true)
	return done
}

// requireClosed fails the test unless c is closed within 10 s.
func requireClosed(t *testing.T, c chan struct{}, msg string) {
	select {
	case <-c:
	case <-time.After(10 * time.Second):
		t.Fatal(msg)
	}
}

func closed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
