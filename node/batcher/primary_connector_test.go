/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/batcher"
	"github.com/Chaguar/orderer/node/comm/tlsgen"
	node_config "github.com/Chaguar/orderer/node/config"
	"github.com/Chaguar/orderer/node/consensus/state"
	protos "github.com/Chaguar/orderer/node/protos/comm"
	"github.com/Chaguar/orderer/testutil"
	"github.com/Chaguar/orderer/testutil/pinning"
	"github.com/Chaguar/orderer/testutil/tx"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestPrimaryConnector(t *testing.T) {
	shardID := types.ShardID(0)
	numParties := 4
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherNodes := createNodes(t, ca, numParties)
	batchersInfo := createBatchersInfo(numParties, batcherNodes, ca)

	consenterNodes := createNodes(t, ca, numParties)
	consentersInfo := createConsentersInfo(numParties, consenterNodes, ca)

	stubConsenters, clean := createConsenterStubs(t, consenterNodes, numParties)
	defer clean()

	routerKeyPairs := pinning.CreateRouterKeyPairs(t, ca, numParties)
	batchers, loggers, configs, clean := createBatchers(t, numParties, shardID, batcherNodes, batchersInfo, consentersInfo, routerKeyPairs, stubConsenters)
	defer clean()

	require.Equal(t, types.PartyID(1), batchers[1].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())

	connector := batcher.CreatePrimaryReqConnector(1, loggers[2], configs[2], batcher.GetBatchersEndpointsAndCerts(configs[2].Shards[0].Batchers), context.Background(), 10*time.Second, 100*time.Millisecond, 1*time.Second)

	// send request via normal submit
	batchers[0].Submit(routerContext(t, routerKeyPairs[0]), tx.CreateStructuredRequest([]byte{1}))

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(1) && batchers[2].Ledger.Height(1) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// send request to primary via connector
	req2, _ := proto.Marshal(tx.CreateStructuredRequest([]byte{2}))
	connector.SendReq(req2)

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(2) && batchers[2].Ledger.Height(1) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	require.Equal(t, types.PartyID(1), batchers[1].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())

	// stop the primary
	batchers[0].Stop()

	// change term
	termChangeState := &state.State{Shards: []state.ShardTerm{{Shard: shardID, Term: 1}}}
	for i := 1; i < numParties; i++ {
		stubConsenters[i].UpdateState(termChangeState)
	}

	require.Eventually(t, func() bool {
		return batchers[1].GetPrimaryID() == types.PartyID(2) && batchers[2].GetPrimaryID() == types.PartyID(2)
	}, 30*time.Second, 10*time.Millisecond)

	// update the connector
	connector.ConnectToNewPrimary(2)

	// send request via normal submit
	batchers[1].Submit(routerContext(t, routerKeyPairs[1]), tx.CreateStructuredRequest([]byte{3}))

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(2) == uint64(1) && batchers[2].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// send request via the connector to a new primary
	req4, _ := proto.Marshal(tx.CreateStructuredRequest([]byte{4}))
	connector.SendReq(req4)

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(2) == uint64(2) && batchers[2].Ledger.Height(2) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	connector.Stop()
}

// fakePrimary is a batcher control service. Unless it reads, it accepts the streams of forwarded
// requests and acks and never reads from them. It counts the streams it accepts and the requests it reads.
type fakePrimary struct {
	protos.UnimplementedBatcherControlServiceServer
	reads    bool
	endAfter int32 // if positive, the first stream of forwarded requests ends after this many requests
	accepted chan struct{}
	streams  atomic.Int32
	ended    atomic.Int32 // streams of forwarded requests that ended
	requests atomic.Int32
	byMark   [256]atomic.Int32 // requests read, by their first byte
}

func newFakePrimary(reads bool) *fakePrimary {
	return &fakePrimary{reads: reads, accepted: make(chan struct{}, 1000)}
}

func (fp *fakePrimary) FwdRequestStream(stream protos.BatcherControlService_FwdRequestStreamServer) error {
	defer fp.ended.Add(1)
	fp.streams.Add(1)
	fp.accepted <- struct{}{}
	if !fp.reads {
		<-stream.Context().Done()
		return nil
	}
	first := fp.streams.Load() == 1
	for {
		msg, err := stream.Recv()
		if err != nil {
			return nil
		}
		if len(msg.Request) > 0 {
			fp.byMark[msg.Request[0]].Add(1)
		}
		if n := fp.requests.Add(1); first && n == fp.endAfter {
			return nil
		}
	}
}

func (fp *fakePrimary) NotifyAck(stream protos.BatcherControlService_NotifyAckServer) error {
	fp.streams.Add(1)
	fp.accepted <- struct{}{}
	if !fp.reads {
		<-stream.Context().Done()
		return nil
	}
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

// serveFakePrimaries serves fake primary i on node i of the shard, and returns the shard's batchers.
func serveFakePrimaries(
	t *testing.T, ca tlsgen.CA, primaries ...*fakePrimary,
) ([]*node, map[types.PartyID]*batcher.EndpointAndCerts) {
	nodes := createNodes(t, ca, len(primaries))
	for i, primary := range primaries {
		protos.RegisterBatcherControlServiceServer(nodes[i].Server(), primary)
		go func() { _ = nodes[i].Start() }()
		t.Cleanup(nodes[i].Stop)
	}
	return nodes, batcher.GetBatchersEndpointsAndCerts(createBatchersInfo(len(primaries), nodes, ca))
}

// waitUntilSendsBlock waits until the count of returned sends stops growing while some are still blocked,
// which shows that the stream to the primary is full.
func waitUntilSendsBlock(t *testing.T, returned *atomic.Int32, senders int32) {
	last := int32(-1)
	require.Eventually(t, func() bool {
		n := returned.Load()
		full := n == last && n < senders
		last = n
		return full
	}, 10*time.Second, 100*time.Millisecond)
}

// newReqConnector creates a request connector of party self to the given primary, with a send timeout of 30 s.
func newReqConnector(t *testing.T, nodes []*node, batchers map[types.PartyID]*batcher.EndpointAndCerts, self, primary types.PartyID) *batcher.PrimaryReqConnector {
	conf := &node_config.BatcherNodeConfig{
		PartyId:            self,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[0].TLSKey,
		TLSCertificateFile: nodes[0].TLSCert,
	}
	connector := batcher.CreatePrimaryReqConnector(
		primary, testutil.CreateLogger(t, int(self)), conf, batchers, context.Background(),
		30*time.Second, 100*time.Millisecond, time.Second,
	)
	t.Cleanup(connector.Stop)
	return connector
}

// fillUntilBlocked forwards requests from one goroutine until SendReq blocks, which shows that the stream and the
// queue are full, and returns the count of returned calls.
func fillUntilBlocked(t *testing.T, connector *batcher.PrimaryReqConnector) *atomic.Int32 {
	request := make([]byte, 64*1024)
	var returned atomic.Int32
	go func() {
		for range fillCalls {
			connector.SendReq(request)
			returned.Add(1)
		}
	}()
	waitUntilSendsBlock(t, &returned, fillCalls)
	return &returned
}

// fillCalls is how many requests fillUntilBlocked forwards: more than the stream and the queue hold.
const fillCalls = 5000

// silentEndpoint listens on an address that accepts TCP connections but never answers, as a primary on a host that
// stopped responding. It returns the address, and a function that returns how many connections it accepted. A connector
// to it stays stuck opening its stream, so nothing it queues is sent.
func silentEndpoint(t *testing.T) (string, func() int) {
	_, silent := testutil.SharedTestPortAllocator().Allocate(t)
	var accepted sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		silent.Close()
		accepted.Lock()
		defer accepted.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
	})
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}
			accepted.Lock()
			conns = append(conns, conn)
			accepted.Unlock()
		}
	}()
	return silent.Addr().String(), func() int {
		accepted.Lock()
		defer accepted.Unlock()
		return len(conns)
	}
}

// Scenario:
//  1. Start primary 1, which accepts the stream of forwarded requests but never reads from it, and primary 2, which
//     reads it.
//  2. Forward requests marked 1 to primary 1 from one goroutine until a call blocks, because the stream and the queue
//     are full. The goroutine stops after the call that is blocked during the switch.
//  3. Switch the connector to primary 2, and expect the blocked call to return at once, well before the send timeout.
//  4. Forward 10 requests marked 2, and expect primary 2 to receive them and none marked 1.
func TestPrimaryReqConnectorSwitchesPrimaryWhileASendIsBlocked(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	stalled, reading := newFakePrimary(false), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, stalled, reading)
	connector := newReqConnector(t, nodes, batchers, 3, 1)

	marked1 := make([]byte, 64*1024)
	marked1[0] = 1
	var returned atomic.Int32
	var switched atomic.Bool
	filled := make(chan struct{})
	go func() {
		defer close(filled)
		for !switched.Load() {
			connector.SendReq(marked1)
			returned.Add(1)
		}
	}()
	waitUntilSendsBlock(t, &returned, math.MaxInt32)

	switched.Store(true)
	connector.ConnectToNewPrimary(types.PartyID(2))
	select {
	case <-filled:
	case <-time.After(10 * time.Second):
		t.Fatal("the call blocked during the switch did not return")
	}

	for range 10 {
		connector.SendReq([]byte{2})
	}
	require.Eventually(t, func() bool { return reading.byMark[2].Load() == 10 }, 10*time.Second, 10*time.Millisecond)
	require.Zero(t, reading.byMark[1].Load())
}

// Scenario:
//  1. Start primary 1, which reads the stream of forwarded requests.
//  2. Forward 100 requests from each of 16 goroutines at once.
//  3. Expect primary 1 to receive all 1600, on one stream.
func TestPrimaryReqConnectorForwardsFromManyGoroutines(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	reading := newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, reading)
	connector := newReqConnector(t, nodes, batchers, 2, 1)

	var senders sync.WaitGroup
	for range 16 {
		senders.Go(func() {
			for range 100 {
				connector.SendReq([]byte{1})
			}
		})
	}
	senders.Wait()
	require.Eventually(t, func() bool { return reading.requests.Load() == 1600 }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), reading.streams.Load())
}

// Scenario:
//  1. Start primary 1 and this batcher, party 2, both reading the stream of forwarded requests.
//  2. Switch the connector to party 2, as when this batcher becomes the primary.
//  3. Forward 10 requests, and expect every call to return and no stream to be opened, to either party.
func TestPrimaryReqConnectorNeverForwardsToItself(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	other, self := newFakePrimary(true), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, other, self)
	connector := newReqConnector(t, nodes, batchers, 2, 1)

	connector.ConnectToNewPrimary(types.PartyID(2))
	for range 10 {
		connector.SendReq([]byte{1})
	}
	require.Never(t, func() bool { return other.streams.Load()+self.streams.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond)
}

// Scenario:
//  1. Start primary 1, which ends the first stream of forwarded requests after 3 requests.
//  2. Forward 3 requests, and wait until primary 1 has received them and ended the stream.
//  3. Keep forwarding, and expect primary 1 to receive a request on a new stream. The ones sent before the connector
//     notices that the stream ended are lost, as with any gRPC stream that ends.
func TestPrimaryReqConnectorReconnectsAfterTheStreamEnds(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	primary := newFakePrimary(true)
	primary.endAfter = 3
	nodes, batchers := serveFakePrimaries(t, ca, primary)
	connector := newReqConnector(t, nodes, batchers, 2, 1)

	for range 3 {
		connector.SendReq([]byte{1})
	}
	require.Eventually(t, func() bool { return primary.requests.Load() == 3 }, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		connector.SendReq([]byte{1})
		return primary.requests.Load() > 3
	}, 10*time.Second, 20*time.Millisecond)
	require.Equal(t, int32(2), primary.streams.Load())
}

// Scenario:
//  1. Start primary 1, which accepts the stream of forwarded requests but never reads from it.
//  2. Forward requests until a call blocks, because the stream and the queue are full.
//  3. Stop the connector, and expect Stop and the blocked call to return.
func TestPrimaryReqConnectorStopsWhileSendsAreBlocked(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	stalled := newFakePrimary(false)
	nodes, batchers := serveFakePrimaries(t, ca, stalled)
	connector := newReqConnector(t, nodes, batchers, 2, 1)

	returned := fillUntilBlocked(t, connector)
	blockedAt := returned.Load()

	stopped := make(chan struct{})
	go func() {
		connector.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return while a send was blocked")
	}
	require.Eventually(t, func() bool { return returned.Load() > blockedAt }, 10*time.Second, 10*time.Millisecond)
}

// Scenario:
//  1. Start primary 1, which accepts the stream of acks but never reads from it, and this batcher, party 2,
//     which reads it.
//  2. Ack batches to primary 1 one after the other, as a secondary does, until an ack blocks because the
//     stream is full.
//  3. Switch the connector to this batcher, which is now the primary.
//  4. Expect the switch to return, the blocked ack to return, and no ack stream opened to this batcher.
func TestPrimaryAckConnectorDropsAcksToTheOldPrimaryWhenBecomingPrimary(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	stalled, self := newFakePrimary(false), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, stalled, self)
	conf := &node_config.BatcherNodeConfig{
		PartyId:            2,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[1].TLSKey,
		TLSCertificateFile: nodes[1].TLSCert,
	}
	connector := batcher.CreatePrimaryAckConnector(
		1, 0, testutil.CreateLogger(t, 2), conf, batchers, context.Background(),
		5*time.Second, 100*time.Millisecond, 500*time.Millisecond,
	)
	t.Cleanup(connector.Stop)
	connector.ConnectToPrimary()
	<-stalled.accepted

	// One sender acks batch after batch to primary 1, as a secondary does, and counts the acks that
	// returned. It keeps acking after the switch, until stopped.
	var returned atomic.Int32
	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := types.BatchSequence(0); !stop.Load(); seq++ {
			connector.SendAck(seq, types.PartyID(1))
			returned.Add(1)
		}
	}()
	waitUntilSendsBlock(t, &returned, math.MaxInt32)
	blocked := returned.Load()

	switched := make(chan struct{})
	go func() {
		connector.ConnectToNewPrimary(types.PartyID(2))
		close(switched)
	}()
	select {
	case <-switched:
	case <-time.After(10 * time.Second):
		t.Fatal("the connector did not switch to this batcher while an ack was blocked")
	}

	require.Eventually(t, func() bool { return returned.Load() > blocked }, 10*time.Second, 10*time.Millisecond,
		"the ack blocked on the old primary did not return")
	require.Never(t, func() bool { return self.streams.Load() > 0 }, 2*time.Second, 50*time.Millisecond,
		"an ack stream was opened to this batcher")
	stop.Store(true)
	<-done
}

// Scenario:
//  1. Start primary 1, which accepts the stream of forwarded requests but never reads from it.
//  2. Create a connector with a send timeout of 1 s, and forward requests until a send blocks on the full stream.
//  3. Expect the blocked send to be cut after the timeout and the connector to open a new stream.
func TestPrimaryReqConnectorCutsASendThatHangs(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	stalled := newFakePrimary(false)
	nodes, batchers := serveFakePrimaries(t, ca, stalled)
	conf := &node_config.BatcherNodeConfig{
		PartyId:            2,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[0].TLSKey,
		TLSCertificateFile: nodes[0].TLSCert,
	}
	connector := batcher.CreatePrimaryReqConnector(
		1, testutil.CreateLogger(t, 2), conf, batchers, context.Background(),
		300*time.Millisecond, 100*time.Millisecond, time.Second,
	)
	t.Cleanup(connector.Stop)

	request := make([]byte, 64*1024)
	for range 10 {
		connector.SendReq(request)
	}
	require.Eventually(t, func() bool { return stalled.streams.Load() >= 2 }, 10*time.Second, 10*time.Millisecond)
}

// Scenario:
//  1. Create a connector with a send timeout of 300 ms to primary 1, whose server is down, so nothing is ever sent.
//  2. Forward requests until the queue is full.
//  3. Forward one more, and expect the call to wait the send timeout and then return, dropping the request.
func TestPrimaryReqConnectorDropsARequestWhenTheQueueStaysFull(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	nodes, batchers := serveFakePrimaries(t, ca, newFakePrimary(true))
	nodes[0].Stop()
	conf := &node_config.BatcherNodeConfig{
		PartyId:            2,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[0].TLSKey,
		TLSCertificateFile: nodes[0].TLSCert,
	}
	connector := batcher.CreatePrimaryReqConnector(
		1, testutil.CreateLogger(t, 2), conf, batchers, context.Background(),
		300*time.Millisecond, 100*time.Millisecond, time.Second,
	)
	t.Cleanup(connector.Stop)

	for range 1001 { // one taken by the forwarding goroutine, 1000 in the queue
		connector.SendReq([]byte{1})
	}
	start := time.Now()
	connector.SendReq([]byte{1})
	waited := time.Since(start)
	require.GreaterOrEqual(t, waited, 300*time.Millisecond)
	require.Less(t, waited, 10*time.Second)
}

// Scenario:
//  1. Start primaries 1 and 2, both reading the stream of forwarded requests.
//  2. Forward a request to primary 1, so that the connector opens a stream to it.
//  3. Switch the connector to primary 2, and forward nothing more.
//  4. Expect the stream to primary 1 to end, and no stream to be opened to primary 2 before a request needs it.
func TestPrimaryReqConnectorClosesTheOldStreamOnAPrimaryChange(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	oldPrimary, newPrimary := newFakePrimary(true), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, oldPrimary, newPrimary)
	connector := newReqConnector(t, nodes, batchers, 3, 1)

	connector.SendReq([]byte{1})
	require.Eventually(t, func() bool { return oldPrimary.requests.Load() == 1 }, 10*time.Second, 10*time.Millisecond)

	connector.ConnectToNewPrimary(types.PartyID(2))
	require.Eventually(t, func() bool { return oldPrimary.ended.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return newPrimary.streams.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond)
}

// Scenario:
//  1. Start primary 1, which accepts the stream of forwarded requests but never reads from it, and this batcher, party 2.
//  2. Forward requests to primary 1 until a call blocks, because the stream and the queue are full.
//  3. Switch the connector to party 2, as when this batcher becomes the primary.
//  4. Expect the blocked call and every later one to return, the stream to primary 1 to end, and no stream to party 2.
func TestPrimaryReqConnectorBecomesPrimaryWhileSendsAreBlocked(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	stalled, self := newFakePrimary(false), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, stalled, self)
	connector := newReqConnector(t, nodes, batchers, 2, 1)

	returned := fillUntilBlocked(t, connector)
	connector.ConnectToNewPrimary(types.PartyID(2))

	require.Eventually(t, func() bool { return returned.Load() == fillCalls }, 30*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return stalled.ended.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
	require.Zero(t, self.streams.Load())
}

// Scenario:
//  1. Listen for primary 1 on a port that accepts TCP connections but never completes the TLS handshake, as a primary on
//     a host that stopped responding. Start primary 2, which reads the stream of forwarded requests.
//  2. Forward a request, and expect the call to return while the connector starts opening the stream to primary 1,
//     well before that attempt fails (20 s).
//  3. Switch the connector to primary 2, and forward a request marked 2.
//  4. Expect primary 2 to receive it within 10 s of the first call, well before the attempt to reach primary 1 fails.
func TestPrimaryReqConnectorSwitchesPrimaryWhileConnecting(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	unused, reading := newFakePrimary(true), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, unused, reading)

	var accepted func() int
	batchers[1].Endpoint, accepted = silentEndpoint(t)
	connector := newReqConnector(t, nodes, batchers, 3, 1)

	start := time.Now()
	connector.SendReq([]byte{1})
	require.Less(t, time.Since(start), 10*time.Second)
	require.Eventually(t, func() bool { return accepted() > 0 }, 10*time.Second, 5*time.Millisecond)
	connector.ConnectToNewPrimary(types.PartyID(2))
	connector.SendReq([]byte{2})
	require.Eventually(t, func() bool { return reading.byMark[2].Load() == 1 }, 10*time.Second-time.Since(start), 10*time.Millisecond)
}

// Scenario:
//  1. Start primary 1, which reads the stream of forwarded requests, and forward a request to it.
//  2. Stop primary 1's server, and forward a request marked 2 while it is down.
//  3. Serve primary 1 again at the same address, without any primary change.
//  4. Keep forwarding requests marked 2, and expect the new server to receive one: a send just after the stop can still
//     succeed into the dead connection and be lost, so the connector reconnects on a later one.
func TestPrimaryReqConnectorReconnectsWhenThePrimaryComesBack(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	first := newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, first)
	connector := newReqConnector(t, nodes, batchers, 2, 1)

	connector.SendReq([]byte{1})
	require.Eventually(t, func() bool { return first.byMark[1].Load() == 1 }, 10*time.Second, 10*time.Millisecond)

	address := nodes[0].Address()
	nodes[0].Stop()
	connector.SendReq([]byte{2})

	keyPair, err := ca.NewServerCertKeyPair(localhost)
	require.NoError(t, err)
	server, err := newGRPCServer(address, ca, keyPair)
	require.NoError(t, err)
	back := newFakePrimary(true)
	protos.RegisterBatcherControlServiceServer(server.Server(), back)
	go func() { _ = server.Start() }()
	t.Cleanup(server.Stop)

	require.Eventually(t, func() bool {
		connector.SendReq([]byte{2})
		return back.byMark[2].Load() > 0
	}, 10*time.Second, 50*time.Millisecond)
}

// Scenario:
//  1. Create a connector to primary 1, whose server is down, with a retry delay of 30 s after the first failure.
//  2. Forward a request: connecting is refused at once, and the connector waits 30 s before it retries.
//  3. Shortly after, switch the connector to primary 2, which reads, and forward a request marked 2.
//  4. Expect primary 2 to receive it within 15 s: the switch ends the backoff rather than waiting it out.
func TestPrimaryReqConnectorSwitchEndsTheBackoff(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	down, reading := newFakePrimary(true), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, down, reading)
	nodes[0].Stop()
	conf := &node_config.BatcherNodeConfig{
		PartyId:            3,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[0].TLSKey,
		TLSCertificateFile: nodes[0].TLSCert,
	}
	connector := batcher.CreatePrimaryReqConnector(
		1, testutil.CreateLogger(t, 3), conf, batchers, context.Background(),
		30*time.Second, 30*time.Second, time.Minute,
	)
	t.Cleanup(connector.Stop)

	connector.SendReq([]byte{1})
	time.Sleep(200 * time.Millisecond) // a refused connection fails within milliseconds
	switched := time.Now()
	connector.ConnectToNewPrimary(types.PartyID(2))
	connector.SendReq([]byte{2})
	require.Eventually(t, func() bool { return reading.byMark[2].Load() == 1 }, 15*time.Second-time.Since(switched), 10*time.Millisecond)
}
