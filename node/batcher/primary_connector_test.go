/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/batcher"
	"github.com/Chaguar/orderer/node/comm"
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

// Scenario:
//  1. Start a shard of 4 batchers, with party 1 the primary, and a connector of party 3 to it.
//  2. Submit a request to party 1, and forward another through the connector; expect both batched by parties 1 and 3.
//  3. Stop party 1, change the term so that party 2 becomes the primary, and switch the connector to it.
//  4. Submit a request to party 2, and forward another through the connector; expect both batched by parties 2 and 3.
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

	connector := batcher.CreatePrimaryConnector(1, loggers[2], configs[2], batcher.GetBatchersEndpointsAndCerts(configs[2].Shards[0].Batchers), context.Background(), defaultTiming, defaultTiming)
	t.Cleanup(connector.Stop)

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
}

// fakePrimary is a batcher control service. It accepts the streams of forwarded requests and of acks, reads each kind
// only if told to, and otherwise holds the stream open without reading. It counts the streams of each kind, the
// requests it reads (by their first byte), and records the acks it reads in order.
type fakePrimary struct {
	protos.UnimplementedBatcherControlServiceServer
	reads     bool  // reads the streams of forwarded requests
	readsAcks bool  // reads the streams of acks
	endAfter  int32 // if positive, the first stream of forwarded requests ends after this many requests
	accepted  chan struct{}
	streams   atomic.Int32 // streams of forwarded requests accepted
	ended     atomic.Int32 // streams of forwarded requests that ended
	requests  atomic.Int32
	byMark    [256]atomic.Int32 // requests read, by their first byte

	ackStreams atomic.Int32
	ackEnded   atomic.Int32
	ackLock    sync.Mutex
	acked      []*protos.Ack // the acks read, in order
}

func newFakePrimary(reads bool) *fakePrimary {
	return &fakePrimary{reads: reads, readsAcks: reads, accepted: make(chan struct{}, 1000)}
}

// ackedSeqs returns the batches acked so far, in the order read.
func (fp *fakePrimary) ackedSeqs() []uint64 {
	fp.ackLock.Lock()
	defer fp.ackLock.Unlock()
	seqs := make([]uint64, 0, len(fp.acked))
	for _, ack := range fp.acked {
		seqs = append(seqs, ack.Seq)
	}
	return seqs
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
	defer fp.ackEnded.Add(1)
	fp.ackStreams.Add(1)
	if !fp.readsAcks {
		<-stream.Context().Done()
		return nil
	}
	for {
		ack, err := stream.Recv()
		if err != nil {
			return nil
		}
		fp.ackLock.Lock()
		fp.acked = append(fp.acked, ack)
		fp.ackLock.Unlock()
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

// defaultTiming is the timing of both lanes unless a test needs another: a send timeout of 30 s, and retries after
// 100 ms doubling up to 1 s.
var defaultTiming = batcher.LinkTiming{SendTimeout: 30 * time.Second, MinRetryInterval: 100 * time.Millisecond, MaxRetryDelay: time.Second}

// newConnector creates a connector of party self in shard 3 to primary, with the given timing for forwards and for acks,
// and stops it at the end of the test.
func newConnector(t *testing.T, nodes []*node, batchers map[types.PartyID]*batcher.EndpointAndCerts, self, primary types.PartyID, forwards, acks batcher.LinkTiming) *batcher.PrimaryConnector {
	conf := &node_config.BatcherNodeConfig{
		PartyId:            self,
		ShardId:            3,
		TLSPrivateKeyFile:  nodes[0].TLSKey,
		TLSCertificateFile: nodes[0].TLSCert,
	}
	connector := batcher.CreatePrimaryConnector(primary, testutil.CreateLogger(t, int(self)), conf, batchers, context.Background(), forwards, acks)
	t.Cleanup(connector.Stop)
	return connector
}

// fillUntilBlocked forwards requests marked mark from one goroutine until SendReq blocks, which shows that the stream
// and the queue are full. It returns a function that stops the goroutine after the call in progress, and a channel
// closed when the goroutine has returned.
func fillUntilBlocked(t *testing.T, connector *batcher.PrimaryConnector, mark byte) (func(), chan struct{}) {
	request := make([]byte, 64*1024)
	request[0] = mark
	var returned atomic.Int32
	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range fillCalls {
			if stop.Load() {
				return
			}
			connector.SendReq(request)
			returned.Add(1)
		}
	}()
	waitUntilSendsBlock(t, &returned, fillCalls)
	return func() { stop.Store(true) }, done
}

// fillCalls is how many requests fillUntilBlocked forwards: more than the stream and the queue hold.
const fillCalls = 5000

// silentEndpoint listens on an address that accepts TCP connections but never answers, as a primary on a host that
// stopped responding. It returns the address, and a function that returns how many connections it accepted. A link
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
//  1. Start primary 1, which reads both kinds of streams.
//  2. Forward 10 requests, and ack batches 0 to 99 to primary 1.
//  3. Expect primary 1 to receive the 10 requests on one stream of forwarded requests, and the 100 acks, in order and
//     for shard 3, on one stream of acks.
func TestPrimaryConnectorRoutesEachLane(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	primary := newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, primary)
	connector := newConnector(t, nodes, batchers, 2, 1, defaultTiming, defaultTiming)

	for range 10 {
		connector.SendReq([]byte{1})
	}
	for seq := range types.BatchSequence(100) {
		connector.SendAck(seq, 1)
	}
	require.Eventually(t, func() bool { return primary.byMark[1].Load() == 10 && len(primary.ackedSeqs()) == 100 }, 10*time.Second, 10*time.Millisecond)
	for i, seq := range primary.ackedSeqs() {
		require.Equal(t, uint64(i), seq)
	}
	primary.ackLock.Lock()
	for _, ack := range primary.acked {
		require.Equal(t, uint32(3), ack.Shard)
	}
	primary.ackLock.Unlock()
	require.Equal(t, int32(1), primary.streams.Load())
	require.Equal(t, int32(1), primary.ackStreams.Load())
}

// Scenario:
//  1. Start primaries 1 and 2, both reading both kinds of streams, and create a connector to primary 1.
//  2. Ack batch 0 to primary 2, which is not the target, and then batch 1 to primary 1.
//  3. Expect primary 1 to receive only the ack of batch 1, and primary 2 nothing: an ack goes only to the primary it
//     is for.
func TestPrimaryConnectorSendsAnAckOnlyToItsPrimary(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	target, other := newFakePrimary(true), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, target, other)
	connector := newConnector(t, nodes, batchers, 3, 1, defaultTiming, defaultTiming)

	connector.SendAck(0, 2)
	connector.SendAck(1, 1)
	require.Eventually(t, func() bool { return len(target.ackedSeqs()) == 1 }, 10*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return len(target.ackedSeqs()) > 1 || other.ackStreams.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond)
	require.Equal(t, []uint64{1}, target.ackedSeqs())
}

// Scenario:
//  1. Start primary 1, which accepts both kinds of streams but never reads them, and this batcher, party 2.
//  2. Ack a batch to primary 1, and forward requests to it from one goroutine until a call blocks, because the stream and
//     the queue are full. The goroutine stops after the call that is blocked during the switch.
//  3. Switch the connector to party 2, as when this batcher becomes the primary.
//  4. Expect the blocked call to return, and both streams to primary 1 to end.
//  5. Forward 10 requests and ack 10 batches to party 2, and expect no stream to be opened to it.
func TestPrimaryConnectorSendsNothingAsThePrimary(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	stalled, self := newFakePrimary(false), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, stalled, self)
	connector := newConnector(t, nodes, batchers, 2, 1, defaultTiming, defaultTiming)

	connector.SendAck(0, 1)
	require.Eventually(t, func() bool { return stalled.ackStreams.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
	stopFilling, filled := fillUntilBlocked(t, connector, 1)

	stopFilling()
	connector.ConnectToNewPrimary(2)
	requireClosed(t, filled, "the forwards blocked on the old primary did not return")
	require.Eventually(t, func() bool { return stalled.ended.Load() == 1 && stalled.ackEnded.Load() == 1 }, 10*time.Second, 10*time.Millisecond)

	for seq := range types.BatchSequence(10) {
		connector.SendReq([]byte{2})
		connector.SendAck(seq, 2)
	}
	require.Never(t, func() bool { return self.streams.Load()+self.ackStreams.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond)
}

// Scenario:
//  1. Start primary 1, which accepts both kinds of streams but never reads them, and primary 2, which reads them.
//  2. Ack batches 0 to 1999 to primary 1, more than its stream and the queue hold, and forward requests marked 1 to it
//     from one goroutine until a call blocks. The goroutine stops after the call that is blocked during the switch.
//  3. Switch the connector to primary 2.
//  4. Expect the blocked call to return, and both streams to primary 1 to end.
//  5. Forward 10 requests marked 2, and ack batches 5000 to 5009 to primary 2.
//  6. Expect primary 2 to receive exactly those: none of the requests or acks queued for primary 1.
func TestPrimaryConnectorDropsTheOldPrimarysMessagesOnASwitch(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	stalled, reading := newFakePrimary(false), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, stalled, reading)
	connector := newConnector(t, nodes, batchers, 3, 1, defaultTiming, defaultTiming)

	for seq := range types.BatchSequence(2000) {
		connector.SendAck(seq, 1)
	}
	stopFilling, filled := fillUntilBlocked(t, connector, 1)

	stopFilling()
	connector.ConnectToNewPrimary(2)
	requireClosed(t, filled, "the forwards blocked on the old primary did not return")
	require.Eventually(t, func() bool { return stalled.ended.Load() == 1 && stalled.ackEnded.Load() == 1 }, 10*time.Second, 10*time.Millisecond)

	for i := range types.BatchSequence(10) {
		connector.SendReq([]byte{2})
		connector.SendAck(5000+i, 2)
	}
	require.Eventually(t, func() bool { return reading.byMark[2].Load() == 10 && len(reading.ackedSeqs()) == 10 }, 10*time.Second, 10*time.Millisecond)
	require.Zero(t, reading.byMark[1].Load())
	require.Equal(t, []uint64{5000, 5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009}, reading.ackedSeqs())
}

// Scenario:
//  1. Create a connector with a forward send timeout of 300 ms to primary 1, whose server is down, so nothing is ever
//     sent.
//  2. Forward requests until the queue is full, and forward one more: expect the call to wait the send timeout and then
//     return, dropping the request.
//  3. Ack 5000 batches, more than the queue holds, and expect every call to return without waiting.
func TestPrimaryConnectorFullQueues(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	nodes, batchers := serveFakePrimaries(t, ca, newFakePrimary(true))
	nodes[0].Stop()
	forwards := defaultTiming
	forwards.SendTimeout = 300 * time.Millisecond
	connector := newConnector(t, nodes, batchers, 2, 1, forwards, defaultTiming)

	for range 1001 { // one taken by the forwarding goroutine, 1000 in the queue
		connector.SendReq([]byte{1})
	}
	start := time.Now()
	connector.SendReq([]byte{1})
	waited := time.Since(start)
	require.GreaterOrEqual(t, waited, 300*time.Millisecond)
	require.Less(t, waited, 10*time.Second)

	acked := make(chan struct{})
	go func() {
		defer close(acked)
		for seq := range types.BatchSequence(5000) {
			connector.SendAck(seq, 1)
		}
	}()
	requireClosed(t, acked, "acking blocked on a full queue")
}

// Scenario:
//  1. Start primary 1, which reads the stream of acks but never the stream of forwarded requests.
//  2. Forward requests until a call blocks, because the stream and the queue are full.
//  3. Ack batches 0 to 99, and expect primary 1 to receive all of them: the acks don't wait behind the forwards.
func TestPrimaryConnectorAcksPassStuckForwards(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	primary := newFakePrimary(true)
	primary.reads = false
	nodes, batchers := serveFakePrimaries(t, ca, primary)
	connector := newConnector(t, nodes, batchers, 2, 1, defaultTiming, defaultTiming)

	fillUntilBlocked(t, connector, 1)
	for seq := range types.BatchSequence(100) {
		connector.SendAck(seq, 1)
	}
	require.Eventually(t, func() bool { return len(primary.ackedSeqs()) == 100 }, 10*time.Second, 10*time.Millisecond)
}

// Scenario:
//  1. Listen for primary 1 on a port that accepts TCP connections but never completes the TLS handshake, so both lanes
//     stay stuck connecting (20 s).
//  2. Ack a batch, and forward requests until the queue is full and a call waits for room.
//  3. Stop the connector, and expect Stop and the waiting call to return well before the attempts to connect fail.
//  4. Expect later calls to return at once.
func TestPrimaryConnectorStopsWhileBothLanesAreConnecting(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	nodes, batchers := serveFakePrimaries(t, ca, newFakePrimary(true))
	var accepted func() int
	batchers[1].Endpoint, accepted = silentEndpoint(t)
	conf := &node_config.BatcherNodeConfig{PartyId: 2, ShardId: 3, TLSPrivateKeyFile: nodes[0].TLSKey, TLSCertificateFile: nodes[0].TLSCert}
	connector := batcher.CreatePrimaryConnector(1, testutil.CreateLogger(t, 2), conf, batchers, context.Background(), defaultTiming, defaultTiming)

	connector.SendAck(0, 1)
	for range 1001 { // one taken by the forwarding goroutine, 1000 in the queue
		connector.SendReq([]byte{1})
	}
	require.Eventually(t, func() bool { return accepted() == 2 }, 10*time.Second, 5*time.Millisecond, "both lanes should be connecting")
	waiting := make(chan struct{})
	go func() {
		defer close(waiting)
		connector.SendReq([]byte{1})
	}()

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		connector.Stop()
	}()
	requireClosed(t, stopped, "Stop did not return while both lanes were connecting")
	requireClosed(t, waiting, "the call waiting for room did not return after Stop")

	later := make(chan struct{})
	go func() {
		defer close(later)
		connector.SendReq([]byte{1})
		connector.SendAck(1, 1)
	}()
	requireClosed(t, later, "a call after Stop did not return")
}

// Scenario:
//  1. Start primary 1, which reads both kinds of streams, behind a listener that counts its open connections.
//  2. Forward a request and ack a batch, and wait until primary 1 has both, over two open connections.
//  3. Stop the connector, and expect both connections to close.
func TestPrimaryConnectorStopClosesItsConnections(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	nodes := createNodes(t, ca, 1) // for its TLS identity; primary 1 is served on its own listener below
	_, listener := testutil.SharedTestPortAllocator().Allocate(t)
	counting := &countingListener{Listener: listener}
	server, err := comm.NewGRPCServerFromListener(counting, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     [][]byte{ca.CertBytes()},
			Key:               nodes[0].TLSKey,
			Certificate:       nodes[0].TLSCert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     [][]byte{ca.CertBytes()},
		},
	})
	require.NoError(t, err)
	primary := newFakePrimary(true)
	protos.RegisterBatcherControlServiceServer(server.Server(), primary)
	go func() { _ = server.Start() }()
	t.Cleanup(server.Stop)
	batchers := batcher.GetBatchersEndpointsAndCerts(createBatchersInfo(1, nodes, ca))
	batchers[1].Endpoint = listener.Addr().String()
	connector := newConnector(t, nodes, batchers, 2, 1, defaultTiming, defaultTiming)

	connector.SendReq([]byte{1})
	connector.SendAck(0, 1)
	require.Eventually(t, func() bool {
		return primary.requests.Load() == 1 && len(primary.ackedSeqs()) == 1 && counting.open.Load() == 2
	}, 10*time.Second, 10*time.Millisecond)

	connector.Stop()
	require.Eventually(t, func() bool { return counting.open.Load() == 0 }, 10*time.Second, 10*time.Millisecond)
}

// countingListener counts the connections it accepted that are still open.
type countingListener struct {
	net.Listener
	open atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.open.Add(1)
	return &countedConn{Conn: conn, listener: l}, nil
}

// countedConn is a connection that a countingListener accepted.
type countedConn struct {
	net.Conn
	listener *countingListener
	closed   sync.Once
}

func (c *countedConn) Close() error {
	c.closed.Do(func() { c.listener.open.Add(-1) })
	return c.Conn.Close()
}

// Scenario:
//  1. Start primary 1, which reads both kinds of streams.
//  2. From each of 16 goroutines at once, forward 100 requests and ack 50 batches.
//  3. Expect primary 1 to receive all 1600 requests and 800 acks, on one stream of each kind.
func TestPrimaryConnectorSendsFromManyGoroutines(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	primary := newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, primary)
	connector := newConnector(t, nodes, batchers, 2, 1, defaultTiming, defaultTiming)

	var senders sync.WaitGroup
	for g := range types.BatchSequence(16) {
		senders.Go(func() {
			for i := range types.BatchSequence(100) {
				connector.SendReq([]byte{1})
				if i < 50 {
					connector.SendAck(g*50+i, 1)
				}
			}
		})
	}
	senders.Wait()
	require.Eventually(t, func() bool { return primary.requests.Load() == 1600 && len(primary.ackedSeqs()) == 800 }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), primary.streams.Load())
	require.Equal(t, int32(1), primary.ackStreams.Load())
}

// Scenario:
//  1. Start primary 1, which reads both kinds of streams, and forward a request marked 1 and ack batch 0 to it.
//  2. Stop primary 1's server, and forward a request and ack a batch while it is down.
//  3. Serve primary 1 again at the same address, without any primary change.
//  4. Keep forwarding requests marked 2 and acking batches, and expect the new server to receive both kinds: a send just
//     after the stop can still succeed into the dead connection and be lost, so each lane reconnects on a later one.
func TestPrimaryConnectorReconnectsWhenThePrimaryComesBack(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	first := newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, first)
	connector := newConnector(t, nodes, batchers, 2, 1, defaultTiming, defaultTiming)

	connector.SendReq([]byte{1})
	connector.SendAck(0, 1)
	require.Eventually(t, func() bool { return first.byMark[1].Load() == 1 && len(first.ackedSeqs()) == 1 }, 10*time.Second, 10*time.Millisecond)

	address := nodes[0].Address()
	nodes[0].Stop()
	connector.SendReq([]byte{2})
	connector.SendAck(1, 1)

	keyPair, err := ca.NewServerCertKeyPair(localhost)
	require.NoError(t, err)
	server, err := newGRPCServer(address, ca, keyPair)
	require.NoError(t, err)
	back := newFakePrimary(true)
	protos.RegisterBatcherControlServiceServer(server.Server(), back)
	go func() { _ = server.Start() }()
	t.Cleanup(server.Stop)

	seq := types.BatchSequence(2)
	require.Eventually(t, func() bool {
		connector.SendReq([]byte{2})
		connector.SendAck(seq, 1)
		seq++
		return back.byMark[2].Load() > 0 && len(back.ackedSeqs()) > 0
	}, 10*time.Second, 50*time.Millisecond)
}
