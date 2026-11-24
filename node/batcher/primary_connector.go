/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chaguar/orderer/common/types"
	node_config "github.com/Chaguar/orderer/node/config"
	protos "github.com/Chaguar/orderer/node/protos/comm"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// laneQueueSize bounds the messages waiting in each lane. While the forward queue is full, SendReq waits, holding back
// the pending store's goroutine that forwards a strike. The ack queue fills only while acks don't get through to a
// primary that keeps making batches, which it does only while other parties confirm them, so an ack dropped then isn't
// needed.
const laneQueueSize = 1000

// PrimaryConnector sends a batcher's messages to the shard's primary: the requests it forwards after their first strike,
// and the acks of the batches it pulled. Each kind has its own lane: a bounded queue, a link with its own stream and
// connection, and one goroutine that takes the messages out in order and delivers them. So an ack never waits behind a
// forward that is retrying, and the stuck stream of one kind can't cut the other. Each message is tagged with the
// primary it is for; one that is no longer the target is dropped, because an ack is meaningful only to the primary
// that made the batch, and the pool starts a request's strike over in the new term.
type PrimaryConnector struct {
	logger      *flogging.FabricLogger
	self        types.PartyID
	shardID     types.ShardID
	forwardWait time.Duration // how long SendReq waits for room in the forward queue
	forwards    lane
	acks        lane
	ackDrops    atomic.Int64 // acks dropped since the ack queue last had room

	ctx     context.Context
	cancel  context.CancelFunc
	running sync.WaitGroup
}

// lane is a queue of messages for the primary and the link that delivers them.
type lane struct {
	link  *PrimaryLink
	queue chan outgoing
}

// outgoing is a message for the primary, and the primary it is for.
type outgoing struct {
	target types.PartyID
	msg    proto.Message
}

// CreatePrimaryConnector creates a connector to primaryID, with the given timing for each lane. It connects a lane at
// its first message.
func CreatePrimaryConnector(
	primaryID types.PartyID, logger *flogging.FabricLogger, config *node_config.BatcherNodeConfig,
	batchers map[types.PartyID]*EndpointAndCerts, ctx context.Context, forwards, acks LinkTiming,
) *PrimaryConnector {
	c := &PrimaryConnector{
		logger:      logger,
		self:        config.PartyId,
		shardID:     config.ShardId,
		forwardWait: forwards.SendTimeout,
		forwards:    newLane(logger, config, batchers, "forwarded requests", openFwdRequestStream, primaryID, forwards),
		acks:        newLane(logger, config, batchers, "acks", openAckStream, primaryID, acks),
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.running.Go(func() { c.run(c.forwards) })
	c.running.Go(func() { c.run(c.acks) })
	return c
}

func newLane(
	logger *flogging.FabricLogger, config *node_config.BatcherNodeConfig, batchers map[types.PartyID]*EndpointAndCerts,
	name string, open func(ctx context.Context, conn *grpc.ClientConn) (grpc.ClientStream, error),
	primaryID types.PartyID, timing LinkTiming,
) lane {
	return lane{
		link:  NewPrimaryLink(logger, config, batchers, name, open, primaryID, timing),
		queue: make(chan outgoing, laneQueueSize),
	}
}

// SendReq queues req for the current primary. While the queue is full it waits, at most the forward send timeout: the
// request is then dropped. It returns at once when this batcher is the primary or the connector is stopped. A waiting
// call doesn't watch the primary: if the primary changes meanwhile, it may still queue its request, which the lane
// then drops; if only the term changes, the primary batches it or rejects it.
func (c *PrimaryConnector) SendReq(req []byte) {
	target := c.forwards.link.CurrentTarget()
	if target == c.self {
		return
	}
	request := outgoing{target: target, msg: &protos.FwdRequest{Request: req}}
	select {
	case c.forwards.queue <- request:
		return
	default:
	}
	timer := time.NewTimer(c.forwardWait)
	defer timer.Stop()
	select {
	case c.forwards.queue <- request:
	case <-timer.C:
		c.logger.Warnf("Dropping a request to forward: the queue to primary %d stayed full for %v", target, c.forwardWait)
	case <-c.ctx.Done():
	}
}

// SendAck queues an ack of batch seq for primary to, and returns at once. The ack lane retries it until it is sent or
// to is no longer the primary. While the queue is full, a new ack is dropped, keeping the older acks the primary
// confirms first; a Warn marks when the drops start, and an Info how many there were once the queue has room again.
func (c *PrimaryConnector) SendAck(seq types.BatchSequence, to types.PartyID) {
	select {
	case c.acks.queue <- outgoing{target: to, msg: &protos.Ack{Shard: uint32(c.shardID), Seq: uint64(seq)}}:
		if dropped := c.ackDrops.Swap(0); dropped > 0 {
			c.logger.Infof("Queueing acks to primary %d again, after dropping %d", to, dropped)
		}
	default:
		if c.ackDrops.Add(1) == 1 {
			c.logger.Warnf("Dropping acks from batch %d on: %d acks to primary %d are waiting to be sent",
				seq, laneQueueSize, to)
		}
	}
}

// ConnectToNewPrimary sends to primaryID from now on, on both lanes. The queued messages for the old primary and the
// ones being sent are dropped, and the connections to the old primary are closed.
func (c *PrimaryConnector) ConnectToNewPrimary(primaryID types.PartyID) {
	c.acks.link.Retarget(primaryID)
	c.forwards.link.Retarget(primaryID)
}

// Stop stops sending, and returns once both lanes' goroutines have returned and disconnected their links.
func (c *PrimaryConnector) Stop() {
	c.cancel()
	c.running.Wait()
}

func (c *PrimaryConnector) run(l lane) {
	defer l.link.Disconnect()
	for {
		select {
		case <-c.ctx.Done():
			return
		case m := <-l.queue:
			l.link.Deliver(c.ctx, m.target, m.msg)
		}
	}
}

func openFwdRequestStream(ctx context.Context, conn *grpc.ClientConn) (grpc.ClientStream, error) {
	return protos.NewBatcherControlServiceClient(conn).FwdRequestStream(ctx)
}

func openAckStream(ctx context.Context, conn *grpc.ClientConn) (grpc.ClientStream, error) {
	return protos.NewBatcherControlServiceClient(conn).NotifyAck(ctx)
}
