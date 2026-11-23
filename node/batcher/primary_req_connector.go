/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"time"

	"github.com/Chaguar/orderer/common/types"
	node_config "github.com/Chaguar/orderer/node/config"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
)

// forwardQueueSize bounds the requests waiting to be forwarded. While the queue is full, SendReq waits, holding back
// the pending store's goroutine that forwards a strike.
const forwardQueueSize = 1000

// PrimaryReqConnector forwards requests whose first strike fired to the shard's primary. Callers put requests in a
// bounded queue, each tagged with the primary it is for, and one goroutine takes them out and delivers them over the
// link. A request whose primary is no longer the target is dropped, because the pool starts its strike over in the
// new term.
type PrimaryReqConnector struct {
	link  *PrimaryLink
	queue chan forwardRequest

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// forwardRequest is a request to forward, and the primary it is for.
type forwardRequest struct {
	target types.PartyID
	req    []byte
}

func CreatePrimaryReqConnector(primaryID types.PartyID, logger *flogging.FabricLogger, config *node_config.BatcherNodeConfig, batchers map[types.PartyID]*EndpointAndCerts, ctx context.Context, timeout, minRetryInterval, maxRetryDelay time.Duration) *PrimaryReqConnector {
	connector := &PrimaryReqConnector{
		link:  NewPrimaryLink(logger, config, batchers, primaryID, timeout, minRetryInterval, maxRetryDelay),
		queue: make(chan forwardRequest, forwardQueueSize),
		done:  make(chan struct{}),
	}
	connector.ctx, connector.cancel = context.WithCancel(ctx)
	go connector.run()
	return connector
}

// SendReq queues req for the primary. While the queue is full it waits, at most the send timeout: the request is then
// dropped, and it strikes again. It returns at once when this batcher is the primary or the connector is stopped. A
// waiting call doesn't watch the term: after a term change it may still queue its request, which the new primary
// either rejects or batches.
func (p *PrimaryReqConnector) SendReq(req []byte) {
	target := p.link.CurrentTarget()
	if target == p.link.config.PartyId {
		return
	}
	request := forwardRequest{target: target, req: req}
	select {
	case p.queue <- request:
		return
	default:
	}
	timer := time.NewTimer(p.link.sendTimeout)
	defer timer.Stop()
	select {
	case p.queue <- request:
	case <-timer.C:
		p.link.logger.Warnf("Dropping a request to forward: the queue to primary %d stayed full for %v", target, p.link.sendTimeout)
	case <-p.ctx.Done():
	}
}

// ConnectToNewPrimary forwards to primaryID from now on. The queued requests for the old primary and the one being sent
// are dropped, and the connection to the old primary is closed.
func (p *PrimaryReqConnector) ConnectToNewPrimary(primaryID types.PartyID) {
	p.link.Retarget(primaryID)
}

// Stop stops forwarding, and returns once the forwarding goroutine has returned and disconnected the link.
func (p *PrimaryReqConnector) Stop() {
	p.cancel()
	<-p.done
}

func (p *PrimaryReqConnector) run() {
	defer close(p.done)
	defer p.link.Disconnect()
	for {
		select {
		case <-p.ctx.Done():
			return
		case request := <-p.queue:
			p.link.Deliver(p.ctx, request.target, request.req)
		}
	}
}
