/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Chaguar/orderer/common/types"
	node_config "github.com/Chaguar/orderer/node/config"
	protos "github.com/Chaguar/orderer/node/protos/comm"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"google.golang.org/grpc"
)

// PrimaryLink is the connection to the shard's primary over which a batcher forwards requests. One goroutine at a
// time calls Deliver, so sends never run concurrently and only that goroutine dials. Retarget and CurrentTarget may be
// called from any goroutine.
type PrimaryLink struct {
	logger   *flogging.FabricLogger
	config   *node_config.BatcherNodeConfig
	batchers map[types.PartyID]*EndpointAndCerts
	// sendTimeout bounds a send on the stream, and the connector's wait for room in its queue.
	sendTimeout      time.Duration
	minRetryInterval time.Duration
	maxRetryDelay    time.Duration

	// lock guards the target and the connection. It is never held across a dial or a send.
	lock        sync.Mutex
	target      types.PartyID
	moved       chan struct{}      // closed when the target changes; each target gets a new one
	connectedTo types.PartyID      // the target of the connection, also while it is being opened
	cancelConn  context.CancelFunc // ends the connection, and opening it
	conn        *grpc.ClientConn
	stream      protos.BatcherControlService_FwdRequestStreamClient
}

// NewPrimaryLink creates a link to target. It connects at the first Deliver.
func NewPrimaryLink(logger *flogging.FabricLogger, config *node_config.BatcherNodeConfig, batchers map[types.PartyID]*EndpointAndCerts, target types.PartyID, sendTimeout, minRetryInterval, maxRetryDelay time.Duration) *PrimaryLink {
	return &PrimaryLink{
		logger:           logger,
		config:           config,
		batchers:         batchers,
		sendTimeout:      sendTimeout,
		minRetryInterval: minRetryInterval,
		maxRetryDelay:    maxRetryDelay,
		target:           target,
		moved:            make(chan struct{}),
	}
}

// Deliver sends req to target, connecting first if needed. A failed send closes the connection, so the next attempt
// uses a new one, and is retried after a backoff. Deliver gives up at once when target stops being the target (or
// already isn't), when it is this batcher, or when ctx ends.
func (l *PrimaryLink) Deliver(ctx context.Context, target types.PartyID, req []byte) {
	moved, current := l.watch(target)
	if !current || target == l.config.PartyId {
		return
	}
	delay := l.minRetryInterval
	for ctx.Err() == nil && !closed(moved) {
		stream, cancel, err := l.streamTo(ctx, target)
		if err != nil {
			if closed(moved) {
				return
			}
			l.logger.Warnf("Failed connecting to primary %d, retrying: %v", target, err)
			delay = waitBackoff(ctx, moved, delay, l.maxRetryDelay)
			continue
		}
		if err := l.send(stream, cancel, req); err != nil {
			if closed(moved) {
				return
			}
			l.logger.Warnf("Failed sending a request to primary %d, reconnecting: %v", target, err)
			l.Disconnect()
			delay = waitBackoff(ctx, moved, delay, l.maxRetryDelay)
			continue
		}
		return
	}
}

// Retarget makes primary the target. It closes the connection to the old one, or stops opening it, which also ends a
// send in progress, and wakes a Deliver that waits to retry.
func (l *PrimaryLink) Retarget(primary types.PartyID) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.target != primary {
		close(l.moved)
		l.target, l.moved = primary, make(chan struct{})
	}
	if l.connectedTo != primary {
		l.disconnectLocked()
	}
}

// Disconnect closes the connection, or stops opening it. The next Deliver connects again.
func (l *PrimaryLink) Disconnect() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.disconnectLocked()
}

// CurrentTarget returns the primary the link sends to.
func (l *PrimaryLink) CurrentTarget() types.PartyID {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.target
}

// watch returns the channel that is closed when target stops being the target, and whether target is the target.
func (l *PrimaryLink) watch(target types.PartyID) (chan struct{}, bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.moved, l.target == target
}

// streamTo returns the stream to target and the function that ends its connection, connecting first if there is
// none. A retarget before or while connecting stops the connecting.
func (l *PrimaryLink) streamTo(ctx context.Context, target types.PartyID) (protos.BatcherControlService_FwdRequestStreamClient, context.CancelFunc, error) {
	l.lock.Lock()
	if l.target != target { // retargeted since Deliver checked: don't open a connection to the old target
		l.lock.Unlock()
		return nil, nil, context.Canceled
	}
	if l.stream != nil && l.connectedTo == target {
		stream, cancel := l.stream, l.cancelConn
		l.lock.Unlock()
		return stream, cancel, nil
	}
	l.disconnectLocked()
	connCtx, cancel := context.WithCancel(ctx)
	l.connectedTo, l.cancelConn = target, cancel
	l.lock.Unlock()

	conn, stream, err := l.dial(connCtx, target)

	l.lock.Lock()
	defer l.lock.Unlock()
	if connCtx.Err() != nil { // a retarget ended the connecting and ran disconnectLocked, or ctx ended (Stop: run disconnects the link)
		if conn != nil {
			conn.Close()
		}
		return nil, nil, connCtx.Err()
	}
	if err != nil {
		l.disconnectLocked()
		return nil, nil, err
	}
	l.conn, l.stream = conn, stream
	return stream, cancel, nil
}

func (l *PrimaryLink) dial(ctx context.Context, target types.PartyID) (*grpc.ClientConn, protos.BatcherControlService_FwdRequestStreamClient, error) {
	endpoint := l.batchers[target]
	if endpoint == nil || endpoint.Endpoint == "" {
		l.logger.Panicf("Could not find primaryID %d of shard %d within %v", target, l.config.ShardId, l.batchers)
	}

	cc := clientConfig(l.config.TLSPrivateKeyFile, l.config.TLSCertificateFile, endpoint.TLSCACerts)
	conn, err := cc.DialContext(ctx, endpoint.Endpoint)
	if err != nil {
		return nil, nil, err
	}
	stream, err := protos.NewBatcherControlServiceClient(conn).FwdRequestStream(ctx)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	l.logger.Infof("Forwarding requests to primary %d: stream opened", target)
	return conn, stream, nil
}

// send sends req on stream. A send that takes longer than the send timeout fails, because cancel then ends the
// connection, and its error says so; so does one whose connection Retarget closes. If the watchdog fires just as the send
// returns, the connection ends anyway and the next send reconnects, which is harmless.
func (l *PrimaryLink) send(stream protos.BatcherControlService_FwdRequestStreamClient, cancel context.CancelFunc, req []byte) error {
	watchdog := time.AfterFunc(l.sendTimeout, cancel)
	err := stream.Send(&protos.FwdRequest{Request: req})
	if !watchdog.Stop() && err != nil {
		return fmt.Errorf("no progress within %v: %w", l.sendTimeout, err)
	}
	return err
}

// disconnectLocked ends the connection or the opening of it, which also ends a send in progress on it.
func (l *PrimaryLink) disconnectLocked() {
	if l.cancelConn != nil {
		l.cancelConn()
	}
	if l.conn != nil {
		l.conn.Close()
	}
	l.connectedTo, l.cancelConn, l.conn, l.stream = 0, nil, nil, nil
}

// waitBackoff waits for delay, or less if ctx ends or the target moves, and returns the delay before the next attempt.
func waitBackoff(ctx context.Context, moved chan struct{}, delay, maxDelay time.Duration) time.Duration {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-moved:
	case <-timer.C:
	}
	return min(2*delay, maxDelay)
}

func closed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
