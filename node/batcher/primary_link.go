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
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// LinkTiming is how a lane's link sends. SendTimeout cuts one send that makes no progress, after which the link
// reconnects. A failed connect or send is retried after MinRetryInterval, doubling up to MaxRetryDelay. A message is
// retried until it is sent, its primary is no longer the target, or the connector stops.
type LinkTiming struct {
	SendTimeout      time.Duration
	MinRetryInterval time.Duration
	MaxRetryDelay    time.Duration
}

// PrimaryLink is a connection to the shard's primary with one stream to it, over which a batcher forwards requests or
// sends acks. One goroutine at a time calls Deliver, so sends never run concurrently and only that goroutine dials.
// Retarget and CurrentTarget may be called from any goroutine.
type PrimaryLink struct {
	logger   *flogging.FabricLogger
	config   *node_config.BatcherNodeConfig
	batchers map[types.PartyID]*EndpointAndCerts
	name     string                                                                      // what the link sends, for its logs
	open     func(ctx context.Context, conn *grpc.ClientConn) (grpc.ClientStream, error) // opens the stream on a connection
	timing   LinkTiming

	// lock guards the target and the connection. It is never held across a dial or a send.
	lock        sync.Mutex
	target      types.PartyID
	moved       chan struct{}      // closed when the target changes; each target gets a new one
	connectedTo types.PartyID      // the target of the connection, also while it is being opened
	cancelConn  context.CancelFunc // ends the connection, and opening it
	conn        *grpc.ClientConn
	stream      grpc.ClientStream
}

// NewPrimaryLink creates a link to target, which opens streams with open. It connects at the first Deliver.
func NewPrimaryLink(
	logger *flogging.FabricLogger, config *node_config.BatcherNodeConfig, batchers map[types.PartyID]*EndpointAndCerts,
	name string, open func(ctx context.Context, conn *grpc.ClientConn) (grpc.ClientStream, error),
	target types.PartyID, timing LinkTiming,
) *PrimaryLink {
	return &PrimaryLink{
		logger:   logger,
		config:   config,
		batchers: batchers,
		name:     name,
		open:     open,
		timing:   timing,
		target:   target,
		moved:    make(chan struct{}),
	}
}

// Deliver sends msg to target, connecting first if needed. A failed send closes the connection, so the next attempt
// uses a new one, and is retried after a backoff. Deliver gives up at once when target stops being the target (or
// already isn't), when it is this batcher, or when ctx ends. A message dropped because its target is no longer the
// primary is logged at Debug.
func (l *PrimaryLink) Deliver(ctx context.Context, target types.PartyID, msg proto.Message) {
	moved, current := l.watch(target)
	if target == l.config.PartyId {
		return
	}
	delay := l.timing.MinRetryInterval
	for current && ctx.Err() == nil && !closed(moved) {
		stream, cancel, err := l.streamTo(ctx, target)
		if err != nil {
			if closed(moved) {
				break
			}
			l.logger.Warnf("Failed connecting to primary %d to send %s, retrying: %v", target, l.name, err)
			delay = waitBackoff(ctx, moved, delay, l.timing.MaxRetryDelay)
			continue
		}
		if err := l.send(stream, cancel, msg); err != nil {
			if closed(moved) {
				break
			}
			l.logger.Warnf("Failed sending %s to primary %d, reconnecting: %v", l.name, target, err)
			l.Disconnect()
			delay = waitBackoff(ctx, moved, delay, l.timing.MaxRetryDelay)
			continue
		}
		return
	}
	if ctx.Err() == nil { // target isn't the primary (any more)
		l.logger.Debugf("Dropping %s queued for primary %d: the primary is now %d", l.name, target, l.CurrentTarget())
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
func (l *PrimaryLink) streamTo(ctx context.Context, target types.PartyID) (grpc.ClientStream, context.CancelFunc, error) {
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

func (l *PrimaryLink) dial(ctx context.Context, target types.PartyID) (*grpc.ClientConn, grpc.ClientStream, error) {
	endpoint := l.batchers[target]
	if endpoint == nil || endpoint.Endpoint == "" {
		l.logger.Panicf("Could not find primaryID %d of shard %d within %v", target, l.config.ShardId, l.batchers)
	}

	cc := clientConfig(l.config.TLSPrivateKeyFile, l.config.TLSCertificateFile, endpoint.TLSCACerts)
	conn, err := cc.DialContext(ctx, endpoint.Endpoint)
	if err != nil {
		return nil, nil, err
	}
	stream, err := l.open(ctx, conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	l.logger.Infof("Sending %s to primary %d: stream opened", l.name, target)
	return conn, stream, nil
}

// send sends msg on stream. A send that takes longer than the send timeout fails, because cancel then ends the
// connection, and its error says so; so does one whose connection Retarget closes. If the watchdog fires just as the send
// returns, the connection ends anyway and the next send reconnects, which is harmless.
func (l *PrimaryLink) send(stream grpc.ClientStream, cancel context.CancelFunc, msg proto.Message) error {
	watchdog := time.AfterFunc(l.timing.SendTimeout, cancel)
	err := stream.SendMsg(msg)
	if !watchdog.Stop() && err != nil {
		return fmt.Errorf("no progress within %v: %w", l.timing.SendTimeout, err)
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
