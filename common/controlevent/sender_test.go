/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package controlevent_test

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaguar/orderer/common/controlevent"
	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/comm"
	"github.com/Chaguar/orderer/node/comm/tlsgen"
	"github.com/Chaguar/orderer/node/config"
	"github.com/Chaguar/orderer/node/consensus/state"
	protos "github.com/Chaguar/orderer/node/protos/comm"
	"github.com/Chaguar/orderer/testutil"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Send 10 control events to a consenter that is down; expect every send to fail.
//  2. Start the consenter and send one control event; expect it to succeed.
//  3. Expect the consenter to hold exactly one connection from the sender.
func TestControlEventSenderClosesConnectionsWhileConsenterIsDown(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	endpoint := testutil.AllocateLocalhostAddress(t)

	sender := newControlEventSender(t, ca, endpoint)
	defer sender.Close()
	for range 10 {
		require.Error(t, sender.SendControlEvent(controlEvent()))
	}

	consenter := startCountingConsenter(t, ca, endpoint, false)
	require.NoError(t, sender.SendControlEvent(controlEvent()))
	require.Eventually(t, func() bool { return consenter.events.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return consenter.openConns.Load() != 1 }, 3*time.Second, 50*time.Millisecond)
}

// Scenario:
//  1. Start a consenter that ends every NotifyEvent stream after the first event.
//  2. Send control events until 10 sends have failed.
//  3. After each failed send, expect the consenter to hold no connection from the sender.
func TestControlEventSenderClosesConnectionWhenSendFails(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	endpoint := testutil.AllocateLocalhostAddress(t)
	consenter := startCountingConsenter(t, ca, endpoint, true)

	sender := newControlEventSender(t, ca, endpoint)
	defer sender.Close()
	deadline := time.Now().Add(30 * time.Second)
	for failures := 0; failures < 10; {
		require.True(t, time.Now().Before(deadline), "only %d sends failed", failures)
		if sender.SendControlEvent(controlEvent()) == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		failures++
		require.Eventually(t, func() bool { return consenter.openConns.Load() == 0 }, 10*time.Second, 10*time.Millisecond)
	}
}

// Scenario:
//  1. Send a control event to a consenter; expect it to succeed and the consenter to hold one connection.
//  2. Close the sender; expect the consenter to hold no connection.
//  3. Send a control event; expect it to fail and the consenter to still hold no connection.
func TestControlEventSenderCloseClosesConnection(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	endpoint := testutil.AllocateLocalhostAddress(t)
	consenter := startCountingConsenter(t, ca, endpoint, false)

	sender := newControlEventSender(t, ca, endpoint)
	require.NoError(t, sender.SendControlEvent(controlEvent()))
	require.Eventually(t, func() bool { return consenter.events.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), consenter.openConns.Load())

	sender.Close()
	require.Eventually(t, func() bool { return consenter.openConns.Load() == 0 }, 10*time.Second, 10*time.Millisecond)

	require.Error(t, sender.SendControlEvent(controlEvent()))
	require.Equal(t, int32(0), consenter.openConns.Load())
}

// newControlEventSender creates a control-event sender to the consenter at endpoint, trusting ca.
func newControlEventSender(t *testing.T, ca tlsgen.CA, endpoint string) controlevent.Sender {
	clientKeyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)
	consenterInfo := config.ConsenterInfo{PartyID: 1, Endpoint: endpoint, TLSCACerts: []config.RawBytes{ca.CertBytes()}}
	factory := &controlevent.SenderFactory{}
	return factory.CreateSender(clientKeyPair.Key, clientKeyPair.Cert, consenterInfo)
}

// controlEvent returns a control event carrying a complaint.
func controlEvent() state.ControlEvent {
	return state.ControlEvent{Complaint: &state.Complaint{ShardTerm: state.ShardTerm{Shard: types.ShardID(1), Term: 1}, Signer: 1}}
}

// countingConsenter is a consenter that counts the control events it receives and its open connections.
type countingConsenter struct {
	protos.UnimplementedConsensusServer
	endStreamAfterEvent bool
	events              atomic.Int32
	openConns           atomic.Int32
}

// startCountingConsenter starts a countingConsenter at endpoint, with a server certificate signed by ca.
func startCountingConsenter(t *testing.T, ca tlsgen.CA, endpoint string, endStreamAfterEvent bool) *countingConsenter {
	serverKeyPair, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)
	listener, err := net.Listen("tcp", endpoint)
	require.NoError(t, err)

	consenter := &countingConsenter{endStreamAfterEvent: endStreamAfterEvent}
	server, err := comm.NewGRPCServerFromListener(&countingListener{Listener: listener, openConns: &consenter.openConns}, comm.ServerConfig{
		SecOpts: comm.SecureOptions{UseTLS: true, Certificate: serverKeyPair.Cert, Key: serverKeyPair.Key},
	})
	require.NoError(t, err)
	protos.RegisterConsensusServer(server.Server(), consenter)
	go server.Start()
	t.Cleanup(server.Stop)
	return consenter
}

func (c *countingConsenter) NotifyEvent(stream protos.Consensus_NotifyEventServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
		c.events.Add(1)
		if c.endStreamAfterEvent {
			return nil
		}
	}
}

// countingListener counts the connections it accepted that are not closed yet.
type countingListener struct {
	net.Listener
	openConns *atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.openConns.Add(1)
	return &countedConn{Conn: conn, openConns: l.openConns}, nil
}

// countedConn decrements the open connection count of its listener once, when it is closed.
type countedConn struct {
	net.Conn
	openConns *atomic.Int32
	once      sync.Once
}

func (c *countedConn) Close() error {
	c.once.Do(func() { c.openConns.Add(-1) })
	return c.Conn.Close()
}
