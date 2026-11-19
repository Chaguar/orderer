/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"google.golang.org/grpc"
)

//go:generate counterfeiter -o mocks/consenter_control_event_sender_creator.go . ConsenterControlEventSenderCreator
type ConsenterControlEventSenderCreator interface {
	CreateConsenterControlEventSender(TLSPrivateKeyFile config.RawBytes, TLSCertificateFile config.RawBytes, consenterInfo config.ConsenterInfo) ConsenterControlEventSender
}

type ConsenterControlEventSenderFactory struct{}

func (f *ConsenterControlEventSenderFactory) CreateConsenterControlEventSender(TLSPrivateKey config.RawBytes, TLSCertificate config.RawBytes, consenterInfo config.ConsenterInfo) ConsenterControlEventSender {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsenterControlEventStream{ctx: ctx, cancel: cancel, ConsenterInfo: consenterInfo, TLSPrivateKey: TLSPrivateKey, TLSCertificate: TLSCertificate}
}

//go:generate counterfeiter -o mocks/consenter_control_event_sender.go . ConsenterControlEventSender
type ConsenterControlEventSender interface {
	SendControlEvent(ce state.ControlEvent) error
	Close()
}

type ConsenterControlEventStream struct {
	lock           sync.Mutex
	ctx            context.Context // cancelled by Close: unblocks a Send in progress and fails later sends
	cancel         context.CancelFunc
	conn           *grpc.ClientConn
	stream         protos.Consensus_NotifyEventClient
	ConsenterInfo  config.ConsenterInfo
	TLSPrivateKey  config.RawBytes
	TLSCertificate config.RawBytes
}

func (s *ConsenterControlEventStream) createStream() error {
	cc := clientConfig(s.TLSPrivateKey, s.TLSCertificate, s.ConsenterInfo.TLSCACerts)
	conn, err := cc.Dial(s.ConsenterInfo.Endpoint)
	if err != nil {
		return fmt.Errorf("failed connecting to %s: %v", s.ConsenterInfo.Endpoint, err)
	}
	s.conn = conn
	cl := protos.NewConsensusClient(conn)
	s.stream, err = cl.NotifyEvent(s.ctx)
	if err != nil {
		s.closeConn()
		return fmt.Errorf("failed creating notify event client: %v", err)
	}
	return nil
}

func (s *ConsenterControlEventStream) SendControlEvent(ce state.ControlEvent) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.ctx.Err() != nil {
		return fmt.Errorf("failed sending control event to consenter %d (endpoint: %s): sender is closed", s.ConsenterInfo.PartyID, s.ConsenterInfo.Endpoint)
	}
	if s.stream == nil {
		if err := s.createStream(); err != nil { // TODO maybe retry
			return fmt.Errorf("failed creating stream to consenter %d (endpoint: %s): %v", s.ConsenterInfo.PartyID, s.ConsenterInfo.Endpoint, err)
		}
	}
	err := s.stream.Send(&protos.Event{
		Payload: ce.Bytes(),
	})
	if err != nil { // TODO maybe retry
		s.closeConn()
		return fmt.Errorf("failed sending control event to consenter %d (endpoint: %s); error: %v", s.ConsenterInfo.PartyID, s.ConsenterInfo.Endpoint, err)
	}
	return nil
}

func (s *ConsenterControlEventStream) Close() {
	s.cancel()
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closeConn()
}

func (s *ConsenterControlEventStream) closeConn() {
	s.stream = nil
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}
