/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package controlevent sends control events to consenters.
package controlevent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Chaguar/orderer/node/comm"
	"github.com/Chaguar/orderer/node/config"
	"github.com/Chaguar/orderer/node/consensus/state"
	protos "github.com/Chaguar/orderer/node/protos/comm"
	"google.golang.org/grpc"
)

type SenderCreator interface {
	CreateSender(TLSPrivateKey config.RawBytes, TLSCertificate config.RawBytes, consenterInfo config.ConsenterInfo) Sender
}

type SenderFactory struct{}

func (f *SenderFactory) CreateSender(TLSPrivateKey config.RawBytes, TLSCertificate config.RawBytes, consenterInfo config.ConsenterInfo) Sender {
	ctx, cancel := context.WithCancel(context.Background())
	return &stream{ctx: ctx, cancel: cancel, ConsenterInfo: consenterInfo, TLSPrivateKey: TLSPrivateKey, TLSCertificate: TLSCertificate}
}

//go:generate counterfeiter -o mocks/sender.go . Sender
type Sender interface {
	SendControlEvent(ce state.ControlEvent) error
	Close()
}

type stream struct {
	lock           sync.Mutex
	ctx            context.Context // cancelled by Close: unblocks a Send in progress and fails later sends
	cancel         context.CancelFunc
	conn           *grpc.ClientConn
	stream         protos.Consensus_NotifyEventClient
	ConsenterInfo  config.ConsenterInfo
	TLSPrivateKey  config.RawBytes
	TLSCertificate config.RawBytes
}

func (s *stream) createStream() error {
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

func (s *stream) SendControlEvent(ce state.ControlEvent) error {
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

func (s *stream) Close() {
	s.cancel()
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closeConn()
}

func (s *stream) closeConn() {
	s.stream = nil
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

func clientConfig(TLSPrivateKeyFile config.RawBytes, TLSCertificateFile config.RawBytes, TlsCACert []config.RawBytes) comm.ClientConfig {
	var tlsCAs [][]byte
	for _, cert := range TlsCACert {
		tlsCAs = append(tlsCAs, cert)
	}

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts:       comm.DefaultKeepaliveOptions,
		SecOpts: comm.SecureOptions{
			Key:               TLSPrivateKeyFile,
			Certificate:       TLSCertificateFile,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}
