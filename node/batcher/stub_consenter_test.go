/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/Chaguar/common/protoutil/identity"
	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/batcher"
	"github.com/Chaguar/orderer/node/comm"
	"github.com/Chaguar/orderer/node/config"
	"github.com/Chaguar/orderer/node/consensus/state"
	protos "github.com/Chaguar/orderer/node/protos/comm"
	"github.com/Chaguar/orderer/testutil"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
)

type stubConsenter struct {
	net                *comm.GRPCServer
	headerChan         chan *state.Header
	key                []byte
	certificate        []byte
	logger             *flogging.FabricLogger
	complaints         int                  // number of complaints received
	bafs               int                  // number of BAFs received
	receivedEvents     []state.ControlEvent // received control events
	receivedEventsLock sync.RWMutex

	ackConfigHandler func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error)
	// createReplicatorHook is called whenever a batcher creates its decision replicator, which a live
	// reconfiguration does while it builds the batcher for the new config.
	createReplicatorHook func()

	// lastDecision is the last decision number sent; each state update sends the next one.
	lastDecision     types.DecisionNum
	lastDecisionLock sync.Mutex
}

func NewStubConsenter(t *testing.T, partyID types.PartyID, n *node) *stubConsenter {
	sc := &stubConsenter{
		complaints:  0,
		bafs:        0,
		logger:      testutil.CreateLogger(t, int(partyID)),
		net:         n.GRPCServer,
		key:         n.TLSKey,
		certificate: n.TLSCert,
		headerChan:  make(chan *state.Header),
	}

	gRPCServer := n.GRPCServer.Server()
	protos.RegisterConsensusServer(gRPCServer, sc)

	go func() {
		if err := n.GRPCServer.Start(); err != nil {
			panic(err)
		}
	}()

	return sc
}

func (sc *stubConsenter) NotifyEvent(stream protos.Consensus_NotifyEventServer) error {
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		var ce state.ControlEvent
		ce.FromBytes(event.Payload)

		sc.receivedEventsLock.Lock()
		if ce.Complaint != nil {
			sc.complaints++
		} else {
			sc.bafs++
		}
		sc.receivedEvents = append(sc.receivedEvents, ce)
		sc.receivedEventsLock.Unlock()
	}
}

func (sc *stubConsenter) SubmitConfig(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	return nil, errors.New("not implemented")
}

func (sc *stubConsenter) Stop() {
	// Stop() of stub consenter does nothing
	// use NetStop() to stop the stub consenter network
}

func (sc *stubConsenter) StopNet() {
	sc.net.Stop()
}

func (sc *stubConsenter) Restart() {
	sc.StopNet()
	addr := sc.net.Address()
	server, err := comm.NewGRPCServer(addr, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: sc.certificate,
			Key:         sc.key,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to restart gRPC server: %v", err))
	}

	sc.net = server
	protos.RegisterConsensusServer(sc.net.Server(), sc)
	go func() {
		if err := sc.net.Start(); err != nil {
			panic(err)
		}
	}()
}

// RestartWithClientRootCAs restarts the stub consenter's gRPC server on the same address, requiring mutual TLS and
// trusting the provided client root CAs. It is used to rebuild an existing consenter's trust after a new party
// joins, so that the new party's batcher can establish connections and send BAFs to the consenter.
func (sc *stubConsenter) RestartWithClientRootCAs(clientRootCAs [][]byte) {
	sc.StopNet()
	addr := sc.net.Address()
	server, err := comm.NewGRPCServer(addr, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			RequireClientCert: true,
			ClientRootCAs:     clientRootCAs,
			ServerRootCAs:     clientRootCAs,
			Certificate:       sc.certificate,
			Key:               sc.key,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to restart gRPC server: %v", err))
	}

	sc.net = server
	protos.RegisterConsensusServer(sc.net.Server(), sc)
	go func() {
		if err := sc.net.Start(); err != nil {
			panic(err)
		}
	}()
}

// Returns the last received control event
func (sc *stubConsenter) LastControlEvent() *state.ControlEvent {
	sc.receivedEventsLock.RLock()
	defer sc.receivedEventsLock.RUnlock()
	return &sc.receivedEvents[len(sc.receivedEvents)-1]
}

func (sc *stubConsenter) BAFCount() int {
	sc.receivedEventsLock.RLock()
	defer sc.receivedEventsLock.RUnlock()
	return sc.bafs
}

func (sc *stubConsenter) ComplaintCount() int {
	sc.receivedEventsLock.RLock()
	defer sc.receivedEventsLock.RUnlock()
	return sc.complaints
}

// Returns the received complaints
func (sc *stubConsenter) Complaints() []*state.Complaint {
	sc.receivedEventsLock.RLock()
	defer sc.receivedEventsLock.RUnlock()
	var complaints []*state.Complaint
	for _, ce := range sc.receivedEvents {
		if ce.Complaint != nil {
			complaints = append(complaints, ce.Complaint)
		}
	}
	return complaints
}

// Returns the received batch attestation fragments
func (sc *stubConsenter) BAFs() []types.BatchAttestationFragment {
	sc.receivedEventsLock.RLock()
	defer sc.receivedEventsLock.RUnlock()
	var bafs []types.BatchAttestationFragment
	for _, ce := range sc.receivedEvents {
		if ce.BAF != nil {
			bafs = append(bafs, ce.BAF)
		}
	}
	return bafs
}

func (sc *stubConsenter) UpdateState(newState *state.State) {
	sc.lastDecisionLock.Lock()
	sc.lastDecision++
	header := &state.Header{
		Num:   sc.lastDecision,
		State: newState,
	}
	sc.lastDecisionLock.Unlock()
	sc.headerChan <- header
}

// SendDecision sends header as is, even if it is older than the last decision sent.
func (sc *stubConsenter) SendDecision(header *state.Header) {
	sc.advanceLastDecision(header.Num)
	sc.headerChan <- header
}

// advanceLastDecision raises lastDecision to num, if it is lower.
func (sc *stubConsenter) advanceLastDecision(num types.DecisionNum) {
	sc.lastDecisionLock.Lock()
	defer sc.lastDecisionLock.Unlock()
	sc.lastDecision = max(sc.lastDecision, num)
}

func (sc *stubConsenter) UpdateStateHeaderWithConfigBlock(decisionNum types.DecisionNum, availableCommonBlocks []*common.Block, newState *state.State) {
	header := &state.Header{
		Num:                          decisionNum,
		AvailableCommonBlocks:        availableCommonBlocks,
		State:                        newState,
		DecisionNumOfLastConfigBlock: decisionNum,
	}
	sc.lastDecisionLock.Lock()
	sc.lastDecision = decisionNum
	sc.lastDecisionLock.Unlock()
	sc.headerChan <- header
}

func (sc *stubConsenter) ReplicateDecision() <-chan *state.Header {
	return sc.headerChan
}

func (sc *stubConsenter) CreateDecisionConsensusReplicator(conf *config.BatcherNodeConfig, signer identity.SignerSerializer, logger *flogging.FabricLogger, num types.DecisionNum) batcher.DecisionReplicator {
	sc.receivedEventsLock.RLock()
	hook := sc.createReplicatorHook
	sc.receivedEventsLock.RUnlock()
	if hook != nil {
		hook()
	}
	sc.advanceLastDecision(num) // the batcher resumes from num, so the next state update sends a newer one
	return sc
}

// SetCreateReplicatorHook installs a hook that CreateDecisionConsensusReplicator calls before it returns.
func (sc *stubConsenter) SetCreateReplicatorHook(hook func()) {
	sc.receivedEventsLock.Lock()
	defer sc.receivedEventsLock.Unlock()
	sc.createReplicatorHook = hook
}

// SetAckConfigHandler installs a handler invoked by AckConfig to intercept the ConfigAck
// requests batchers send to the consenter. When no handler is set, AckConfig replies with an
// empty (successful) response.
func (sc *stubConsenter) SetAckConfigHandler(handler func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error)) {
	sc.receivedEventsLock.Lock()
	defer sc.receivedEventsLock.Unlock()
	sc.ackConfigHandler = handler
}

func (sc *stubConsenter) AckConfig(_ context.Context, req *protos.ConfigAck) (*protos.ConfigAckResponse, error) {
	sc.receivedEventsLock.RLock()
	handler := sc.ackConfigHandler
	sc.receivedEventsLock.RUnlock()

	if handler != nil {
		return handler(req)
	}
	return &protos.ConfigAckResponse{}, nil
}
