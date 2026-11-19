/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package orderers

import (
	"github.com/Chaguar/orderer/common/types"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
)

type ConnectionSourcer interface {
	RandomEndpoint() (*Endpoint, error)
	ShuffledEndpoints() []*Endpoint
	Update2(Party2Endpoint)
}

type ConnectionSourceCreator interface {
	// CreateConnectionSource creates a ConnectionSourcer implementation.
	// In a peer, selfEndpoint == "";
	// In an orderer selfEndpoint carries the (delivery service) endpoint of the orderer.
	CreateConnectionSource(logger *flogging.FabricLogger, selfPartyID types.PartyID) ConnectionSourcer
}

type ConnectionSourceFactory struct{}

func (f *ConnectionSourceFactory) CreateConnectionSource(logger *flogging.FabricLogger, selfPartyID types.PartyID) ConnectionSourcer {
	return NewConnectionSource(logger, selfPartyID)
}
