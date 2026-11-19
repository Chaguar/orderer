/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	commonsync "github.com/Chaguar/orderer/common/synchronizer"
	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/config"
	"github.com/Chaguar/orderer/node/comm"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
)

//go:generate counterfeiter -o mocks/genesis_fetcher.go . GenesisFetcher
type GenesisFetcher interface {
	GenesisByEndpoints() (map[string]*common.Block, error)
	Close()
}

//go:generate counterfeiter -o mocks/genesis_fetcher_factory.go . GenesisFetcherFactory
type GenesisFetcherFactory interface {
	// CreateGenesisFetcher creates a new genesis fetcher.
	CreateGenesisFetcher(
		myPartyID types.PartyID,
		support AssemblerSupport,
		baseDialer *comm.PredicateDialer,
		clusterConfig config.Cluster,
		bccsp bccsp.BCCSP,
		logger *flogging.FabricLogger,
	) (GenesisFetcher, error)
}

type GenesisFetcherCreator struct{}

func (*GenesisFetcherCreator) CreateGenesisFetcher(
	myPartyID types.PartyID,
	support AssemblerSupport,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
	logger *flogging.FabricLogger,
) (GenesisFetcher, error) {
	return commonsync.NewBlockPuller(myPartyID, support, baseDialer, clusterConfig, bccsp, logger, config.ExtractAssemblerAddresses)
}
