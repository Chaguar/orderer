/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"

	"github.com/Chaguar/common/protoutil"
	"github.com/Chaguar/orderer/common/deliverclient/blocksprovider"
	"github.com/Chaguar/orderer/common/deliverclient/orderers"
	commonsync "github.com/Chaguar/orderer/common/synchronizer"
	arma_types "github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/common/utils"
	"github.com/Chaguar/orderer/config"
	"github.com/Chaguar/orderer/node/comm"
	smartbft_types "github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
)

type BFTSynchronizer struct {
	lastReconfig        smartbft_types.Reconfig
	selfID              uint64
	LatestConfig        func() (smartbft_types.Configuration, []uint64)
	BlockToDecision     func(*common.Block) (*smartbft_types.Decision, error)
	OnCommit            func(*common.Block) smartbft_types.Reconfig
	Support             ConsenterSupport
	CryptoProvider      bccsp.BCCSP
	ClusterDialer       *comm.PredicateDialer
	LocalConfigCluster  config.Cluster
	BlockPullerFactory  HeightDetectorFactory
	VerifierFactory     commonsync.VerifierFactory
	BFTDelivererFactory commonsync.BFTDelivererFactory
	Logger              *flogging.FabricLogger
	JoinConfigBlock     *common.Block // JoinConfigBlock is the config block that was used to join the cluster.

	mutex    sync.Mutex
	syncBuff *commonsync.SyncBuffer
	stopped  bool // set by Stop and never cleared
}

func (s *BFTSynchronizer) Sync() smartbft_types.SyncResponse {
	s.Logger.Debugf("BFT Sync initiated, party: %d", s.selfID)

	// After sync has ended, successfully or not, reset the state of the last reconfig.
	defer func() {
		s.lastReconfig = smartbft_types.Reconfig{}
	}()

	decision, err := s.synchronize()
	if err != nil {
		s.Logger.Warnf("Could not synchronize with remote orderers due to %s, returning state from local ledger, party: %d", err, s.selfID)

		h := s.Support.Height()
		s.Logger.Warnf("Could not synchronize with remote orderers, returning state from local ledger, h: %d, party: %d", h, s.selfID)
		if h == 0 && s.isStopped() {
			// Stopped before the genesis block was fetched; there is no state to return.
			config, nodes := s.LatestConfig()
			return smartbft_types.SyncResponse{Reconfig: smartbft_types.ReconfigSync{CurrentNodes: nodes, CurrentConfig: config}}
		}
		if h == 0 {
			s.Logger.Panicf("Cannot join the cluster: failed to fetch genesis block and fetch state from local ledger: party: %d", s.selfID)
		}

		block := s.Support.Block(h - 1)
		config, nodes := s.LatestConfig()
		latestDec, err := s.BlockToDecision(block)
		if err != nil {
			s.Logger.Warnf("Could not return state from local ledger, h: %d, party: %d; err: %s", h, s.selfID, err)
			return smartbft_types.SyncResponse{
				Latest: smartbft_types.Decision{},
				Reconfig: smartbft_types.ReconfigSync{
					InReplicatedDecisions: false, // If we read from ledger we do not need to reconfigure.
					CurrentNodes:          nodes,
					CurrentConfig:         config,
				},
			}
		}

		return smartbft_types.SyncResponse{
			Latest: *latestDec,
			Reconfig: smartbft_types.ReconfigSync{
				InReplicatedDecisions: false, // If we read from ledger we do not need to reconfigure.
				CurrentNodes:          nodes,
				CurrentConfig:         config,
			},
		}
	}

	s.Logger.Debugf("reconfig: %+v", s.lastReconfig)
	return smartbft_types.SyncResponse{
		Latest: *decision,
		Reconfig: smartbft_types.ReconfigSync{
			InReplicatedDecisions: s.lastReconfig.InLatestDecision,
			CurrentConfig:         s.lastReconfig.CurrentConfig,
			CurrentNodes:          s.lastReconfig.CurrentNodes,
		},
	}
}

// Stop stops the current sync, if any, and every later one.
func (s *BFTSynchronizer) Stop() {
	s.Logger.Infof("Stopping BFT Synchronizer, party: %d", s.selfID)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.stopped = true
	if s.syncBuff == nil {
		return
	}

	s.syncBuff.Stop() // This will stop the BFT deliverer and any go-routines we started in Sync()
}

// Buffer return the internal SyncBuffer for testability.
func (s *BFTSynchronizer) Buffer() *commonsync.SyncBuffer {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.syncBuff
}

func (s *BFTSynchronizer) synchronize() (*smartbft_types.Decision, error) {
	if s.isStopped() {
		return nil, errors.New("synchronizer stopped")
	}

	// === We probe all the endpoints and establish a target height, as well as detect the self endpoint.
	// TODO make the target height detection stoppable
	targetHeight, _, err := s.detectTargetHeight()
	if err != nil {
		return nil, fmt.Errorf("cannot get detect target height: %w", err)
	}

	startHeight := s.Support.Height()
	if startHeight >= targetHeight {
		return nil, fmt.Errorf("already at target height of %d", targetHeight)
	}

	if startHeight == 0 {
		genesisBlock, err := s.fetchGenesisBlock()
		if err != nil {
			s.Logger.Panicf("Cannot join the cluster: failed to fetch genesis block: %s", err)
		}
		s.Support.WriteConfigBlock(genesisBlock)
		startHeight = s.Support.Height()

		s.Logger.Infof("Fetched and wrote genesis block, new height: %d, party: %d", startHeight, s.selfID)
	}

	// === Create a buffer to accept the blocks delivered from the BFTDeliverer.
	capacityBlocks := uint(100)
	if absMax := s.Support.SharedConfig().BatchSize().AbsoluteMaxBytes; absMax > 0 {
		capacityBlocks = max(uint(s.LocalConfigCluster.ReplicationBufferSize)/uint(absMax), 100)
	}
	s.mutex.Lock()
	if s.stopped { // Stop was called while the target height was detected
		s.mutex.Unlock()
		return nil, errors.New("synchronizer stopped")
	}
	s.syncBuff = commonsync.NewSyncBuffer(capacityBlocks)
	s.mutex.Unlock()

	// === Create the BFT block deliverer and start a go-routine that fetches block and inserts them into the syncBuffer.
	bftDeliverer, err := s.createBFTDeliverer(startHeight, arma_types.PartyID(s.selfID))
	if err != nil {
		return nil, fmt.Errorf("cannot create BFT block deliverer: %w", err)
	}

	go bftDeliverer.DeliverBlocks()
	defer bftDeliverer.Stop()

	// === Loop on sync-buffer and pull blocks, writing them to the ledger, returning the last block pulled.
	lastPulledBlock, err := s.getBlocksFromSyncBuffer(startHeight, targetHeight)
	if err != nil {
		return nil, fmt.Errorf("failed to get any blocks from SyncBuffer: %w", err)
	}

	decision, err := s.BlockToDecision(lastPulledBlock)
	if err != nil {
		return nil, fmt.Errorf("cannot get decision from block: %w", err)
	}

	// TODO add a short string representation of the decision for better logging.
	s.Logger.Infof("Returning decision from block [%d]", lastPulledBlock.GetHeader().GetNumber())
	return decision, err
}

// detectTargetHeight probes remote endpoints and detects what is the target height this node needs to reach.
// TODO make this method stoppable, currently it can take a long time if remote endpoints are not responsive, and we have no way to interrupt it.
func (s *BFTSynchronizer) detectTargetHeight() (uint64, string, error) {
	blockPuller, err := s.BlockPullerFactory.CreateHeightDetector(arma_types.PartyID(s.selfID), s.Support, s.ClusterDialer, s.LocalConfigCluster, s.CryptoProvider, s.Logger)
	if err != nil {
		return 0, "", fmt.Errorf("cannot create HeightDetector: %w", err)
	}
	defer blockPuller.Close()

	heightByEndpoint, myEndpoint, err := blockPuller.HeightsByEndpoints()
	if err != nil {
		return 0, "", fmt.Errorf("cannot get HeightsByEndpoints: %w", err)
	}

	s.Logger.Infof("HeightsByEndpoints: %+v, my endpoint: %s", heightByEndpoint, myEndpoint)

	delete(heightByEndpoint, myEndpoint)
	var heights []uint64
	for _, value := range heightByEndpoint {
		heights = append(heights, value)
	}

	if len(heights) == 0 {
		return 0, "", errors.New("no cluster members to synchronize with")
	}

	targetHeight := s.computeTargetHeight(heights)
	s.Logger.Infof("Detected target height: %d", targetHeight)
	return targetHeight, myEndpoint, nil
}

// computeTargetHeight compute the target height to synchronize to.
//
// heights: a slice containing the heights of accessible peers, length must be >0.
// clusterSize: the cluster size, must be >0.
//
// We should emit a height that is equal or smaller than a height given by a correct node.
func (s *BFTSynchronizer) computeTargetHeight(heights []uint64) uint64 {
	sort.Slice(heights, func(i, j int) bool { return heights[i] > heights[j] }) // Descending
	clusterSize := len(s.Support.SharedConfig().Consenters())
	f := uint64(clusterSize-1) / 3 // The number of tolerated byzantine faults
	lenH := uint64(len(heights))

	s.Logger.Debugf("Cluster size: %d, F: %d, Heights: %v", clusterSize, f, heights)

	if lenH < f+1 {
		s.Logger.Debugf("Returning %d", heights[int(lenH)-1])
		return heights[int(lenH)-1]
	}
	s.Logger.Debugf("Returning %d", heights[f])
	return heights[f]
}

// createBFTDeliverer creates and initializes the BFT block deliverer.
func (s *BFTSynchronizer) createBFTDeliverer(startHeight uint64, myParty arma_types.PartyID) (commonsync.BFTBlockDeliverer, error) {
	lastDecision := s.Support.Block(startHeight - 1)
	ledgerLastConfigBlock, err := s.Support.LastConfigBlock(lastDecision)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve last config block: %w", err)
	}
	if s.JoinConfigBlock == nil {
		return nil, errors.New("join config block is nil")
	}

	s.Logger.Infof("Creating BFTDeliverer, last decision in ledger: %d, last config block in ledger: %d, join config block: %d", lastDecision.Header.Number, ledgerLastConfigBlock.Header.Number, s.JoinConfigBlock.Header.Number)

	var delivererInitializationBlock *common.Block
	if s.JoinConfigBlock.Header.Number > ledgerLastConfigBlock.Header.Number {
		delivererInitializationBlock = s.JoinConfigBlock
	} else {
		s.Logger.Warnf("Join config block is not more recent than last config block in consensusledger")
		delivererInitializationBlock = ledgerLastConfigBlock
	}

	blockOps := &utils.CommonConfigBlockOperations{}
	currentConfigEnv, err := blockOps.ConfigFromBlock(delivererInitializationBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve envelope from config block: %w", err)
	}

	updatableVerifier, err := s.VerifierFactory.CreateBlockVerifier(ledgerLastConfigBlock, lastDecision, s.CryptoProvider, s.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create BlockVerificationAssistant: %w", err)
	}

	clientConfig := s.ClusterDialer.Config // The cluster and block puller use slightly different options
	clientConfig.AsyncConnect = false
	clientConfig.SecOpts.VerifyCertificate = nil

	tlsCertHash, err := protoutil.HashTLSCertificate(clientConfig.SecOpts.Certificate)
	if err != nil {
		return nil, fmt.Errorf("failed hashing the TLS certificate: %w", err)
	}

	// The maximal amount of time to wait before retrying to connect.
	maxRetryInterval := s.LocalConfigCluster.ReplicationMaxRetryInterval
	// The minimal amount of time to wait before retrying. The retry interval doubles after every unsuccessful attempt.
	minRetryInterval := s.LocalConfigCluster.ReplicationMinRetryInterval
	// The maximal duration of a Sync. After this time Sync returns with whatever it had pulled until that point.
	maxRetryDuration := s.LocalConfigCluster.ReplicationMaxRetryDuration
	// If a remote orderer does not deliver blocks for this amount of time, even though it can do so, it is replaced as the block deliverer.
	blockCensorshipTimeOut := maxRetryDuration / 3

	bftDeliverer := s.BFTDelivererFactory.CreateBFTDeliverer(
		s.Support.ChannelID(),
		s.syncBuff,
		commonsync.NewLedgerInfoAdapter(s.Support),
		updatableVerifier,
		blocksprovider.DialerAdapter{ClientConfig: clientConfig},
		&orderers.ConnectionSourceFactory{}, // no overrides in the orderer
		s.CryptoProvider,
		make(chan struct{}),
		s.Support,
		blocksprovider.DeliverAdapter{},
		&blocksprovider.BFTCensorshipMonitorFactory{},
		&commonsync.EndpointsExtractor{Extract: config.ExtractConsenterAddresses},
		flogging.MustGetLogger("orderer.blocksprovider").With("channel", s.Support.ChannelID()),
		minRetryInterval,
		maxRetryInterval,
		blockCensorshipTimeOut,
		maxRetryDuration,
		func() (stopRetries bool) {
			s.syncBuff.Stop()
			return true // In the orderer we must limit the time we try to do Synch()
		},
		tlsCertHash,
	)

	s.Logger.Infof("Created a BFTDeliverer on channel: %s", s.Support.ChannelID())
	bftDeliverer.Initialize(currentConfigEnv.GetConfig(), myParty)

	return bftDeliverer, nil
}

// fetchGenesisBlock fetches the genesis block from remote orderers.
// TODO make this method stoppable, currently it can take a long time if remote endpoints are not responsive, and we have no way to interrupt it.
func (s *BFTSynchronizer) fetchGenesisBlock() (*common.Block, error) {
	s.Logger.Infof("Fetching genesis block, party: %d", s.selfID)
	blockPuller, err := s.BlockPullerFactory.CreateHeightDetector(arma_types.PartyID(s.selfID), s.Support, s.ClusterDialer, s.LocalConfigCluster, s.CryptoProvider, s.Logger)
	if err != nil {
		return nil, fmt.Errorf("cannot create HeightDetector: %w", err)
	}
	defer blockPuller.Close()

	genesisByEndpoint, err := blockPuller.GenesisByEndpoints()
	if err != nil {
		return nil, fmt.Errorf("cannot get GenesisByEndpoints: %w", err)
	}

	s.Logger.Infof("Received genesis blocks from %d endpoints: %v", len(genesisByEndpoint), slices.Collect(maps.Keys(genesisByEndpoint)))

	clusterSize := len(s.Support.SharedConfig().Consenters())
	return commonsync.SelectGenesisBlock(s.Logger, genesisByEndpoint, clusterSize)
}

func (s *BFTSynchronizer) getBlocksFromSyncBuffer(startHeight, targetHeight uint64) (*common.Block, error) {
	targetSeq := targetHeight - 1
	seq := startHeight
	var blocksFetched int
	s.Logger.Debugf("Will fetch sequences [%d-%d]", seq, targetSeq)

	var lastPulledBlock *common.Block
	// PullBlock may still return buffered blocks after Stop, so check the flag too.
	for seq <= targetSeq && !s.isStopped() {
		block := s.syncBuff.PullBlock(seq)
		if block == nil {
			s.Logger.Debugf("Failed to fetch block [%d] from cluster", seq)
			break
		}
		if protoutil.IsConfigBlock(block) {
			s.Support.WriteConfigBlock(block)
			s.Logger.Debugf("Fetched and committed config block [%d] from cluster", seq)
		} else {
			s.Support.WriteBlockSync(block)
			s.Logger.Debugf("Fetched and committed block [%d] from cluster", seq)
		}
		lastPulledBlock = block

		prevInLatestDecision := s.lastReconfig.InLatestDecision
		s.lastReconfig = s.OnCommit(lastPulledBlock)
		s.lastReconfig.InLatestDecision = s.lastReconfig.InLatestDecision || prevInLatestDecision
		s.Logger.Debugf("Last reconfig %+v", s.lastReconfig)
		seq++
		blocksFetched++
	}

	s.syncBuff.Stop()

	if lastPulledBlock == nil {
		return nil, fmt.Errorf("failed pulling block %d", seq)
	}

	s.Logger.Infof("Finished synchronizing with cluster, fetched %d blocks, starting from block [%d], up until and including block [%d]",
		blocksFetched, startHeight, lastPulledBlock.Header.Number)

	return lastPulledBlock, nil
}

func (s *BFTSynchronizer) isStopped() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.stopped
}
