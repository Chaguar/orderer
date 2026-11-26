/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"testing"

	"github.com/Chaguar/common/protoutil"
	arma_types "github.com/Chaguar/orderer/common/types"
	node_consensus "github.com/Chaguar/orderer/node/consensus"
	"github.com/Chaguar/orderer/node/consensus/state"
	"github.com/Chaguar/orderer/node/ledger"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/stretchr/testify/require"
)

// Inputs of a data block and of a config block, all non-zero so that a field left unset by a creator, or
// left unchecked by a verifier, shows up as a mismatch.
var (
	testPrevHash = []byte{9, 8, 7, 6}

	testBatchID = state.NewAvailableBatch(3, 2, 7, []byte{1, 2, 3, 4})

	testConfigReq = []byte("config request")
)

const (
	testDataBlockNum       = uint64(5)
	testConfigBlockNum     = uint64(6)
	testTXCount            = uint64(100)
	testDecisionNum        = arma_types.DecisionNum(9)
	testBatchCount         = 4
	testBatchIndex         = 2
	testConfigBatchIndex   = 3
	testLastConfigBlockNum = uint64(1)
)

func TestCreateDataCommonBlock(t *testing.T) {
	block, err := node_consensus.CreateDataCommonBlock(testDataBlockNum, testPrevHash, testBatchID, testTXCount, testDecisionNum, testBatchCount, testBatchIndex, testLastConfigBlockNum)
	require.NoError(t, err)

	require.Equal(t, testDataBlockNum, block.GetHeader().GetNumber())
	require.Equal(t, testPrevHash, block.GetHeader().GetPreviousHash())
	require.Equal(t, testBatchID.Digest(), block.GetHeader().GetDataHash())
	require.Empty(t, block.GetData().GetData())

	primary, shard, seq, decisionNum, batchIndex, batchCount, txCount, err := ledger.AssemblerBlockMetadataFromBytes(block.GetMetadata().GetMetadata()[common.BlockMetadataIndex_ORDERER])
	require.NoError(t, err)
	require.Equal(t, testBatchID.Primary(), primary)
	require.Equal(t, testBatchID.Shard(), shard)
	require.Equal(t, testBatchID.Seq(), seq)
	require.Equal(t, testDecisionNum, decisionNum)
	require.Equal(t, uint32(testBatchIndex), batchIndex)
	require.Equal(t, uint32(testBatchCount), batchCount)
	require.Equal(t, testTXCount, txCount)

	lastConfig, err := protoutil.GetLastConfigIndexFromBlock(block)
	require.NoError(t, err)
	require.Equal(t, testLastConfigBlockNum, lastConfig)
}

func TestCreateConfigCommonBlock(t *testing.T) {
	configBlock, err := node_consensus.CreateConfigCommonBlock(testConfigBlockNum, testPrevHash, testTXCount, testDecisionNum, testBatchCount, testConfigBatchIndex, testConfigReq)
	require.NoError(t, err)

	require.Equal(t, testConfigBlockNum, configBlock.GetHeader().GetNumber())
	require.Equal(t, testPrevHash, configBlock.GetHeader().GetPreviousHash())
	require.Equal(t, [][]byte{testConfigReq}, configBlock.GetData().GetData())
	require.Equal(t, requestsDigest([][]byte{testConfigReq}), configBlock.GetHeader().GetDataHash())

	primary, shard, seq, decisionNum, batchIndex, batchCount, txCount, err := ledger.AssemblerBlockMetadataFromBytes(configBlock.GetMetadata().GetMetadata()[common.BlockMetadataIndex_ORDERER])
	require.NoError(t, err)
	require.Equal(t, arma_types.PartyID(0), primary)
	require.Equal(t, arma_types.ShardIDConsensus, shard)
	require.Equal(t, arma_types.BatchSequence(0), seq)
	require.Equal(t, testDecisionNum, decisionNum)
	require.Equal(t, uint32(testConfigBatchIndex), batchIndex)
	require.Equal(t, uint32(testBatchCount), batchCount)
	require.Equal(t, testTXCount, txCount)

	// A config block is its own last config.
	lastConfig, err := protoutil.GetLastConfigIndexFromBlock(configBlock)
	require.NoError(t, err)
	require.Equal(t, testConfigBlockNum, lastConfig)
}

func TestCreateAndVerifyDataCommonBlock(t *testing.T) {
	for _, tst := range []struct {
		name            string
		err             string
		blockNum        uint64
		prevHash        []byte
		primary         arma_types.PartyID
		shard           arma_types.ShardID
		seq             arma_types.BatchSequence
		digest          []byte
		decisionNum     arma_types.DecisionNum
		batchCount      int
		batchIndex      int
		txCount         uint64
		lastConfigBlock uint64
	}{
		{
			name: "no error",
		},
		{
			name:     "wrong block number",
			err:      "proposed block header number",
			blockNum: 1,
		},
		{
			name:     "wrong prev hash",
			err:      "proposed block header prev hash",
			prevHash: []byte{1},
		},
		{
			name:    "wrong primary",
			err:     "proposed block metadata",
			primary: 1,
		},
		{
			name:  "wrong shard",
			err:   "proposed block metadata",
			shard: 1,
		},
		{
			name: "wrong seq",
			err:  "proposed block metadata",
			seq:  1,
		},
		{
			name:   "wrong digest",
			err:    "proposed block data hash",
			digest: []byte{1},
		},
		{
			name:        "wrong decision number",
			err:         "proposed block metadata",
			decisionNum: 1,
		},
		{
			name:       "wrong batch count",
			err:        "proposed block metadata",
			batchCount: 1,
		},
		{
			name:       "wrong batch index",
			err:        "proposed block metadata",
			batchIndex: 1,
		},
		{
			name:    "wrong tx count",
			err:     "proposed block metadata",
			txCount: 1,
		},
		{
			name:            "wrong last config block",
			err:             "last config in block",
			lastConfigBlock: 1,
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			block, err := node_consensus.CreateDataCommonBlock(0, nil, state.NewAvailableBatch(0, 0, 0, nil), 0, 0, 0, 0, 0)
			require.NoError(t, err)
			require.NotNil(t, block)
			err = node_consensus.VerifyDataCommonBlock(block, tst.blockNum, tst.prevHash, state.NewAvailableBatch(tst.primary, tst.shard, tst.seq, tst.digest), tst.txCount, tst.decisionNum, tst.batchCount, tst.batchIndex, tst.lastConfigBlock)
			if tst.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tst.err)
			}

			block, err = node_consensus.CreateDataCommonBlock(tst.blockNum, tst.prevHash, state.NewAvailableBatch(tst.primary, tst.shard, tst.seq, tst.digest), tst.txCount, tst.decisionNum, tst.batchCount, tst.batchIndex, tst.lastConfigBlock)
			require.NoError(t, err)
			require.NotNil(t, block)
			err = node_consensus.VerifyDataCommonBlock(block, 0, nil, state.NewAvailableBatch(0, 0, 0, nil), 0, 0, 0, 0, 0)
			if tst.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tst.err)
			}
		})
	}
}

func TestCreateAndVerifyConfigCommonBlock(t *testing.T) {
	for _, tst := range []struct {
		name        string
		err         string
		blockNum    uint64
		prevHash    []byte
		dataHash    []byte
		decisionNum arma_types.DecisionNum
		batchCount  int
		batchIndex  int
		txCount     uint64
	}{
		{
			name: "no error",
		},
		{
			name:     "wrong block number",
			err:      "proposed config block header number",
			blockNum: 1,
		},
		{
			name:     "wrong prev hash",
			err:      "proposed config block header prev hash",
			prevHash: []byte{1},
		},
		{
			name:     "wrong data hash",
			err:      "proposed config block data hash",
			dataHash: []byte{1},
		},
		{
			name:        "wrong decision number",
			err:         "proposed config block metadata",
			decisionNum: 1,
		},
		{
			name:       "wrong batch count",
			err:        "proposed config block metadata",
			batchCount: 1,
		},
		{
			name:       "wrong batch index",
			err:        "proposed config block metadata",
			batchIndex: 1,
		},
		{
			name:    "wrong tx count",
			err:     "proposed config block metadata",
			txCount: 1,
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			configBlock, err := node_consensus.CreateConfigCommonBlock(0, nil, 0, 0, 0, 0, nil)
			require.NoError(t, err)
			require.NotNil(t, configBlock)

			nilConfigReq := arma_types.BatchedRequests([][]byte{nil})
			dataHash := nilConfigReq.Digest()
			if tst.dataHash != nil {
				dataHash = tst.dataHash
			}

			err = node_consensus.VerifyConfigCommonBlock(configBlock, tst.blockNum, tst.prevHash, dataHash, tst.txCount, tst.decisionNum, tst.batchCount, tst.batchIndex)
			if tst.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tst.err)
			}

			var configReq []byte
			if tst.dataHash != nil {
				configReq = tst.dataHash
			}

			configBlock, err = node_consensus.CreateConfigCommonBlock(tst.blockNum, tst.prevHash, tst.txCount, tst.decisionNum, tst.batchCount, tst.batchIndex, configReq)
			require.NoError(t, err)
			require.NotNil(t, configBlock)

			err = node_consensus.VerifyConfigCommonBlock(configBlock, 0, nil, nilConfigReq.Digest(), 0, 0, 0, 0)
			if tst.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tst.err)
			}
		})
	}
}

// TestVerifyDataCommonBlockRejectsTamperedBlock checks that the data block verifier rejects a block that
// differs from the one it computes in a part that is not an input of the creator.
func TestVerifyDataCommonBlockRejectsTamperedBlock(t *testing.T) {
	for _, tst := range []struct {
		name   string
		tamper func(*common.Block)
		err    string
	}{
		{
			name:   "untampered",
			tamper: func(*common.Block) {},
		},
		{
			// Data in a data block could make protoutil.IsConfigBlock take it for a config block.
			name: "carries data",
			tamper: func(b *common.Block) {
				b.Data = &common.BlockData{Data: [][]byte{testConfigReq}}
			},
			err: "proposed data block carries 1 data entries instead of none",
		},
		{
			name: "undecodable last config metadata",
			tamper: func(b *common.Block) {
				b.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = []byte{0xff}
			},
			err: "failed getting proposed block metadata last config",
		},
		{
			name: "undecodable last config",
			tamper: func(b *common.Block) {
				b.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{Value: []byte{0xff}})
			},
			err: "failed unmarshaling proposed block metadata last config",
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			block, err := node_consensus.CreateDataCommonBlock(testDataBlockNum, testPrevHash, testBatchID, testTXCount, testDecisionNum, testBatchCount, testBatchIndex, testLastConfigBlockNum)
			require.NoError(t, err)
			tst.tamper(block)

			err = node_consensus.VerifyDataCommonBlock(block, testDataBlockNum, testPrevHash, testBatchID, testTXCount, testDecisionNum, testBatchCount, testBatchIndex, testLastConfigBlockNum)
			if tst.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tst.err)
			}
		})
	}
}

// TestVerifyConfigCommonBlockRejectsTamperedBlock checks that the config block verifier rejects a block that
// differs from the one it computes in a part that is not an input of the creator.
func TestVerifyConfigCommonBlockRejectsTamperedBlock(t *testing.T) {
	otherReq := []byte("other request")
	for _, tst := range []struct {
		name   string
		tamper func(*common.Block)
		err    string
	}{
		{
			name:   "untampered",
			tamper: func(*common.Block) {},
		},
		{
			name: "nil data",
			tamper: func(b *common.Block) {
				b.Data = nil
			},
			err: "empty config block data",
		},
		{
			name: "no data entries",
			tamper: func(b *common.Block) {
				b.Data = &common.BlockData{}
			},
			err: "empty config block data",
		},
		{
			name: "more than one tx",
			tamper: func(b *common.Block) {
				b.Data.Data = append(b.Data.Data, otherReq)
				b.Header.DataHash = requestsDigest(b.Data.Data)
			},
			err: "config block does not contain only one tx",
		},
		{
			name: "data replaced, header data hash kept",
			tamper: func(b *common.Block) {
				b.Data.Data = [][]byte{otherReq}
			},
			err: "proposed config block data hash isn't equal to computed data digest",
		},
		{
			name: "data and header data hash replaced",
			tamper: func(b *common.Block) {
				b.Data.Data = [][]byte{otherReq}
				b.Header.DataHash = requestsDigest(b.Data.Data)
			},
			err: "proposed config block data hash isn't equal to computed data hash",
		},
		{
			name: "last config is not the block itself",
			tamper: func(b *common.Block) {
				b.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
					Value: protoutil.MarshalOrPanic(&common.LastConfig{Index: testLastConfigBlockNum}),
				})
			},
			err: "last config in block metadata points to 1 instead of the config block number 6",
		},
		{
			name: "undecodable last config metadata",
			tamper: func(b *common.Block) {
				b.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = []byte{0xff}
			},
			err: "failed getting proposed config block metadata last config",
		},
		{
			name: "undecodable last config",
			tamper: func(b *common.Block) {
				b.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{Value: []byte{0xff}})
			},
			err: "failed unmarshaling proposed config block metadata last config",
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			configBlock, err := node_consensus.CreateConfigCommonBlock(testConfigBlockNum, testPrevHash, testTXCount, testDecisionNum, testBatchCount, testConfigBatchIndex, testConfigReq)
			require.NoError(t, err)
			tst.tamper(configBlock)

			dataHash := requestsDigest([][]byte{testConfigReq})
			err = node_consensus.VerifyConfigCommonBlock(configBlock, testConfigBlockNum, testPrevHash, dataHash, testTXCount, testDecisionNum, testBatchCount, testConfigBatchIndex)
			if tst.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tst.err)
			}
		})
	}
}

// TestVerifyCommonBlockRejectsMalformedBlock checks that the block verifiers reject a proposed block whose
// header or metadata is missing or short, instead of panicking on it.
func TestVerifyCommonBlockRejectsMalformedBlock(t *testing.T) {
	shortMetadata := protoutil.NewBlock(0, nil)
	shortMetadata.Metadata.Metadata = shortMetadata.Metadata.Metadata[:common.BlockMetadataIndex_ORDERER]
	for _, tst := range []struct {
		name  string
		block *common.Block
		err   string
	}{
		{name: "nil block", block: nil, err: "block is nil"},
		{name: "no header", block: &common.Block{Metadata: protoutil.NewBlock(0, nil).Metadata}, err: "block has no header"},
		{name: "no metadata", block: &common.Block{Header: &common.BlockHeader{}}, err: "block metadata has 0 entries"},
		{name: "short metadata", block: shortMetadata, err: "block metadata has 3 entries"},
	} {
		t.Run(tst.name, func(t *testing.T) {
			err := node_consensus.VerifyDataCommonBlock(tst.block, 0, nil, state.NewAvailableBatch(0, 0, 0, nil), 0, 0, 0, 0, 0)
			require.ErrorContains(t, err, tst.err)
			err = node_consensus.VerifyConfigCommonBlock(tst.block, 0, nil, nil, 0, 0, 0, 0)
			require.ErrorContains(t, err, tst.err)
		})
	}
}

// TestVerifyCommonBlockAcceptsExtraMetadata checks that a block with more metadata entries than this build
// knows of is accepted, so a node does not reject the blocks of a node built with a newer fabric-protos.
func TestVerifyCommonBlockAcceptsExtraMetadata(t *testing.T) {
	block, err := node_consensus.CreateDataCommonBlock(0, nil, state.NewAvailableBatch(0, 0, 0, nil), 0, 0, 0, 0, 0)
	require.NoError(t, err)
	block.Metadata.Metadata = append(block.Metadata.Metadata, []byte{})
	require.NoError(t, node_consensus.VerifyDataCommonBlock(block, 0, nil, state.NewAvailableBatch(0, 0, 0, nil), 0, 0, 0, 0, 0))

	configBlock, err := node_consensus.CreateConfigCommonBlock(0, nil, 0, 0, 0, 0, nil)
	require.NoError(t, err)
	configBlock.Metadata.Metadata = append(configBlock.Metadata.Metadata, []byte{})
	nilConfigReq := arma_types.BatchedRequests([][]byte{nil})
	require.NoError(t, node_consensus.VerifyConfigCommonBlock(configBlock, 0, nil, nilConfigReq.Digest(), 0, 0, 0, 0))
}

func requestsDigest(reqs [][]byte) []byte {
	br := arma_types.BatchedRequests(reqs)
	return br.Digest()
}
