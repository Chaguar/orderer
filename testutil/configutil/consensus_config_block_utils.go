/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configutil

import (
	"github.com/Chaguar/common/common/channelconfig"
	"github.com/Chaguar/common/protoutil"
	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/consensus"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
)

// CreateConsensusConfigBlock creates a config block received from consensus.
func CreateConsensusConfigBlock(bundle channelconfig.Resources, configUpdateEnvelope *common.Envelope, prevBlockHeader *common.BlockHeader, txCount uint64, decisionNum types.DecisionNum, batchCount int, batchIndex int) (*common.Block, error) {
	configEnvelope, err := bundle.ConfigtxValidator().ProposeConfigUpdate(configUpdateEnvelope)
	if err != nil {
		return nil, err
	}
	env, err := protoutil.CreateSignedEnvelope(common.HeaderType_CONFIG, bundle.ConfigtxValidator().ChannelID(), nil, configEnvelope, int32(0), 0)
	if err != nil {
		return nil, err
	}

	configReq, err := protoutil.Marshal(env)
	if err != nil {
		return nil, err
	}

	prevHash := protoutil.BlockHeaderHash(prevBlockHeader)
	configBlock, err := consensus.CreateConfigCommonBlock(prevBlockHeader.GetNumber()+1, prevHash, txCount, decisionNum, batchCount, batchIndex, configReq)
	if err != nil {
		return nil, err
	}

	return configBlock, nil
}
