/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Chaguar/common/common/channelconfig"
	"github.com/Chaguar/common/protoutil"
	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"

	"github.com/Chaguar/common/api/ordererpb"
	"github.com/Chaguar/orderer/config"
	"github.com/Chaguar/orderer/config/generate"
	"github.com/Chaguar/orderer/testutil"
	"github.com/Chaguar/orderer/testutil/fabric"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestReadGenesisBlock(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigBinaryPath := testutil.PrepareSharedConfigBinary(t, dir)
	block, err := generate.CreateGenesisBlock(dir, dir, sharedConfigYaml, sharedConfigBinaryPath, fabric.GetDevConfigDir())
	require.NoError(t, err)
	require.NotNil(t, block)

	blockPath := filepath.Join(dir, "bootstrap.block")
	data, err := os.ReadFile(blockPath)
	require.NoError(t, err)
	configBlock, err := protoutil.UnmarshalBlock(data)
	require.NoError(t, err)
	require.NotNil(t, block)
	consensusMetaData, err := config.ReadSharedConfigFromBootstrapConfigBlock(configBlock, factory.GetDefault())
	require.NoError(t, err)

	var sharedConfigFromBlock ordererpb.SharedConfig
	err = proto.Unmarshal(consensusMetaData, &sharedConfigFromBlock)
	require.NoError(t, err)

	sharedConfigYamlPath := filepath.Join(dir, "bootstrap", "shared_config.yaml")
	actualSharedConfig, _, err := config.LoadSharedConfig(sharedConfigYamlPath)
	require.NoError(t, err)

	proto.Equal(&sharedConfigFromBlock, actualSharedConfig)
}

func TestReadGenesisBlock_Errors(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigBinaryPath := testutil.PrepareSharedConfigBinary(t, dir)
	block, err := generate.CreateGenesisBlock(dir, dir, sharedConfigYaml, sharedConfigBinaryPath, fabric.GetDevConfigDir())
	require.NoError(t, err)
	require.NotNil(t, block)

	blockPath := filepath.Join(dir, "bootstrap.block")
	data, err := os.ReadFile(blockPath)
	require.NoError(t, err)
	configBlock, err := protoutil.UnmarshalBlock(data)
	require.NoError(t, err)
	require.NotNil(t, block)

	// Mess the configBlock
	configBlock.Data = nil

	// expect error
	c, err := config.ReadSharedConfigFromBootstrapConfigBlock(configBlock, factory.GetDefault())
	require.EqualError(t, err, "block data is nil")
	require.Nil(t, c)
}

func TestReadConsensusMetadataFromConfigBlockWithoutOrderer(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigBinaryPath := testutil.PrepareSharedConfigBinary(t, dir)
	configBlock, err := generate.CreateGenesisBlock(dir, dir, sharedConfigYaml, sharedConfigBinaryPath, fabric.GetDevConfigDir())
	require.NoError(t, err)

	// Remove the orderer group from the config.
	envelope, err := protoutil.ExtractEnvelope(configBlock, 0)
	require.NoError(t, err)
	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	require.NoError(t, err)
	configEnvelope := &common.ConfigEnvelope{}
	require.NoError(t, proto.Unmarshal(payload.Data, configEnvelope))
	require.Contains(t, configEnvelope.Config.ChannelGroup.Groups, channelconfig.OrdererGroupKey)
	delete(configEnvelope.Config.ChannelGroup.Groups, channelconfig.OrdererGroupKey)
	payload.Data = protoutil.MarshalOrPanic(configEnvelope)
	envelope.Payload = protoutil.MarshalOrPanic(payload)
	configBlock.Data.Data[0] = protoutil.MarshalOrPanic(envelope)

	metadata, err := config.ReadConsensusMetadataFromConfigBlock(configBlock, factory.GetDefault())
	require.EqualError(t, err, "orderer entry in the config block is empty")
	require.Nil(t, metadata)
}
