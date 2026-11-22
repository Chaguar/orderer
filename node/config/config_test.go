/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"os"
	"path"
	"testing"

	"github.com/Chaguar/orderer/common/types"
	"github.com/stretchr/testify/require"
)

func TestRouterNodeConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	batchers := []BatcherInfo{
		{1, "127.0.0.1:7050", []RawBytes{{1, 2, 3}, {4, 5, 6}}, RawBytes("BatcherPubKey-1"), RawBytes("TLS CERT")},
		{2, "127.0.0.1:7051", []RawBytes{{1, 2, 3}, {4, 5, 6}}, RawBytes("BatcherPubKey-2"), RawBytes("TLS CERT")},
	}

	consenter := ConsenterInfo{1, "127.0.0.1:7050", RawBytes("ConsenterPubKey-1"), []RawBytes{{1, 2, 3}, {4, 5, 6}}}

	shards := []ShardInfo{{ShardId: 1, Batchers: batchers}}
	rnc := &RouterNodeConfig{
		TLSCertificateFile:            []byte("tls cert"),
		TLSPrivateKeyFile:             []byte("tls key"),
		PartyID:                       1,
		Shards:                        shards,
		Consenter:                     consenter,
		NumOfConnectionsForBatcher:    1,
		NumOfgRPCStreamsPerConnection: 2,
		ClientRootCAs:                 [][]byte{},
	}

	path := path.Join(dir, "router_node_config.yaml")
	err = NodeConfigToYAML(rnc, path)
	require.NoError(t, err)

	var rncFromYAML RouterNodeConfig
	err = NodeConfigFromYAML(&rncFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, rncFromYAML, *rnc)
}

func TestBatcherNodeConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	batchers := []BatcherInfo{
		{1, "127.0.0.1:7050", []RawBytes{{1, 2, 3}, {4, 5, 6}}, RawBytes("BatcherPubKey-1"), RawBytes("TLS CERT")},
		{2, "127.0.0.1:7051", []RawBytes{{1, 2, 3}, {4, 5, 6}}, RawBytes("BatcherPubKey-2"), RawBytes("TLS CERT")},
	}
	shards := []ShardInfo{{ShardId: 1, Batchers: batchers}}
	consenters := []ConsenterInfo{{1, "127.0.0.1:7050", RawBytes("ConsenterPubKey-1"), []RawBytes{{1, 2, 3}, {4, 5, 6}}}}

	bnc := &BatcherNodeConfig{
		Shards:             shards,
		Consenters:         consenters,
		PartyId:            1,
		TLSPrivateKeyFile:  RawBytes("TlsPrivateKey"),
		TLSCertificateFile: RawBytes("TlsCertKey"),
		SigningPrivateKey:  RawBytes("SigningPrivateKey"),
		ClientRootCAs:      [][]byte{},
	}

	path := path.Join(dir, "batcher_node_config.yaml")
	err = NodeConfigToYAML(bnc, path)
	require.NoError(t, err)

	var bncFromYAML BatcherNodeConfig
	err = NodeConfigFromYAML(&bncFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, bncFromYAML, *bnc)
}

func TestConsenterNodeConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	batchers := []BatcherInfo{
		{1, "127.0.0.1:7050", []RawBytes{{1, 2, 3}, {4, 5, 6}}, RawBytes("BatcherPubKey-1"), RawBytes("TLS CERT")},
		{2, "127.0.0.1:7051", []RawBytes{{1, 2, 3}, {4, 5, 6}}, RawBytes("BatcherPubKey-2"), RawBytes("TLS CERT")},
	}
	shards := []ShardInfo{{ShardId: 1, Batchers: batchers}}
	consenters := []ConsenterInfo{{1, "127.0.0.1:7050", RawBytes("ConsenterPubKey-1"), []RawBytes{{1, 2, 3}, {4, 5, 6}}}}
	assemblers := []AssemblerInfo{{1, "127.0.0.1:7050", []RawBytes{{1, 2, 3}, {4, 5, 6}}, RawBytes("AssemblerPubKey-1"), RawBytes("TLS CERT")}}
	router := RouterInfo{1, "127.0.0.1:7050", []RawBytes{{1, 2, 3}, {4, 5, 6}}, RawBytes("ConsenterPubKey-1")}

	cnc := &ConsenterNodeConfig{
		Shards:             shards,
		Consenters:         consenters,
		Assemblers:         assemblers,
		Router:             router,
		PartyId:            1,
		TLSPrivateKeyFile:  RawBytes("TlsPrivateKey"),
		TLSCertificateFile: RawBytes("TlsCertKey"),
		SigningPrivateKey:  RawBytes("SigningPrivateKey"),
		ClientRootCAs:      [][]byte{},
	}

	path := path.Join(dir, "consenter_node_config.yaml")
	err = NodeConfigToYAML(cnc, path)
	require.NoError(t, err)

	var cncFromYAML ConsenterNodeConfig
	err = NodeConfigFromYAML(&cncFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, cncFromYAML, *cnc)
}

func TestAssemblerNodeConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	batchers := []BatcherInfo{
		{1, "127.0.0.1:7050", []RawBytes{{1, 2, 3}, {4, 5, 6}}, RawBytes("BatcherPubKey-1"), RawBytes("TLS CERT")},
		{2, "127.0.0.1:7051", []RawBytes{{1, 2, 3}, {4, 5, 6}}, RawBytes("BatcherPubKey-2"), RawBytes("TLS CERT")},
	}
	shards := []ShardInfo{{ShardId: 1, Batchers: batchers}}
	consenters := []ConsenterInfo{
		{1, "127.0.0.1:7050", RawBytes("ConsenterPubKey-1"), []RawBytes{{1, 2, 3}, {4, 5, 6}}},
		{2, "127.0.0.1:7051", RawBytes("ConsenterPubKey-2"), []RawBytes{{1, 2, 3}, {4, 5, 6}}},
	}

	anc := &AssemblerNodeConfig{
		TLSCertificateFile: RawBytes{4, 5, 6},
		TLSPrivateKeyFile:  RawBytes{7, 8, 9},
		PartyId:            1,
		Shards:             shards,
		Consenters:         consenters,
		ClientRootCAs:      [][]byte{},
	}

	path := path.Join(dir, "assembler_node_config.yaml")
	err = NodeConfigToYAML(anc, path)
	require.NoError(t, err)

	var ancFromYAML AssemblerNodeConfig
	err = NodeConfigFromYAML(&ancFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, ancFromYAML, *anc)
}

// Scenario:
//  1. Build the config of the assembler of party 2, listing the consenters of parties 1, 2 and 3.
//  2. Expect OwnConsenter to return the consenter of party 2.
//  3. Remove the consenter of party 2 from the list, and expect OwnConsenter to fail.
func TestAssemblerOwnConsenter(t *testing.T) {
	consenters := []ConsenterInfo{
		{PartyID: 1, Endpoint: "127.0.0.1:7050"},
		{PartyID: 2, Endpoint: "127.0.0.1:7051"},
		{PartyID: 3, Endpoint: "127.0.0.1:7052"},
	}
	anc := &AssemblerNodeConfig{PartyId: 2, Consenters: consenters}

	consenter, err := anc.OwnConsenter()
	require.NoError(t, err)
	require.Equal(t, consenters[1], consenter)

	anc.Consenters = []ConsenterInfo{consenters[0], consenters[2]}
	_, err = anc.OwnConsenter()
	require.EqualError(t, err, "no consenter of party 2 among the 2 consenters")
}

func TestShardsIDsFromBatcherConfig(t *testing.T) {
	shards := []ShardInfo{{ShardId: 1}, {ShardId: 3}, {ShardId: 2}, {ShardId: 4}}
	bnc := &BatcherNodeConfig{
		Shards: shards,
	}
	require.Equal(t, []types.ShardID{1, 2, 3, 4}, bnc.GetShardsIDs())
}
