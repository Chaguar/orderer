/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"fmt"
	"sort"

	"github.com/Chaguar/common/common/policies"
	"github.com/Chaguar/orderer/common/types"
)

func (c *BatcherNodeConfig) GetShardsIDs() []types.ShardID {
	var ids []types.ShardID
	for _, shard := range c.Shards {
		ids = append(ids, shard.ShardId)
	}
	sort.Slice(ids, func(i, j int) bool {
		return int(ids[i]) < int(ids[j])
	})
	return ids
}

func (c *ConsenterNodeConfig) GetRequestMaxBytes() uint64 {
	return c.RequestMaxBytes
}

func (c *ConsenterNodeConfig) GetClientSignatureVerificationRequired() bool {
	return c.ClientSignatureVerificationRequired
}

func (c *ConsenterNodeConfig) GetChannelID() string {
	return c.Bundle.ConfigtxValidator().ChannelID()
}

func (c *ConsenterNodeConfig) GetPolicyManager() policies.Manager {
	return c.Bundle.PolicyManager()
}

func (c *BatcherNodeConfig) GetRequestMaxBytes() uint64 {
	return c.RequestMaxBytes
}

func (c *BatcherNodeConfig) GetClientSignatureVerificationRequired() bool {
	return c.ClientSignatureVerificationRequired
}

func (c *BatcherNodeConfig) GetChannelID() string {
	return c.Bundle.ConfigtxValidator().ChannelID()
}

func (c *BatcherNodeConfig) GetPolicyManager() policies.Manager {
	return c.Bundle.PolicyManager()
}

func (rfc *RouterNodeConfig) GetRequestMaxBytes() uint64 {
	return rfc.RequestMaxBytes
}

func (rfc *RouterNodeConfig) GetClientSignatureVerificationRequired() bool {
	return rfc.ClientSignatureVerificationRequired
}

func (rfc *RouterNodeConfig) GetChannelID() string {
	return rfc.Bundle.ConfigtxValidator().ChannelID()
}

func (rfc *RouterNodeConfig) GetPolicyManager() policies.Manager {
	return rfc.Bundle.PolicyManager()
}

func (c *AssemblerNodeConfig) GetChannelID() string {
	return c.Bundle.ConfigtxValidator().ChannelID()
}

// OwnConsenter returns the consenter of the assembler's own party, which the assembler pulls decisions from and
// acks configs to.
func (c *AssemblerNodeConfig) OwnConsenter() (ConsenterInfo, error) {
	for _, consenter := range c.Consenters {
		if consenter.PartyID == c.PartyId {
			return consenter, nil
		}
	}
	return ConsenterInfo{}, fmt.Errorf("no consenter of party %d among the %d consenters", c.PartyId, len(c.Consenters))
}
