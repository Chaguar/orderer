/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/config"
)

type EndpointAndCerts struct {
	Endpoint   string
	TLSCACerts []config.RawBytes
}

func GetBatchersEndpointsAndCerts(batchers []config.BatcherInfo) map[types.PartyID]*EndpointAndCerts {
	m := make(map[types.PartyID]*EndpointAndCerts)
	for _, batcher := range batchers {
		m[batcher.PartyID] = &EndpointAndCerts{
			Endpoint:   batcher.Endpoint,
			TLSCACerts: batcher.TLSCACerts,
		}
	}
	return m
}
