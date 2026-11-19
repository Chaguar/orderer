/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package orderers_test

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/stretchr/testify/require"

	"github.com/Chaguar/orderer/common/deliverclient/orderers"
	"github.com/Chaguar/orderer/common/types"
)

func TestCreateConnectionSource(t *testing.T) {
	factory := &orderers.ConnectionSourceFactory{}
	require.NotNil(t, factory)

	lg := flogging.MustGetLogger("test")
	connSource := factory.CreateConnectionSource(lg, types.PartyID(0))
	require.NotNil(t, connSource)

	factory = &orderers.ConnectionSourceFactory{}
	require.NotNil(t, factory)
	connSource = factory.CreateConnectionSource(lg, types.PartyID(1))
	require.NotNil(t, connSource)
}
