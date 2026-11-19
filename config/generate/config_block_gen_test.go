/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package generate_test

import (
	"testing"

	"github.com/Chaguar/orderer/testutil/fabric"

	"github.com/Chaguar/orderer/testutil"

	"github.com/Chaguar/orderer/config/generate"
	"github.com/stretchr/testify/require"
)

func TestCreateGenesisBlock(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigPath := testutil.PrepareSharedConfigBinary(t, dir)

	block, err := generate.CreateGenesisBlock(dir, dir, sharedConfigYaml, sharedConfigPath, fabric.GetDevConfigDir())
	require.NoError(t, err)
	require.NotNil(t, block)
}
