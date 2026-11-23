/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliverclient

import (
	"errors"
	"fmt"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"

	"github.com/Chaguar/common/common/channelconfig"
	"github.com/Chaguar/common/common/policies"
	"github.com/Chaguar/common/protoutil"
)

func createErrorFunc(err error) protoutil.BlockVerifierFunc {
	err = fmt.Errorf("failed to initialize block verifier function: %w", err)
	return func(_ *common.BlockHeader, _ *common.BlockMetadata) error {
		return err
	}
}

// BlockVerifierAssembler creates a BlockVerifier out of a config envelope
type BlockVerifierAssembler struct {
	Logger *flogging.FabricLogger
	BCCSP  bccsp.BCCSP
}

// VerifierFromConfig creates a BlockVerifier from the given configuration.
func (bva *BlockVerifierAssembler) VerifierFromConfig(configuration *common.ConfigEnvelope, channel string) (protoutil.BlockVerifierFunc, error) {
	bundle, err := channelconfig.NewBundle(channel, configuration.Config, bva.BCCSP)
	if err != nil {
		return createErrorFunc(err), err
	}

	policy, exists := bundle.PolicyManager().GetPolicy(policies.BlockValidation)
	if !exists {
		err := fmt.Errorf("no `%s` policy in config block", policies.BlockValidation)
		return createErrorFunc(err), err
	}

	var consenters []*common.Consenter
	cfg, ok := bundle.OrdererConfig()
	if !ok {
		err := errors.New("no orderer section in config block")
		return createErrorFunc(err), err
	}
	consenters = cfg.Consenters()

	return protoutil.BlockSignatureVerifier(true, consenters, policy), nil
}
