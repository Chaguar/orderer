/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"bytes"
	"encoding/asn1"
	"errors"
	"fmt"

	"github.com/Chaguar/common/api/msppb"
	"github.com/Chaguar/common/common/channelconfig"
	"github.com/Chaguar/common/common/policies"
	"github.com/Chaguar/common/protoutil"
	"github.com/Chaguar/orderer/node/consensus/state"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
)

type SigVerifierFunc func(block *common.Block, verifyData bool) error

func createErrorFunc(err error) SigVerifierFunc {
	err = fmt.Errorf("failed to initialize sig verifier function: %w", err)
	return func(_ *common.Block, _ bool) error {
		return err
	}
}

// SigVerifierCreator creates a SigVerifier out of a config envelope
type SigVerifierCreator struct {
	Logger *flogging.FabricLogger
	BCCSP  bccsp.BCCSP
}

// SigVerifierFromConfig creates a SigVerifier from the given configuration.
func (svc *SigVerifierCreator) SigVerifierFromConfig(configuration *common.ConfigEnvelope, channel string, configBlockNum uint64) (SigVerifierFunc, error) {
	if configuration == nil {
		err := errors.New("nil configuration")
		return createErrorFunc(err), err
	}

	bundle, err := channelconfig.NewBundle(channel, configuration.Config, svc.BCCSP)
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

	bsv := &BlockSigVerifier{
		Policy:         policy,
		Consenters:     consenters,
		Logger:         svc.Logger,
		ConfigBlockNum: configBlockNum,
	}

	return bsv.Verify, nil
}

//go:generate counterfeiter -o mocks/policy.go . policy
type policy interface { // copied from common.policies to avoid circular import.
	// EvaluateSignedData takes a set of SignedData and evaluates whether
	// 1) the signatures are valid over the related message
	// 2) the signing identities satisfy the policy
	EvaluateSignedData(signatureSet []*protoutil.SignedData) error
}

// BlockSigVerifier can be used to verify signatures over blocks using the given policy.
type BlockSigVerifier struct {
	Policy         policy
	Consenters     []*common.Consenter
	Logger         *flogging.FabricLogger
	ConfigBlockNum uint64
}

// Verify verifies a consenter block's signatures using the verifier's policy.
func (v *BlockSigVerifier) Verify(consenterBlock *common.Block, verifyData bool) error {
	if consenterBlock == nil {
		return errors.New("consenter block is nil")
	}
	if consenterBlock.Header == nil {
		return errors.New("consenter block header is nil")
	}
	if consenterBlock.Metadata == nil || consenterBlock.Metadata.Metadata == nil {
		return errors.New("consenter block metadata is nil")
	}
	if len(consenterBlock.Metadata.Metadata) < len(common.BlockMetadataIndex_name) {
		return errors.New("consenter block metadata len is less than expected")
	}
	if verifyData {
		if consenterBlock.Data == nil || len(consenterBlock.Data.Data) == 0 {
			return errors.New("consenter block data is nil or empty")
		}
	}

	// Extract signatures from metadata
	sigsBytes := consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES]
	sigs, err := state.BytesToDecisionSignatures(sigsBytes)
	if err != nil {
		return fmt.Errorf("failed to deserialize signatures from metadata: %w", err)
	}

	// Pre-calculate the header bytes for proposal signature
	blockHeaderBytes := protoutil.BlockHeaderBytes(consenterBlock.GetHeader())

	// Pre-calculate orderer block metadata for proposal signature
	proposalMetadata := consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER]
	lastConfigIndex, err := state.GetLastConfigIndexFromConsenterBlock(consenterBlock)
	if err != nil {
		return fmt.Errorf("failed to get last config index from consenter block: %w", err)
	}
	proposalOrdererBlockMetadata := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
		LastConfig:        &common.LastConfig{Index: lastConfigIndex},
		ConsenterMetadata: proposalMetadata,
	})

	var hdr state.Header
	var availableCommonBlocksHeaderBytes [][]byte
	var ordererBlockMetadata [][]byte
	if verifyData {
		proposalBytes := consenterBlock.Data.Data[0]
		proposal, err := state.BytesToProposal(proposalBytes)
		if err != nil {
			return fmt.Errorf("failed to deserialize proposal from block data: %w", err)
		}
		if err := hdr.Deserialize(proposal.Header); err != nil {
			return fmt.Errorf("failed deserializing proposal header: %w", err)
		}
		// Pre-calculate the available common blocks block header bytes
		availableCommonBlocksHeaderBytes = make([][]byte, 0, len(hdr.AvailableCommonBlocks))
		for i, availableBlock := range hdr.AvailableCommonBlocks {
			if availableBlock.Header == nil {
				return fmt.Errorf("available common block header is nil at index %d", i)
			}
			availableCommonBlocksHeaderBytes = append(availableCommonBlocksHeaderBytes, protoutil.BlockHeaderBytes(availableBlock.GetHeader()))
		}

		// Pre-calculate orderer block metadata
		ordererBlockMetadata = make([][]byte, 0, len(hdr.AvailableCommonBlocks))
		lastConfigNum := v.ConfigBlockNum
		for i, availableBlock := range hdr.AvailableCommonBlocks {
			if availableBlock.Metadata == nil {
				return fmt.Errorf("available common block metadata is nil at index %d", i)
			}
			if len(availableBlock.Metadata.Metadata) <= int(common.BlockMetadataIndex_ORDERER) {
				return fmt.Errorf("available common block metadata is missing ORDERER entry at index %d", i)
			}
			if protoutil.IsConfigBlock(availableBlock) {
				lastConfigNum = availableBlock.Header.Number
			}
			ordererBlockMetadata = append(ordererBlockMetadata, protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
				LastConfig:        &common.LastConfig{Index: lastConfigNum},
				ConsenterMetadata: availableBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
			}))
		}
	}

	signatureSets := make([][]*protoutil.SignedData, 0, len(hdr.AvailableCommonBlocks)+1)
	for range len(hdr.AvailableCommonBlocks) + 1 {
		signatureSets = append(signatureSets, make([]*protoutil.SignedData, 0, len(sigs)))
	}
	for _, sig := range sigs {
		var values [][]byte
		if _, err := asn1.Unmarshal(sig.Value, &values); err != nil {
			v.Logger.Warnf("failed to unmarshal signature values with id %d; err: %s", sig.ID, err)
			continue
		}
		var msgs [][]byte
		if _, err := asn1.Unmarshal(sig.Msg, &msgs); err != nil {
			v.Logger.Warnf("failed to unmarshal signature msgs with id %d; err: %s", sig.ID, err)
			continue
		}
		if len(msgs) != len(values) {
			v.Logger.Warnf("signature with id %d has different number of values and msgs", sig.ID)
			continue
		}
		if len(msgs) == 0 {
			v.Logger.Warnf("signature with id %d has no messages", sig.ID)
			continue
		}
		if verifyData {
			if len(msgs) != len(hdr.AvailableCommonBlocks)+1 {
				v.Logger.Warnf("signature with id %d has different number of messages than available common blocks", sig.ID)
				continue
			}
		}
		proposalMsg := &protoutil.MessageToSign{}
		if err := proposalMsg.ASN1Unmarshal(msgs[0]); err != nil {
			v.Logger.Warnf("failed to unmarshal the first signature msg with id %d; err: %s", sig.ID, err)
			continue
		}
		// verify proposal message identifier header
		idHeader, err := protoutil.UnmarshalIdentifierHeader(proposalMsg.IdentifierHeader)
		if err != nil {
			v.Logger.Warnf("failed to unmarshal identifier header from proposal message; err: %s", err)
			continue
		}
		if uint64(idHeader.GetIdentifier()) != sig.ID {
			v.Logger.Warnf("signature ID %d does not match identifier header ID %d in proposal message", sig.ID, idHeader.GetIdentifier())
			continue
		}
		signerIdentity := v.searchConsenterIdentityByID(idHeader.GetIdentifier())
		if signerIdentity == nil {
			v.Logger.Warnf("signature with id %d has unknown consenter identity", sig.ID)
			continue
		}

		if verifyData {
			signatures, err := v.collectBlocksSignatures(msgs[1:], values[1:], availableCommonBlocksHeaderBytes, ordererBlockMetadata, sig.ID)
			if err != nil {
				v.Logger.Warnf("failed to collect signatures for all blocks with signer id %d; err: %s", sig.ID, err)
				continue
			}
			for i := range len(hdr.AvailableCommonBlocks) {
				signatureSets[i+1] = append(signatureSets[i+1], signatures[i])
			}
		}

		computedMsg := &protoutil.MessageToSign{
			IdentifierHeader:     proposalMsg.IdentifierHeader,
			BlockHeader:          blockHeaderBytes,
			OrdererBlockMetadata: proposalOrdererBlockMetadata,
		}
		marshaledComputedMsg := computedMsg.ASN1MarshalOrPanic()
		if !bytes.Equal(marshaledComputedMsg, msgs[0]) {
			v.Logger.Warnf("first signature msg with id %d does not match computed message", sig.ID)
			continue
		}
		signatureSets[0] = append(signatureSets[0], &protoutil.SignedData{
			Identity:  signerIdentity,
			Data:      marshaledComputedMsg,
			Signature: values[0],
		})
	}

	if err := v.Policy.EvaluateSignedData(signatureSets[0]); err != nil {
		return fmt.Errorf("failed to evaluate signature set of the first (proposal) signature for block ID %d: %w", consenterBlock.GetHeader().GetNumber(), err)
	}

	if verifyData {
		for i := range len(hdr.AvailableCommonBlocks) {
			if err := v.Policy.EvaluateSignedData(signatureSets[i+1]); err != nil {
				return fmt.Errorf("failed to evaluate signature set in index %d for block ID %d: %w", i, consenterBlock.GetHeader().GetNumber(), err)
			}
		}
	}

	return nil
}

func (v *BlockSigVerifier) searchConsenterIdentityByID(identifier uint32) *msppb.Identity {
	for _, consenter := range v.Consenters {
		if consenter.Id == identifier {
			return msppb.NewIdentity(consenter.MspId, consenter.Identity)
		}
	}
	return nil
}

func (v *BlockSigVerifier) collectBlocksSignatures(msgs, values, blockHeaderBytes, ordererBlockMetadata [][]byte, id uint64) ([]*protoutil.SignedData, error) {
	if len(msgs) != len(values) || len(msgs) != len(blockHeaderBytes) || len(msgs) != len(ordererBlockMetadata) {
		return nil, fmt.Errorf("the length of the msgs, values, blockHeaderBytes and ordererBlockMetadata slices are not equal for signature id %d", id)
	}
	signatureSet := make([]*protoutil.SignedData, 0, len(msgs))
	for i, msg := range msgs {
		signedMsg := &protoutil.MessageToSign{}
		if err := signedMsg.ASN1Unmarshal(msg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal the signature msg in index %d with id %d: %w", i, id, err)
		}
		// verify message identifier header
		idHeader, err := protoutil.UnmarshalIdentifierHeader(signedMsg.IdentifierHeader)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal identifier header from the signature msg in index %d with id %d: %w", i, id, err)
		}
		if uint64(idHeader.GetIdentifier()) != id {
			return nil, fmt.Errorf("signature ID %d does not match identifier header ID %d in signature msg in index %d", id, idHeader.GetIdentifier(), i)
		}
		signerIdentity := v.searchConsenterIdentityByID(idHeader.GetIdentifier())
		if signerIdentity == nil {
			return nil, fmt.Errorf("signature with id %d has unknown consenter identity", id)
		}
		computedMsg := &protoutil.MessageToSign{
			IdentifierHeader:     signedMsg.IdentifierHeader,
			BlockHeader:          blockHeaderBytes[i],
			OrdererBlockMetadata: ordererBlockMetadata[i],
		}
		marshaledComputedMsg := computedMsg.ASN1MarshalOrPanic()
		if !bytes.Equal(marshaledComputedMsg, msg) {
			return nil, fmt.Errorf("signature msg in index %d with id %d does not match computed message", i, id)
		}
		signatureSet = append(signatureSet, &protoutil.SignedData{
			Identity:  signerIdentity,
			Data:      marshaledComputedMsg,
			Signature: values[i],
		})
	}
	return signatureSet, nil
}
