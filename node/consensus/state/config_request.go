/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"errors"
	"fmt"

	"github.com/Chaguar/common/common/configtx"
	"github.com/Chaguar/common/protoutil"
	"github.com/Chaguar/orderer/common/types"
	stateprotos "github.com/Chaguar/orderer/node/protos/state"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"google.golang.org/protobuf/proto"
)

type ConfigRequest struct {
	Envelope *common.Envelope
}

func (c *ConfigRequest) ConfigSequence() (types.ConfigSequence, error) {
	payload, err := protoutil.UnmarshalPayload(c.Envelope.Payload)
	if err != nil {
		return 0, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return 0, fmt.Errorf("failed to unmarshal config envelope: %w", err)
	}

	if configEnvelope.Config == nil {
		return 0, errors.New("config envelope has nil config")
	}

	return types.ConfigSequence(configEnvelope.Config.Sequence), nil
}

// ID returns the transaction id of the config request, taken from the channel header of the
// CONFIG_UPDATE envelope (ConfigEnvelope.LastUpdate) that generated this configuration.
func (c *ConfigRequest) ID() (string, error) {
	payload, err := protoutil.UnmarshalPayload(c.Envelope.Payload)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal config envelope: %w", err)
	}

	if configEnvelope.LastUpdate == nil {
		return "", errors.New("config envelope has nil last update")
	}

	lastUpdatePayload, err := protoutil.UnmarshalPayload(configEnvelope.LastUpdate.Payload)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal last update payload: %w", err)
	}

	if lastUpdatePayload.Header == nil {
		return "", errors.New("last update payload has nil header")
	}

	channelHeader, err := protoutil.UnmarshalChannelHeader(lastUpdatePayload.Header.ChannelHeader)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal channel header: %w", err)
	}

	if channelHeader.Type != int32(common.HeaderType_CONFIG_UPDATE) {
		return "", fmt.Errorf("unexpected channel header type %d", channelHeader.Type)
	}

	// The SigFilter already rejects an empty TxID, on submit and on the followers' validation of a proposed config,
	// so this check is only a defense in depth for other callers.
	if channelHeader.TxId == "" {
		return "", errors.New("channel header has empty tx id")
	}

	return channelHeader.TxId, nil
}

func (c *ConfigRequest) toProto() *stateprotos.ConfigRequest {
	envelopeBytes, err := protoutil.Marshal(c.Envelope)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal envelope: %v", err))
	}
	return &stateprotos.ConfigRequest{
		Envelope: envelopeBytes,
	}
}

func (c *ConfigRequest) fromProto(pc *stateprotos.ConfigRequest) error {
	envelope, err := protoutil.UnmarshalEnvelope(pc.Envelope)
	if err != nil {
		return fmt.Errorf("failed to unmarshal envelope: %w", err)
	}
	c.Envelope = envelope
	return nil
}

func (c *ConfigRequest) Bytes() []byte {
	protoConfigRequest := c.toProto()
	bytes, err := proto.Marshal(protoConfigRequest)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal config request: %v", err))
	}
	return bytes
}

func (c *ConfigRequest) FromBytes(bytes []byte) error {
	protoConfigRequest := &stateprotos.ConfigRequest{}
	if err := proto.Unmarshal(bytes, protoConfigRequest); err != nil {
		return fmt.Errorf("failed to unmarshal config request: %w", err)
	}
	return c.fromProto(protoConfigRequest)
}

func (c *ConfigRequest) String() string {
	configSeq, err := c.ConfigSequence()
	if err != nil {
		return fmt.Sprintf("Config Request with error: %v", err)
	}
	id, err := c.ID()
	if err != nil {
		return fmt.Sprintf("Config Request with config sequence %d and error extracting id: %v", configSeq, err)
	}
	return fmt.Sprintf("Config Request with tx id %q and config sequence %d", id, configSeq)
}
