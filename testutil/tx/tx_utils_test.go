/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tx

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"testing"

	"github.com/Chaguar/common/api/msppb"
	"github.com/Chaguar/common/protoutil"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/testutil/tlsgen"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// envSizesToTest returns the sizes from minSize up, and the sizes around 16384, where the data and payload length
// varints grow from two bytes to three. The varints grow from one byte to two below minSize.
func envSizesToTest(minSize int) []int {
	var sizes []int
	for envSize := minSize; envSize <= minSize+300; envSize++ {
		sizes = append(sizes, envSize)
	}
	for envSize := 16384 - 300; envSize <= 16384+300; envSize++ {
		sizes = append(sizes, envSize)
	}
	return sizes
}

func TestPrepareUnsignedEnvelopeSize(t *testing.T) {
	sessionNumber := make([]byte, 16)
	minSize := proto.Size(PrepareUnsignedEnvelope(0, 0, sessionNumber))

	for _, envSize := range envSizesToTest(minSize) {
		size := proto.Size(PrepareUnsignedEnvelope(envSize, envSize, sessionNumber))
		// a size that would need a varint length to grow by a byte cannot be met exactly, and is one byte short.
		require.True(t, size == envSize || size == envSize-1, "requested %d, got %d", envSize, size)
	}
}

func TestPrepareSignedEnvelopeSize(t *testing.T) {
	signer, certBytes := createTestSigner(t)
	sessionNumber := make([]byte, 16)
	builder, err := NewShortModeEnvelopeBuilder(signer, certBytes, "org1")
	require.NoError(t, err)

	for name, prepare := range map[string]func(txNumber int, envSize int) *common.Envelope{
		"certificate": func(txNumber int, envSize int) *common.Envelope {
			return PrepareSignedEnvelopeWithCertificate(txNumber, envSize, sessionNumber, signer, certBytes, "org1")
		},
		"certificate id": func(txNumber int, envSize int) *common.Envelope {
			return PrepareSignedEnvelopeWithCertificateID(txNumber, envSize, sessionNumber, signer, certBytes, "org1")
		},
		"certificate id builder": func(txNumber int, envSize int) *common.Envelope {
			return builder.Envelope(txNumber, envSize, sessionNumber)
		},
	} {
		t.Run(name, func(t *testing.T) {
			// the smallest envelope a request can always be met with: the minimum data, and the largest signature.
			minEnv := prepare(0, 0)
			minSize := proto.Size(minEnv) - len(minEnv.Signature) + maxECDSASignatureSize(signer)
			for _, envSize := range envSizesToTest(minSize) {
				size := proto.Size(prepare(envSize, envSize))
				// a shorter signature and a varint length that cannot grow leave it a few bytes short.
				require.True(t, size <= envSize && size >= envSize-4, "requested %d, got %d", envSize, size)
			}
		})
	}
}

func TestStructuredEnvelopeTxID(t *testing.T) {
	for _, env := range []*common.Envelope{
		CreateStructuredEnvelope([]byte{1}),
		CreateStructuredConfigUpdateEnvelope([]byte{1}),
		CreateStructuredConfigEnvelope([]byte{1}),
		{Payload: CreateStructuredRequest([]byte{1}).Payload},
		{Payload: CreateStructuredConfigUpdateRequest([]byte{1}).Payload},
	} {
		requireValidTxID(t, env.Payload)
	}
}

func TestPreparedEnvelopesUniqueTxID(t *testing.T) {
	signer, certBytes := createTestSigner(t)
	sessionNumber := make([]byte, 16)
	builder, err := NewShortModeEnvelopeBuilder(signer, certBytes, "org1")
	require.NoError(t, err)

	for name, create := range map[string]func() []byte{
		"unsigned": func() []byte { return PrepareUnsignedEnvelope(1, 512, sessionNumber).Payload },
		"signed with certificate": func() []byte {
			return PrepareSignedEnvelopeWithCertificate(1, 512, sessionNumber, signer, certBytes, "org1").Payload
		},
		"signed with certificate id": func() []byte {
			return PrepareSignedEnvelopeWithCertificateID(1, 512, sessionNumber, signer, certBytes, "org1").Payload
		},
		"signed with certificate id builder": func() []byte {
			return builder.Envelope(1, 512, sessionNumber).Payload
		},
		"signed structured": func() []byte {
			return CreateSignedStructuredEnvelope([]byte{1}, signer, certBytes, "org1").Payload
		},
		"signed structured with certificate id": func() []byte {
			return CreateSignedStructuredEnvelopeWithCertID([]byte{1}, signer, certBytes, "org1").Payload
		},
		"config update": func() []byte {
			return protoutil.MarshalOrPanic(CreatePayloadWithConfigUpdate([]byte{1}, certBytes, "org1"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.NotEqual(t, requireValidTxID(t, create()), requireValidTxID(t, create()))
		})
	}
}

// Scenario:
//  1. Create a signing identity and a short mode envelope builder for it.
//  2. Prepare three envelopes with the builder.
//  3. Expect each signature to verify against the public key of the signer's certificate.
//  4. Expect each creator identity to name the certificate by its certificate-ID, and to carry no certificate.
func TestShortModeEnvelopeBuilderSignsWithCertificateID(t *testing.T) {
	signer, certBytes := createTestSigner(t)
	builder, err := NewShortModeEnvelopeBuilder(signer, certBytes, "org1")
	require.NoError(t, err)

	block, _ := pem.Decode(certBytes)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	publicKey, isECDSA := cert.PublicKey.(*ecdsa.PublicKey)
	require.True(t, isECDSA)
	certDigest := sha256.Sum256(cert.Raw)
	certID := hex.EncodeToString(certDigest[:])

	for txNumber := range 3 {
		env := builder.Envelope(txNumber, 512, make([]byte, 16))
		require.NotNil(t, env)

		digest := sha256.Sum256(env.Payload)
		require.True(t, ecdsa.VerifyASN1(publicKey, digest[:], env.Signature))

		payload, err := protoutil.UnmarshalPayload(env.Payload)
		require.NoError(t, err)
		shdr, err := protoutil.UnmarshalSignatureHeader(payload.Header.SignatureHeader)
		require.NoError(t, err)
		creator := &msppb.Identity{}
		require.NoError(t, proto.Unmarshal(shdr.Creator, creator))
		require.Equal(t, "org1", creator.MspId)
		require.Equal(t, certID, creator.GetCertificateId())
		require.Empty(t, creator.GetCertificate())
	}
}

// Scenario:
//  1. Create a short mode envelope builder with content that is not a PEM certificate.
//  2. Expect an error and no builder.
func TestShortModeEnvelopeBuilderRejectsInvalidCertificate(t *testing.T) {
	signer, _ := createTestSigner(t)
	builder, err := NewShortModeEnvelopeBuilder(signer, []byte("not a certificate"), "org1")
	require.Error(t, err)
	require.Nil(t, builder)
}

// requireValidTxID checks that the payload's TxID is the one derived from its signature header, and returns it.
func requireValidTxID(t *testing.T, payloadBytes []byte) string {
	payload, err := protoutil.UnmarshalPayload(payloadBytes)
	require.NoError(t, err)
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	require.NoError(t, err)
	shdr, err := protoutil.UnmarshalSignatureHeader(payload.Header.SignatureHeader)
	require.NoError(t, err)
	require.NoError(t, protoutil.CheckTxID(chdr.TxId, shdr.Nonce, shdr.Creator))
	return chdr.TxId
}

func createTestSigner(t *testing.T) (*crypto.ECDSASigner, []byte) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	keyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)
	block, _ := pem.Decode(keyPair.Key)
	require.NotNil(t, block)
	privateKey, err := CreateECDSAPrivateKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: block.Bytes}))
	require.NoError(t, err)
	return (*crypto.ECDSASigner)(privateKey), keyPair.Cert
}
