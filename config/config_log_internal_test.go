/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/testutil/tlsgen"
	"github.com/stretchr/testify/require"
)

func TestCertificatesForLog(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	first, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)
	second, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	hashOf := func(certPEM []byte) string {
		block, _ := pem.Decode(certPEM)
		require.NotNil(t, block)
		hash := sha256.Sum256(block.Bytes)
		return "sha256:" + hex.EncodeToString(hash[:])
	}
	join := func(parts ...[]byte) []byte {
		return bytes.Join(parts, []byte("\n"))
	}
	notCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not a certificate")})
	const redacted = "<redacted, not a valid certificate>"

	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{"empty", nil, "<empty>"},
		{"single certificate", first.Cert, hashOf(first.Cert)},
		{"certificate bundle", join(first.Cert, second.Cert), hashOf(first.Cert) + "," + hashOf(second.Cert)},
		{"text around certificates", join([]byte("Bag Attributes\nsubject=CN=x"), first.Cert, []byte("issuer=CN=y"), second.Cert), hashOf(first.Cert) + "," + hashOf(second.Cert)},
		{"certificate with private key", join(first.Cert, first.Key), hashOf(first.Cert)},
		{"private key only", first.Key, redacted},
		{"invalid certificate block", notCert, redacted},
		{"not PEM", []byte("plain text"), redacted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := certificatesForLog(tc.data)
			require.Equal(t, tc.want, got)
			require.NotContains(t, got, "-----BEGIN")
		})
	}
}
