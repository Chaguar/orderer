/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"testing"

	"github.com/Chaguar/orderer/testutil/tlsgen"
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
	detailsOf := func(cert *x509.Certificate) certificateDetails {
		return certificateDetails{
			Subject:   cert.Subject.String(),
			Issuer:    cert.Issuer.String(),
			NotBefore: cert.NotBefore.UTC(),
			NotAfter:  cert.NotAfter.UTC(),
		}
	}
	firstHash, secondHash := hashOf(first.Cert), hashOf(second.Cert)
	firstDetails := map[string]certificateDetails{firstHash: detailsOf(first.TLSCert)}
	bothDetails := map[string]certificateDetails{firstHash: detailsOf(first.TLSCert), secondHash: detailsOf(second.TLSCert)}

	join := func(parts ...[]byte) []byte {
		return bytes.Join(parts, []byte("\n"))
	}
	notCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not a certificate")})
	const redacted = "<redacted, not a valid certificate>"

	for _, tc := range []struct {
		name        string
		data        []byte
		want        string
		wantDetails map[string]certificateDetails
	}{
		{"empty", nil, "<empty>", map[string]certificateDetails{}},
		{"single certificate", first.Cert, firstHash, firstDetails},
		{"certificate bundle", join(first.Cert, second.Cert), firstHash + "," + secondHash, bothDetails},
		{"same certificate twice", join(first.Cert, first.Cert), firstHash + "," + firstHash, firstDetails},
		{"text around certificates", join([]byte("Bag Attributes\nsubject=CN=x"), first.Cert, []byte("issuer=CN=y"), second.Cert), firstHash + "," + secondHash, bothDetails},
		{"certificate with private key", join(first.Cert, first.Key), firstHash, firstDetails},
		{"private key only", first.Key, redacted, map[string]certificateDetails{}},
		{"invalid certificate block", notCert, redacted, map[string]certificateDetails{}},
		{"not PEM", []byte("plain text"), redacted, map[string]certificateDetails{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			certs := make(map[string]certificateDetails)
			got := certificatesForLog(tc.data, certs)
			require.Equal(t, tc.want, got)
			require.NotContains(t, got, "-----BEGIN")
			require.Equal(t, tc.wantDetails, certs)

			// Without a map, only the hashes are returned.
			require.Equal(t, tc.want, certificatesForLog(tc.data, nil))
		})
	}
}
