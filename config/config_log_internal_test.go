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
	"strings"
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
			certs := make(map[string]*x509.Certificate)
			got := certificatesForLog(tc.data, certs)
			require.Equal(t, tc.want, got)
			require.NotContains(t, got, "-----BEGIN")

			var collected []string
			for name := range certs {
				collected = append(collected, name)
			}
			if strings.HasPrefix(tc.want, "sha256:") {
				require.ElementsMatch(t, strings.Split(tc.want, ","), collected)
			} else {
				require.Empty(t, collected)
			}
		})
	}
}

func TestCertificateDetailsForLog(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	pair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	certs := make(map[string]*x509.Certificate)
	name := certificatesForLog(pair.Cert, certs)

	details := certificateDetailsForLog(certs)
	require.Len(t, details, 1)
	cert := pair.TLSCert
	require.Equal(t, certificateDetails{
		Subject:   cert.Subject.String(),
		Issuer:    cert.Issuer.String(),
		NotBefore: cert.NotBefore.UTC(),
		NotAfter:  cert.NotAfter.UTC(),
	}, details[name])
}
