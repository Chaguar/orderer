/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	"github.com/Chaguar/common/api/msppb"
	"github.com/Chaguar/common/api/ordererpb"
	"github.com/Chaguar/common/common/channelconfig"
	fabmsp "github.com/Chaguar/common/msp"
	arma_types "github.com/Chaguar/orderer/common/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"google.golang.org/protobuf/proto"
)

// validatePartyCertificates validates TLS CA and CA certificates, and verifies TLS and signing certificates for the party.
func validatePartyCertificates(party *ordererpb.PartyConfig, ignoreExpiration bool) error {
	if len(party.TLSCACerts) == 0 {
		return errors.New("empty TLS CA certificates")
	}

	if len(party.CACerts) == 0 {
		return errors.New("empty signing CA certificates")
	}

	tlsPool := x509.NewCertPool()
	for _, raw := range party.TLSCACerts {
		cert, err := validateCACert(raw)
		if err != nil {
			return fmt.Errorf("invalid TLS CA certificate: %w", err)
		}
		tlsPool.AddCert(cert)
	}

	tlsOpts := x509.VerifyOptions{
		Roots:         tlsPool,
		Intermediates: nil, // TODO: add intermediate TLS CA certs when added to the party config
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
	}

	signPool := x509.NewCertPool()
	for _, raw := range party.CACerts {
		cert, err := validateCACert(raw)
		if err != nil {
			return fmt.Errorf("invalid CA certificate: %w", err)
		}
		signPool.AddCert(cert)
	}

	signOpts := x509.VerifyOptions{
		Roots:         signPool,
		Intermediates: nil, // TODO: add intermediate CA certs when added to the party config
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageAny,
		},
	}

	if party.RouterConfig != nil {
		if err := verifyCert(party.RouterConfig.GetNodeConfig().GetServerTlsCert(), tlsOpts, ignoreExpiration); err != nil {
			return fmt.Errorf("router TLS certificate validation failed: %w", err)
		}
		if err := verifyCert(party.RouterConfig.GetNodeConfig().GetSignCert(), signOpts, ignoreExpiration); err != nil {
			return fmt.Errorf("router signing certificate validation failed: %w", err)
		}
	} else {
		return errors.New("router config is nil")
	}

	if party.AssemblerConfig != nil {
		if err := verifyCert(party.AssemblerConfig.GetNodeConfig().GetServerTlsCert(), tlsOpts, ignoreExpiration); err != nil {
			return fmt.Errorf("assembler TLS certificate validation failed: %w", err)
		}
		if err := verifyCert(party.AssemblerConfig.GetNodeConfig().GetSignCert(), signOpts, ignoreExpiration); err != nil {
			return fmt.Errorf("assembler signing certificate validation failed: %w", err)
		}
	} else {
		return errors.New("assembler config is nil")
	}

	if party.ConsenterConfig != nil {
		if err := verifyCert(party.ConsenterConfig.GetNodeConfig().GetServerTlsCert(), tlsOpts, ignoreExpiration); err != nil {
			return fmt.Errorf("consenter TLS certificate validation failed: %w", err)
		}
		if err := verifyCert(party.ConsenterConfig.GetNodeConfig().GetSignCert(), signOpts, ignoreExpiration); err != nil {
			return fmt.Errorf("consenter signing certificate validation failed: %w", err)
		}
	} else {
		return errors.New("consenter config is nil")
	}

	for _, b := range party.BatchersConfig {
		if b == nil {
			return errors.New("batcher config is nil")
		}

		if err := verifyCert(b.GetNodeConfig().GetServerTlsCert(), tlsOpts, ignoreExpiration); err != nil {
			return fmt.Errorf("batcher TLS certificate validation failed: %w", err)
		}
		if err := verifyCert(b.GetNodeConfig().GetSignCert(), signOpts, ignoreExpiration); err != nil {
			return fmt.Errorf("batcher signing certificate validation failed: %w", err)
		}

	}

	return nil
}

// validateUniqueSigningCerts verifies that signing certificates have unique public keys across all nodes.
func validateUniqueSigningCerts(parties []*ordererpb.PartyConfig) error {
	type nodeSignCert struct {
		node arma_types.NodeIdentity
		cert []byte
	}

	signCertOwners := make(map[string]arma_types.NodeIdentity)
	for _, party := range parties {
		if party == nil {
			return errors.New("party config is nil")
		}

		partyID := arma_types.PartyID(party.PartyID)
		signCerts := []nodeSignCert{
			{node: arma_types.NewRouterIdentity(partyID), cert: party.GetRouterConfig().GetNodeConfig().GetSignCert()},
			{node: arma_types.NewAssemblerIdentity(partyID), cert: party.GetAssemblerConfig().GetNodeConfig().GetSignCert()},
			{node: arma_types.NewConsenterIdentity(partyID), cert: party.GetConsenterConfig().GetNodeConfig().GetSignCert()},
		}
		for _, b := range party.BatchersConfig {
			signCerts = append(signCerts, nodeSignCert{
				node: arma_types.NewBatcherIdentity(partyID, arma_types.ShardID(b.GetShardId())),
				cert: b.GetNodeConfig().GetSignCert(),
			})
		}

		for _, sc := range signCerts {
			cert, err := parseCertificateFromBytes(sc.cert)
			if err != nil {
				return fmt.Errorf("invalid signing certificate of %s: %w", sc.node, err)
			}

			certKey := string(cert.RawSubjectPublicKeyInfo)
			if owner, exists := signCertOwners[certKey]; exists {
				return fmt.Errorf("%s and %s share the same signing key", owner, sc.node)
			}
			signCertOwners[certKey] = sc.node
		}
	}

	return nil
}

// validateKnownCerts verifies the known certificates of every orderer and application organization MSP in config:
// each must be a PEM-encoded certificate that chains to the MSP's root or intermediate CA certificates, with
// expiration enforced. A known certificate that prev already holds in the same MSP is skipped, so only added and
// changed known certificates are checked; a nil prev checks them all. It is skipped also when the MSP's CA
// certificates changed: the MSP setup already rejects an ECDSA-signed known certificate that does not chain to them.
func validateKnownCerts(config, prev *common.Config) error {
	mspConfigs, err := orgMSPConfigs(config)
	if err != nil {
		return err
	}
	prevMSPConfigs, err := orgMSPConfigs(prev)
	if err != nil {
		return err
	}

	for mspID, mspConfig := range mspConfigs {
		opts := x509.VerifyOptions{
			Roots:         x509.NewCertPool(),
			Intermediates: x509.NewCertPool(),
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		}
		for _, cert := range mspConfig.RootCerts {
			opts.Roots.AppendCertsFromPEM(cert)
		}
		for _, cert := range mspConfig.IntermediateCerts {
			opts.Intermediates.AppendCertsFromPEM(cert)
		}

		prevMSPConfig := prevMSPConfigs[mspID]
		prevKnownCerts := make(map[string]struct{}, len(prevMSPConfig.GetKnownCerts()))
		for _, cert := range prevMSPConfig.GetKnownCerts() {
			prevKnownCerts[string(cert)] = struct{}{}
		}

		for _, cert := range mspConfig.KnownCerts {
			if _, unchanged := prevKnownCerts[string(cert)]; unchanged {
				continue
			}
			if err := verifyCert(cert, opts, false); err != nil {
				return fmt.Errorf("invalid known certificate of MSP %s: %w", mspID, err)
			}
		}
	}

	return nil
}

// orgMSPConfigs returns the Fabric MSP config of every orderer and application organization, keyed by MSP ID.
// An MSP ID defined in both groups has the same config in each, as channelconfig enforces.
func orgMSPConfigs(config *common.Config) (map[string]*msppb.FabricMSPConfig, error) {
	mspConfigs := make(map[string]*msppb.FabricMSPConfig)
	for _, groupKey := range []string{channelconfig.OrdererGroupKey, channelconfig.ApplicationGroupKey} {
		for orgName, org := range config.GetChannelGroup().GetGroups()[groupKey].GetGroups() {
			mspConfig := &msp.MSPConfig{}
			if err := proto.Unmarshal(org.GetValues()[channelconfig.MSPKey].GetValue(), mspConfig); err != nil {
				return nil, fmt.Errorf("failed to unmarshal MSP config of organization %s: %w", orgName, err)
			}
			if mspConfig.Type != int32(fabmsp.FABRIC) {
				continue
			}

			fabricConfig := &msppb.FabricMSPConfig{}
			if err := proto.Unmarshal(mspConfig.Config, fabricConfig); err != nil {
				return nil, fmt.Errorf("failed to unmarshal Fabric MSP config of organization %s: %w", orgName, err)
			}
			mspConfigs[fabricConfig.Name] = fabricConfig
		}
	}

	return mspConfigs, nil
}

// validateCACert decodes PEM cert, parses and ensures it is a CA.
func validateCACert(raw []byte) (*x509.Certificate, error) {
	if len(raw) == 0 {
		return nil, errors.New("certificate is empty")
	}
	cert, err := parseCertificateFromBytes(raw)
	if err != nil {
		return nil, err
	}

	if !cert.IsCA {
		return nil, fmt.Errorf("certificate %q is not a CA", cert.Subject.String())
	}

	return cert, nil
}

// verifyCert decodes PEM cert, parses and verifies the certificate chain, with optional expiration handling.
func verifyCert(raw []byte, opts x509.VerifyOptions, ignoreExpiration bool) error {
	if len(raw) == 0 {
		return errors.New("certificate is empty")
	}

	cert, err := parseCertificateFromBytes(raw)
	if err != nil {
		return err
	}

	if ignoreExpiration {
		opts.CurrentTime = cert.NotBefore
	}

	if _, err := cert.Verify(opts); err != nil {
		return fmt.Errorf("verifying certificate with serial number %d: %w", cert.SerialNumber, err)
	}

	if ignoreExpiration && time.Now().Before(cert.NotBefore) {
		return fmt.Errorf("certificate with serial number %d is not yet valid", cert.SerialNumber)
	}

	return nil
}

func parseCertificateFromBytes(cert []byte) (*x509.Certificate, error) {
	pemBlock, _ := pem.Decode(cert)
	if pemBlock == nil {
		return nil, errors.New("no PEM data found in certificate")
	}

	if pemBlock.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("expected PEM block of type CERTIFICATE, but found %s", pemBlock.Type)
	}

	certificate, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate from ASN1 structure: %w", err)
	}

	return certificate, nil
}
