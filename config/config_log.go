/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"strings"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/reflect/protoreflect"
	"gopkg.in/yaml.v3"
)

// logConfiguration logs the local and shared startup configuration.
// At debug level, it also logs the details of each certificate in the shared config.
func logConfiguration(logger *flogging.FabricLogger, conf *Configuration, role string, blockNumber uint64) {
	logger.Infof("Local config (%s):\n%s", role, yamlForLog(localConfigForLog(conf.LocalConfig.NodeLocalConfig)))

	// Collect the certificate details only when they will be logged.
	var certs map[string]certificateDetails
	if logger.IsEnabledFor(zapcore.DebugLevel) {
		certs = make(map[string]certificateDetails)
	}
	logger.Infof("Shared config (block %d):\n%s", blockNumber, yamlForLog(protoTree(conf.SharedConfig.ProtoReflect(), certs)))
	if certs != nil {
		logger.Debugf("Shared config certificates (block %d):\n%s", blockNumber, yamlForLog(certs))
	}
}

func yamlForLog(value any) string {
	data, err := yaml.Marshal(value)
	if err != nil {
		return fmt.Sprintf("<failed to render configuration: %v>", err)
	}
	return strings.TrimSuffix(string(data), "\n")
}

// localConfigForLog copies the local config with only safe BCCSP fields.
func localConfigForLog(source *NodeLocalConfig) *NodeLocalConfig {
	node := *source
	general := *source.GeneralConfig
	node.GeneralConfig = &general

	safe := &factory.FactoryOpts{Default: general.BCCSP.Default}
	if sw := general.BCCSP.SW; sw != nil {
		safe.SW = &factory.SwOpts{Hash: sw.Hash, Security: sw.Security}
		if sw.FileKeystore != nil {
			safe.SW.FileKeystore = &factory.FileKeystoreOpts{KeyStorePath: sw.FileKeystore.KeyStorePath}
		}
	}
	general.BCCSP = safe

	return &node
}

// protoTree converts protobuf fields for logging, replacing certificates with their SHA-256 hashes.
// If certs is not nil, it adds the details of each certificate to it, keyed by its logged hash.
func protoTree(msg protoreflect.Message, certs map[string]certificateDetails) map[string]any {
	out := make(map[string]any)
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsMap() {
			continue
		}
		if field.IsList() {
			list := msg.Get(field).List()
			values := make([]any, list.Len())
			for j := 0; j < list.Len(); j++ {
				values[j] = protoValue(field, list.Get(j), certs)
			}
			out[string(field.Name())] = values
			continue
		}
		if field.Message() == nil || msg.Has(field) {
			out[string(field.Name())] = protoValue(field, msg.Get(field), certs)
		}
	}
	return out
}

func protoValue(field protoreflect.FieldDescriptor, value protoreflect.Value, certs map[string]certificateDetails) any {
	switch field.Kind() {
	case protoreflect.BytesKind:
		return certificatesForLog(value.Bytes(), certs)
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return protoTree(value.Message(), certs)
	default:
		return value.Interface()
	}
}

// certificatesForLog returns the SHA-256 hash of the DER of each PEM certificate in data,
// ignoring any other content, or redacts data that holds no valid certificate.
// If certs is not nil, it adds the details of each certificate to it, keyed by its hash.
//
// The hash after the "sha256:" prefix matches the output of:
//
//	openssl x509 -in cert.pem -outform der | sha256sum
//
// so a logged certificate can be matched against its file.
func certificatesForLog(data []byte, certs map[string]certificateDetails) string {
	if len(data) == 0 {
		return "<empty>"
	}

	var hashes []string
	for block, rest := pem.Decode(data); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		hash := sha256.Sum256(block.Bytes)
		name := "sha256:" + hex.EncodeToString(hash[:])
		if certs != nil {
			certs[name] = certificateDetails{
				Subject:   cert.Subject.String(),
				Issuer:    cert.Issuer.String(),
				NotBefore: cert.NotBefore.UTC(),
				NotAfter:  cert.NotAfter.UTC(),
			}
		}
		hashes = append(hashes, name)
	}
	if len(hashes) == 0 {
		return "<redacted, not a valid certificate>"
	}
	return strings.Join(hashes, ",")
}

// certificateDetails describes a certificate in the debug-level configuration log.
type certificateDetails struct {
	Subject   string    `yaml:"subject"`
	Issuer    string    `yaml:"issuer"`
	NotBefore time.Time `yaml:"notBefore"`
	NotAfter  time.Time `yaml:"notAfter"`
}
