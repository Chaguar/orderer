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
	logYAML(logger, logger.Infof, fmt.Sprintf("Local config (%s)", role), localConfigForLog(conf.LocalConfig.NodeLocalConfig))

	certs := make(map[string]*x509.Certificate)
	logYAML(logger, logger.Infof, fmt.Sprintf("Shared config (block %d)", blockNumber), protoTree(conf.SharedConfig.ProtoReflect(), certs))

	if logger.IsEnabledFor(zapcore.DebugLevel) {
		logYAML(logger, logger.Debugf, fmt.Sprintf("Shared config certificates (block %d)", blockNumber), certificateDetailsForLog(certs))
	}
}

func logYAML(logger *flogging.FabricLogger, logf func(string, ...any), title string, value any) {
	data, err := yaml.Marshal(value)
	if err != nil {
		logger.Warnf("%s: failed to render configuration: %v", title, err)
		return
	}
	logf("%s:\n%s", title, strings.TrimSuffix(string(data), "\n"))
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
// It adds each certificate to certs, keyed by its logged hash.
func protoTree(msg protoreflect.Message, certs map[string]*x509.Certificate) map[string]any {
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

func protoValue(field protoreflect.FieldDescriptor, value protoreflect.Value, certs map[string]*x509.Certificate) any {
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
// It adds each certificate to certs, keyed by its hash.
//
// The hash after the "sha256:" prefix matches the output of:
//
//	openssl x509 -in cert.pem -outform der | sha256sum
//
// so a logged certificate can be matched against its file.
func certificatesForLog(data []byte, certs map[string]*x509.Certificate) string {
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
		certs[name] = cert
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

// certificateDetailsForLog returns the details of each certificate, keyed by its logged hash.
func certificateDetailsForLog(certs map[string]*x509.Certificate) map[string]certificateDetails {
	details := make(map[string]certificateDetails, len(certs))
	for name, cert := range certs {
		details[name] = certificateDetails{
			Subject:   cert.Subject.String(),
			Issuer:    cert.Issuer.String(),
			NotBefore: cert.NotBefore.UTC(),
			NotAfter:  cert.NotAfter.UTC(),
		}
	}
	return details
}
