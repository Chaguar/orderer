/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"time"

	"github.com/Chaguar/orderer/node/comm"
	"github.com/Chaguar/orderer/node/config"
)

func clientConfig(TLSPrivateKeyFile config.RawBytes, TLSCertificateFile config.RawBytes, TlsCACert []config.RawBytes) comm.ClientConfig {
	var tlsCAs [][]byte
	for _, cert := range TlsCACert {
		tlsCAs = append(tlsCAs, cert)
	}

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts:       comm.DefaultKeepaliveOptions,
		SecOpts: comm.SecureOptions{
			Key:               TLSPrivateKeyFile,
			Certificate:       TLSCertificateFile,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}
