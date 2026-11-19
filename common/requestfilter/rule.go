/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter

import (
	"github.com/Chaguar/common/common/policies"
	"github.com/Chaguar/orderer/node/protos/comm"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
)

//go:generate counterfeiter -o ./mocks/rule.go . Rule
type Rule interface {
	Verify(request *comm.Request) error
	Update(config FilterConfig) error
}

//go:generate counterfeiter -o ./mocks/structureRule.go . StructureRule
type StructureRule interface {
	VerifyAndClassify(request *comm.Request) (common.HeaderType, error)
	Update(config FilterConfig) error
}

// FilterConfig is an interface that gives the necessary information to verify rules.
//
//go:generate counterfeiter -o ./mocks/filter_config.go . FilterConfig
type FilterConfig interface {
	GetRequestMaxBytes() uint64
	GetClientSignatureVerificationRequired() bool
	GetChannelID() string
	GetPolicyManager() policies.Manager
}

// AcceptRule - always returns nil as a result for Verify
type AcceptRule struct{}

func (a AcceptRule) Verify(request *comm.Request) error {
	return nil
}

func (a AcceptRule) Update(config FilterConfig) error {
	return nil
}
