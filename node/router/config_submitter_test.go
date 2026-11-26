/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Chaguar/common/protoutil/identity/mocks"
	"github.com/Chaguar/orderer/common/operations"
	policyMocks "github.com/Chaguar/orderer/common/policy/mocks"
	"github.com/Chaguar/orderer/common/types"
	ordererRulesMocks "github.com/Chaguar/orderer/config/verify/mocks"
	"github.com/Chaguar/orderer/node/comm/tlsgen"
	nodeconfig "github.com/Chaguar/orderer/node/config"
	protos "github.com/Chaguar/orderer/node/protos/comm"
	"github.com/Chaguar/orderer/testutil"
	"github.com/Chaguar/orderer/testutil/stub"
	"github.com/Chaguar/orderer/testutil/tx"
	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/stretchr/testify/require"
)

type configSubmitTestSetup struct {
	stubConsenter   *stub.StubConsenter
	configSubmitter *configSubmitter
}

func (s configSubmitTestSetup) Start() {
	s.stubConsenter.Start()
}

func (s configSubmitTestSetup) Stop() {
	s.stubConsenter.Stop()
	s.configSubmitter.Stop()
}

func createConfigSubmitTestSetup(t *testing.T) configSubmitTestSetup {
	logger := testutil.CreateLogger(t, 0)
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	stubConsenter := stub.NewStubConsenter(t, ca, types.PartyID(1))

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	bundle, verifier := createTestBundleAndVerifier()
	fakeSigner := &mocks.SignerSerializer{}

	mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
	req := &protos.Request{}
	mockConfigUpdateProposer.ProposeConfigUpdateReturns(req, nil)

	mockConfigRulesVerifier := &ordererRulesMocks.FakeOrdererRules{}
	mockConfigRulesVerifier.ValidateNewConfigReturns(nil)
	mockConfigRulesVerifier.ValidateTransitionReturns(nil)

	conf := &nodeconfig.RouterNodeConfig{
		Consenter: nodeconfig.ConsenterInfo{
			Endpoint:   stubConsenter.GetConsenterEndpoint(),
			TLSCACerts: []nodeconfig.RawBytes{ca.CertBytes()},
		},
		TLSCertificateFile: ckp.Cert,
		TLSPrivateKeyFile:  ckp.Key,
		Bundle:             bundle,
		PartyID:            types.PartyID(1),
		BCCSP:              factory.GetDefault(),
		Metrics:            &operations.Metrics{Provider: "disabled"},
	}

	configSubmitter := NewConfigSubmitter(conf, logger, verifier, fakeSigner, mockConfigUpdateProposer, mockConfigRulesVerifier, NewRouterMetrics(conf, logger))
	t.Cleanup(configSubmitter.Stop)

	return configSubmitTestSetup{configSubmitter: configSubmitter, stubConsenter: &stubConsenter}
}

func TestConfigSubmitterForward(t *testing.T) {
	setup := createConfigSubmitTestSetup(t)
	setup.Start()
	defer setup.Stop()
	configSubmitter := setup.configSubmitter
	stubConsenter := setup.stubConsenter

	req := tx.CreateStructuredRequest([]byte("data"))
	feedbackChan := make(chan Response, 1)
	configSubmitter.Forward(CreateTrackedRequest(req, feedbackChan, nil, nil))

	resp := <-feedbackChan
	require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")

	require.Eventually(t, func() bool {
		return stubConsenter.ReceivedMessageCount() == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestConfigSubmitterMultipleForward(t *testing.T) {
	setup := createConfigSubmitTestSetup(t)
	setup.Start()
	defer setup.Stop()
	configSubmitter := setup.configSubmitter
	stubConsenter := setup.stubConsenter

	numOfRequests := 10
	for i := 0; i < numOfRequests; i++ {
		req := tx.CreateStructuredRequest([]byte("data"))
		feedbackChan := make(chan Response, 1)
		configSubmitter.Forward(CreateTrackedRequest(req, feedbackChan, nil, nil))

		resp := <-feedbackChan
		require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")
	}

	require.Eventually(t, func() bool {
		return stubConsenter.ReceivedMessageCount() == uint32(numOfRequests)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestForwardWithConsensusRestart(t *testing.T) {
	setup := createConfigSubmitTestSetup(t)
	setup.Start()
	defer setup.Stop()

	configSubmitter := setup.configSubmitter
	stubConsenter := setup.stubConsenter

	req := tx.CreateStructuredRequest([]byte("data"))
	feedbackChan := make(chan Response, 1)

	// submit one request, and wait for the response
	configSubmitter.Forward(CreateTrackedRequest(req, feedbackChan, nil, nil))
	resp := <-feedbackChan
	require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")

	// stop the consenter and wait until it is down
	stubConsenter.Stop()
	time.Sleep(250 * time.Millisecond)

	// forward another request
	configSubmitter.Forward(CreateTrackedRequest(req, feedbackChan, nil, nil))

	// wait and restart the consenter
	time.Sleep(250 * time.Millisecond)
	stubConsenter.Restart()

	resp = <-feedbackChan
	require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")

	require.Eventually(t, func() bool {
		return stubConsenter.ReceivedMessageCount() == uint32(2)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestConfigSubmitterReconnectionAbort(t *testing.T) {
	setup := createConfigSubmitTestSetup(t)
	setup.Start()

	configSubmitter := setup.configSubmitter
	stubConsenter := setup.stubConsenter
	req := tx.CreateStructuredRequest([]byte("data"))
	feedbackChan := make(chan Response, 1)

	// submit one request, and wait for the response
	configSubmitter.Forward(CreateTrackedRequest(req, feedbackChan, nil, nil))
	resp := <-feedbackChan
	require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")

	// stop the consenter and wait until it is down
	stubConsenter.Stop()
	time.Sleep(250 * time.Millisecond)

	// forward a request and stop the config submitter
	errChan := make(chan error)
	go func() {
		errChan <- configSubmitter.forwardRequest(CreateTrackedRequest(req, feedbackChan, nil, nil))
	}()
	configSubmitter.Stop()

	err := <-errChan
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, fmt.Sprintf("reconnection to consensus %s aborted, because context is done and configSubmitter stopped", configSubmitter.consensusEndpoint))
}

// Scenario:
//  1. Forward a config update that the consenter rejects, and expect it not to be marked as a bad request (so 500).
//  2. Forward a config update that fails validation, and expect it to be marked as a bad request (400).
func TestConfigSubmitterRejectionCodes(t *testing.T) {
	setup := createConfigSubmitTestSetup(t)
	setup.Start()
	defer setup.Stop()
	configSubmitter := setup.configSubmitter
	req := tx.CreateStructuredRequest([]byte("data"))
	feedbackChan := make(chan Response, 1)

	// the stub consenter rejects every config update
	configSubmitter.Forward(CreateTrackedRequest(req, feedbackChan, nil, nil))
	resp := <-feedbackChan
	var badReq *badRequestError
	require.EqualError(t, resp.GetResponseError(), "dummy submit config")
	require.NotErrorAs(t, resp.GetResponseError(), &badReq)

	configSubmitter.configRulesVerifier.(*ordererRulesMocks.FakeOrdererRules).ValidateNewConfigReturns(errors.New("invalid config"))
	configSubmitter.Forward(CreateTrackedRequest(req, feedbackChan, nil, nil))
	resp = <-feedbackChan
	require.ErrorAs(t, resp.GetResponseError(), &badReq)
	require.EqualError(t, resp.GetResponseError(), "error in validating config rules: invalid config")
}

// Scenario:
//  1. Create a config submitter with a queue of one and without its goroutine, so
//     that nothing drains the queue.
//  2. Forward a request, filling the queue, and expect no response.
//  3. Forward a second request and expect it to be rejected within a timeout,
//     rather than blocking.
func TestConfigSubmitterForwardRejectsWhenQueueIsFull(t *testing.T) {
	cs := &configSubmitter{
		ctx:                   context.Background(),
		logger:                testutil.CreateLogger(t, 0),
		configRequestsChannel: make(chan *TrackedRequest, 1),
	}

	feedbackChan := make(chan Response, 2)
	req := tx.CreateStructuredRequest([]byte("config"))

	cs.Forward(CreateTrackedRequest(req, feedbackChan, []byte("req1"), nil))
	require.Empty(t, feedbackChan)

	go cs.Forward(CreateTrackedRequest(req, feedbackChan, []byte("req2"), nil))

	select {
	case resp := <-feedbackChan:
		require.ErrorContains(t, resp.GetResponseError(), "config request queue is full")
		require.Equal(t, []byte("req2"), resp.reqID)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "Forward did not reject a config request when the queue was full")
	}
}

// Scenario:
//  1. Start a config submitter and its consenter, forward a config update, and expect the consenter's answer.
//  2. Stop the consenter, so that a config update waits while the submitter retries the connection.
//  3. Forward three config updates, and wait until the first is being submitted and the other two are queued.
//  4. Stop the config submitter, and expect all three to be answered by the time Stop returns: the queued two
//     with "router is stopping", and the one being submitted with the aborted reconnection.
//  5. Forward a fifth config update, and expect it to be answered at once with "router is stopping".
//  6. Stop the config submitter again, and expect it to return.
func TestConfigSubmitterStopAnswersEveryConfigUpdate(t *testing.T) {
	setup := createConfigSubmitTestSetup(t)
	setup.stubConsenter.Start()
	configSubmitter := setup.configSubmitter

	req := tx.CreateStructuredRequest([]byte("data"))
	feedbackChan := make(chan Response, 4)
	configSubmitter.Forward(CreateTrackedRequest(req, feedbackChan, []byte("req0"), nil))
	resp := <-feedbackChan
	require.Equal(t, "dummy submit config", resp.SubmitResponse.Error)

	setup.stubConsenter.Stop()

	for _, reqID := range []string{"req1", "req2", "req3"} {
		configSubmitter.Forward(CreateTrackedRequest(req, feedbackChan, []byte(reqID), nil))
	}
	// req1 is being submitted once the proposer has been called for it, which forwardRequest does first.
	proposer := configSubmitter.configUpdateProposer.(*policyMocks.FakeConfigUpdateProposer)
	require.Eventually(t, func() bool {
		return proposer.ProposeConfigUpdateCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)
	require.Len(t, configSubmitter.configRequestsChannel, 2)

	configSubmitter.Stop()

	require.Len(t, feedbackChan, 3)
	responses := make(map[string]error)
	for range 3 {
		resp := <-feedbackChan
		responses[string(resp.reqID)] = resp.GetResponseError()
	}
	require.ErrorIs(t, responses["req1"], context.Canceled)
	require.ErrorContains(t, responses["req1"], "aborted, because context is done and configSubmitter stopped")
	require.ErrorContains(t, responses["req2"], "router is stopping")
	require.ErrorContains(t, responses["req3"], "router is stopping")

	configSubmitter.Forward(CreateTrackedRequest(req, feedbackChan, []byte("req4"), nil))
	require.Len(t, feedbackChan, 1)
	resp = <-feedbackChan
	require.Equal(t, []byte("req4"), resp.reqID)
	require.ErrorContains(t, resp.GetResponseError(), "router is stopping")

	configSubmitter.Stop()
}
