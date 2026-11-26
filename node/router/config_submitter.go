/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Chaguar/common/common/channelconfig"
	"github.com/Chaguar/common/protoutil/identity"
	"github.com/Chaguar/orderer/common/policy"
	"github.com/Chaguar/orderer/common/requestfilter"
	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/config/verify"
	"github.com/Chaguar/orderer/node/comm"
	nodeconfig "github.com/Chaguar/orderer/node/config"
	protos "github.com/Chaguar/orderer/node/protos/comm"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// configSubmitter submits the config updates handed to it to the consenter of its party, one at a time, from a
// single goroutine that runs from its creation until Stop.
type configSubmitter struct {
	consensusEndpoint     string
	consensusRootCAs      [][]byte
	tlsCert               []byte
	tlsKey                []byte
	logger                *flogging.FabricLogger
	configRequestsChannel chan *TrackedRequest
	// ctx is cancelled by Stop. It aborts the config update being submitted, and marks the submitter stopped.
	ctx        context.Context
	cancelFunc context.CancelFunc
	// lock orders a send on configRequestsChannel in Forward against its close in Stop.
	lock sync.Mutex
	// done is closed once the goroutine reading configRequestsChannel has answered every config update in it.
	done                 chan struct{}
	bundle               channelconfig.Resources
	verifier             *requestfilter.RulesVerifier
	signer               identity.SignerSerializer
	configUpdateProposer policy.ConfigUpdateProposer
	configRulesVerifier  verify.OrdererRules
	partyID              types.PartyID
	bccsp                bccsp.BCCSP
	metrics              *RouterMetrics
}

// NewConfigSubmitter creates a config submitter and starts the goroutine that submits the config updates
// forwarded to it.
func NewConfigSubmitter(conf *nodeconfig.RouterNodeConfig, logger *flogging.FabricLogger, verifier *requestfilter.RulesVerifier, signer identity.SignerSerializer, configUpdateProposer policy.ConfigUpdateProposer, configRulesVerifier verify.OrdererRules, metrics *RouterMetrics) *configSubmitter {
	var tlsCAsOfConsenter [][]byte
	for _, rawTLSCA := range conf.Consenter.TLSCACerts {
		tlsCAsOfConsenter = append(tlsCAsOfConsenter, rawTLSCA)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cs := &configSubmitter{
		consensusEndpoint:     conf.Consenter.Endpoint,
		consensusRootCAs:      tlsCAsOfConsenter,
		tlsCert:               conf.TLSCertificateFile,
		tlsKey:                conf.TLSPrivateKeyFile,
		logger:                logger,
		configRequestsChannel: make(chan *TrackedRequest, 100),
		ctx:                   ctx,
		cancelFunc:            cancel,
		done:                  make(chan struct{}),
		bundle:                conf.Bundle,
		verifier:              verifier,
		signer:                signer,
		configUpdateProposer:  configUpdateProposer,
		configRulesVerifier:   configRulesVerifier,
		partyID:               conf.PartyID,
		bccsp:                 conf.BCCSP,
		metrics:               metrics,
	}

	go cs.run()

	return cs
}

// Forward forwards the config request from the shard router to the config submitter
// requests channel. A full queue means the path to the consenter is stuck; the
// request is rejected rather than blocking the submitting client indefinitely.
// A request forwarded after Stop is rejected as well.
func (cs *configSubmitter) Forward(tr *TrackedRequest) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if cs.ctx.Err() != nil {
		tr.feedback.reply(Response{err: fmt.Errorf("router is stopping, cannot process request %x", tr.reqID), reqID: tr.reqID})
		return
	}

	select {
	case cs.configRequestsChannel <- tr:
	default:
		cs.logger.Warnf("config requests channel is full, rejecting config request %x", tr.reqID)
		tr.feedback.reply(Response{
			err:   errors.New("server error: config request queue is full, try again later"),
			reqID: tr.reqID,
		})
	}
}

// Stop rejects every config update forwarded from now on and aborts the one being submitted. It returns
// once that update is answered and every config update still in the queue is answered with an error, so that
// the router can deliver these answers before it stops sending them.
// Calling it again only waits for that.
func (cs *configSubmitter) Stop() {
	cs.lock.Lock()
	if cs.ctx.Err() == nil {
		cs.logger.Infof("config submitter is stopping")
		cs.cancelFunc()
		close(cs.configRequestsChannel)
	}
	cs.lock.Unlock()

	<-cs.done
}

// run submits the config updates in the queue one at a time, until Stop closes it. The updates still in the
// queue by then are answered with an error.
func (cs *configSubmitter) run() {
	defer close(cs.done)

	for tr := range cs.configRequestsChannel {
		if cs.ctx.Err() != nil {
			tr.feedback.reply(Response{err: fmt.Errorf("router is stopping, cannot process request %x", tr.reqID), reqID: tr.reqID})
			continue
		}
		if err := cs.forwardRequest(tr); err != nil {
			// Stop cancelled the dial or the SubmitConfig call; any other error is a real failure.
			if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
				cs.logger.Infof("config request %x aborted, config submitter is stopping: %v", tr.reqID, err)
				continue
			}
			cs.logger.Errorf("error forwarding config request to consenter: %v", err)
		}
	}

	cs.logger.Infof("config submitter has stopped")
}

// forwardRequest forwards the config request from the config submitter to the consensus
func (cs *configSubmitter) forwardRequest(tr *TrackedRequest) error {
	if tr == nil {
		return errors.New("received nil tracked request")
	}

	feedback := Response{reqID: tr.reqID}
	var err error

	configRequest, err := cs.configUpdateProposer.ProposeConfigUpdate(tr.request, cs.bundle, cs.signer, cs.verifier, cs.bccsp)
	if err != nil {
		feedback.err = badRequest(fmt.Errorf("error in verification and proposing update: %w", err))
		tr.feedback.reply(feedback)
		return err
	}

	env := &common.Envelope{Payload: configRequest.Payload, Signature: configRequest.Signature}
	if err = cs.configRulesVerifier.ValidateNewConfig(env, cs.bccsp, cs.partyID); err != nil {
		feedback.err = badRequest(fmt.Errorf("error in validating config rules: %w", err))
		tr.feedback.reply(feedback)
		return err
	}

	if err = cs.configRulesVerifier.ValidateTransition(cs.bundle, env, cs.bccsp); err != nil {
		feedback.err = badRequest(fmt.Errorf("error in validating config transition rules: %w", err))
		tr.feedback.reply(feedback)
		return err
	}

	var resp *protos.SubmitResponse
	resp, err = cs.submitConfigRequestToConsensus(tr.request)
	if resp != nil {
		// The consenter does not know the router's reqID, which stream clients correlate by.
		resp.ReqID = tr.reqID
	}
	feedback.SubmitResponse = resp
	if err != nil {
		feedback.err = fmt.Errorf("error forwarding config request to consenter: %w", err)
	} else {
		cs.metrics.forwardedToConsenter.Add(1)
		if resp.Error != "" {
			feedback.err = errors.New(resp.Error)
		}
	}

	tr.feedback.reply(feedback)
	return err
}

func (cs *configSubmitter) submitConfigRequestToConsensus(req *protos.Request) (*protos.SubmitResponse, error) {
	conn, err := cs.connectToConsenter()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cl := protos.NewConsensusClient(conn)

	resp, err := cl.SubmitConfig(cs.ctx, req)

	return resp, err
}

func (cs *configSubmitter) connectToConsenter() (*grpc.ClientConn, error) {
	conn, err := cs.tryToConnect()
	if err == nil {
		return conn, err
	}

	// repeatedly try to connect, with backoff
	interval := minRetryInterval
	numOfRetries := 1
	for {
		select {
		case <-cs.ctx.Done():
			return nil, fmt.Errorf("reconnection to consensus %s aborted, because context is done and configSubmitter stopped: %w", cs.consensusEndpoint, cs.ctx.Err())
		case <-time.After(interval):
			cs.logger.Debugf("Retry attempt #%d", numOfRetries)
			numOfRetries++
			conn, err := cs.tryToConnect()
			if err != nil {
				interval = min(interval*2, maxRetryInterval)
				// a dial aborted by Stop is not a failure; the next iteration returns on ctx.Done.
				if cs.ctx.Err() == nil {
					cs.logger.Errorf("Reconnection to consensus failed: %v, trying again in: %s", err, interval)
				}
				continue
			} else {
				cs.logger.Debugf("Reconnection to consensus %s succeeded", cs.consensusEndpoint)
				return conn, nil
			}
		}
	}
}

func (cs *configSubmitter) tryToConnect() (*grpc.ClientConn, error) {
	cc := comm.ClientConfig{
		AsyncConnect: false,
		KaOpts:       comm.DefaultKeepaliveOptions,
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			ServerRootCAs:     cs.consensusRootCAs,
			Key:               cs.tlsKey,
			Certificate:       cs.tlsCert,
			RequireClientCert: true,
		},
		DialTimeout: time.Second * 20,
	}

	conn, err := cc.DialContext(cs.ctx, cs.consensusEndpoint)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
