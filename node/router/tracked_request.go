/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"errors"
	"fmt"

	protos "github.com/Chaguar/orderer/node/protos/comm"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
)

// ErrThrottled is the sentinel error carried by a Response when a request is
// rejected by the router's rate limiter.
var ErrThrottled = errors.New("request throttled by router rate limiter")

// errContextDone is carried by a Response when the client gave up on a Submit before the answer arrived.
// It is not counted in router_requests_rejected: the router refused nothing.
var errContextDone = errors.New("context done before receiving response")

// badRequestError is an error for a request that is itself wrong.
type badRequestError struct {
	err error
}

func (e *badRequestError) Error() string {
	return e.err.Error()
}

func (e *badRequestError) Unwrap() error {
	return e.err
}

func badRequest(err error) error {
	return &badRequestError{err: err}
}

type TrackedRequest struct {
	request  *protos.Request // the request to be forward to the batcher
	feedback *clientFeedback // the feedback path of the submitting client
	reqID    []byte          // identifier used to disseminate requests across shards in the router
	trace    []byte          // used to trace the request in the router. If nil, the request is untraced, and a response is sent no later than after forwarding to the batcher.
}

type Response struct {
	err   error
	reqID []byte
	*protos.SubmitResponse
}

func (resp *Response) GetResponseError() error {
	return resp.err
}

// responseToSubmitResponse converts a response to the answer of the Submit API. A SubmitResponse has no status
// field, so the status the Broadcast API would answer with is put in front of the error text, as in
// "client error (400): request verification error: empty payload field".
func responseToSubmitResponse(response *Response) *protos.SubmitResponse {
	resp := response.SubmitResponse
	if resp == nil {
		resp = &protos.SubmitResponse{ReqID: response.reqID}
	}
	if response.err != nil {
		status := errorStatus(response.err)
		class := "server"
		if status == common.Status_BAD_REQUEST {
			class = "client"
		}
		resp.Error = fmt.Sprintf("%s error (%d): %s", class, status, response.err)
	}
	return resp
}

// responseToBroadcastResponse converts a response to the answer of the Broadcast API, with the error text in Info.
func responseToBroadcastResponse(response *Response) *orderer.BroadcastResponse {
	if response.err == nil {
		return &orderer.BroadcastResponse{Status: common.Status_SUCCESS}
	}
	return &orderer.BroadcastResponse{Status: errorStatus(response.err), Info: response.err.Error()}
}

// errorStatus classifies an error the router answers a client with: SERVICE_UNAVAILABLE for a throttled request
// (common.Status has no RESOURCE_EXHAUSTED), BAD_REQUEST for a request that is itself wrong, and
// INTERNAL_SERVER_ERROR for every other error. The same status sets the code a rejected request is counted under
// in router_requests_rejected.
func errorStatus(err error) common.Status {
	var badReq *badRequestError
	switch {
	case errors.Is(err, ErrThrottled):
		return common.Status_SERVICE_UNAVAILABLE
	case errors.As(err, &badReq):
		return common.Status_BAD_REQUEST
	default:
		return common.Status_INTERNAL_SERVER_ERROR
	}
}
