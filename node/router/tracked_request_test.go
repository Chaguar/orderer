/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"errors"
	"fmt"
	"testing"

	protos "github.com/Chaguar/orderer/node/protos/comm"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Convert a response without an error, and expect SUCCESS with no Info on Broadcast, and no error on Submit.
//  2. Convert a throttled response, and expect SERVICE_UNAVAILABLE on Broadcast, and a "server error (503)" on
//     Submit.
//  3. Convert a bad request, also wrapped in another error, and expect BAD_REQUEST on Broadcast, and a
//     "client error (400)" on Submit.
//  4. Convert any other error, and expect INTERNAL_SERVER_ERROR on Broadcast, and a "server error (500)" on Submit.
//  5. In every case, expect the error text in the Broadcast Info, and after the status on Submit.
func TestResponseConversionClassifiesTheError(t *testing.T) {
	badReq := badRequest(errors.New("request verification error: empty payload field"))
	wrappedBadReq := fmt.Errorf("rejected: %w", badReq)
	serverErr := errors.New("could not establish connection between router and batcher")

	for _, tc := range []struct {
		name            string
		err             error
		wantStatus      common.Status
		wantInfo        string
		wantSubmitError string
	}{
		{
			name:            "success",
			wantStatus:      common.Status_SUCCESS,
			wantInfo:        "",
			wantSubmitError: "",
		},
		{
			name:            "throttled",
			err:             ErrThrottled,
			wantStatus:      common.Status_SERVICE_UNAVAILABLE,
			wantInfo:        "request throttled by router rate limiter",
			wantSubmitError: "server error (503): request throttled by router rate limiter",
		},
		{
			name:            "bad request",
			err:             badReq,
			wantStatus:      common.Status_BAD_REQUEST,
			wantInfo:        "request verification error: empty payload field",
			wantSubmitError: "client error (400): request verification error: empty payload field",
		},
		{
			name:            "wrapped bad request",
			err:             wrappedBadReq,
			wantStatus:      common.Status_BAD_REQUEST,
			wantInfo:        "rejected: request verification error: empty payload field",
			wantSubmitError: "client error (400): rejected: request verification error: empty payload field",
		},
		{
			name:            "server error",
			err:             serverErr,
			wantStatus:      common.Status_INTERNAL_SERVER_ERROR,
			wantInfo:        "could not establish connection between router and batcher",
			wantSubmitError: "server error (500): could not establish connection between router and batcher",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			br := responseToBroadcastResponse(&Response{err: tc.err})
			require.Equal(t, tc.wantStatus, br.Status)
			require.Equal(t, tc.wantInfo, br.Info)

			sr := responseToSubmitResponse(&Response{err: tc.err, reqID: []byte("reqID")})
			require.Equal(t, tc.wantSubmitError, sr.Error)
			require.Equal(t, []byte("reqID"), sr.ReqID)
		})
	}
}

// Scenario:
//  1. Convert a response that carries the batcher's SubmitResponse and the error made from its text.
//  2. Expect the batcher's SubmitResponse, with the error classified as a server error and its trace ID kept.
func TestResponseToSubmitResponseClassifiesABatcherError(t *testing.T) {
	batcherResp := &protos.SubmitResponse{Error: "request already exists", TraceId: []byte("trace")}

	sr := responseToSubmitResponse(&Response{err: errors.New(batcherResp.Error), SubmitResponse: batcherResp})
	require.Equal(t, "server error (500): request already exists", sr.Error)
	require.Equal(t, []byte("trace"), sr.TraceId)
}
