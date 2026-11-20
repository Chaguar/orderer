/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring_test

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-lib-go/common/metrics/metricsfakes"
	"github.com/stretchr/testify/require"

	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
)

func TestNewRateLimitMetrics(t *testing.T) {
	fakeProvider := &metricsfakes.Provider{}

	fakeCounter := &metricsfakes.Counter{}
	fakeCounter.WithReturns(fakeCounter)

	fakeProvider.NewCounterReturns(fakeCounter)

	m := monitoring.NewRateLimitMetrics(fakeProvider, "router")
	require.NotNil(t, m)
	require.Equal(t, 2, fakeProvider.NewCounterCallCount(),
		"should register admitted and rejected counters")
}

func TestRecordAllow_Admitted(t *testing.T) {
	fakeProvider := &metricsfakes.Provider{}
	admittedCounter := &metricsfakes.Counter{}
	admittedCounter.WithReturns(admittedCounter)
	rejectedCounter := &metricsfakes.Counter{}
	rejectedCounter.WithReturns(rejectedCounter)

	callCount := 0
	fakeProvider.NewCounterStub = func(opts metrics.CounterOpts) metrics.Counter {
		callCount++
		if callCount == 1 {
			return admittedCounter
		}
		return rejectedCounter
	}

	m := monitoring.NewRateLimitMetrics(fakeProvider, "router")
	m.RecordAllow(true)
	m.RecordAllow(true)

	// Two allowed calls → admitted counter Add(1) called twice, rejected counter untouched.
	require.Equal(t, 2, admittedCounter.AddCallCount(),
		"admitted counter must be incremented for each allowed request")
	require.Equal(t, 0, rejectedCounter.AddCallCount(),
		"rejected counter must not be incremented for allowed requests")
}

func TestRecordAllow_Rejected(t *testing.T) {
	fakeProvider := &metricsfakes.Provider{}
	fakeCounter := &metricsfakes.Counter{}
	fakeCounter.WithReturns(fakeCounter)
	fakeProvider.NewCounterReturns(fakeCounter)

	m := monitoring.NewRateLimitMetrics(fakeProvider, "router")
	m.RecordAllow(false)

	// At least one Add call must have been made.
	require.GreaterOrEqual(t, fakeCounter.AddCallCount(), 1,
		"rejected counter must be incremented")
}

func TestRecordAllow_NilMetricsIsNoop(t *testing.T) {
	var m *monitoring.RateLimitMetrics
	require.NotPanics(t, func() {
		m.RecordAllow(true)
		m.RecordAllow(false)
	}, "RecordAllow on nil RateLimitMetrics must not panic")
}
