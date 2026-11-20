/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"github.com/hyperledger/fabric-lib-go/common/metrics"
)

// RateLimitMetrics holds Prometheus counters for rate-limiter admission
// decisions. It is intended to be embedded (or referenced) in components
// that perform throttling (e.g. the router) so that operators can observe
// how often requests are admitted versus rejected by the global GCRA limiter.
//
// All fields are safe for concurrent use once initialised by NewRateLimitMetrics.
type RateLimitMetrics struct {
	// Admitted counts requests that passed the rate-limiter (Allow() == true).
	Admitted metrics.Counter
	// Rejected counts requests that were dropped by the rate-limiter (Allow() == false).
	Rejected metrics.Counter
}

var (
	rateLimitAdmitted = metrics.CounterOpts{
		Name:         "ratelimit_admitted_total",
		Help:         "Total number of requests admitted by the rate limiter.",
		LabelNames:   []string{"component"},
		StatsdFormat: "%{#fqname}.%{component}",
	}

	rateLimitRejected = metrics.CounterOpts{
		Name:         "ratelimit_rejected_total",
		Help:         "Total number of requests rejected by the rate limiter.",
		LabelNames:   []string{"component"},
		StatsdFormat: "%{#fqname}.%{component}",
	}
)

// NewRateLimitMetrics registers and returns a RateLimitMetrics bound to the
// given provider. component is a short label that identifies which part of the
// system the metrics belong to (e.g. "router", "batcher").
func NewRateLimitMetrics(provider metrics.Provider, component string) *RateLimitMetrics {
	return &RateLimitMetrics{
		Admitted: provider.NewCounter(rateLimitAdmitted).With(component),
		Rejected: provider.NewCounter(rateLimitRejected).With(component),
	}
}

// RecordAllow increments the appropriate counter based on whether the request
// was admitted (allowed == true) or rejected (allowed == false).
// It is a no-op when m is nil, so callers can safely pass a nil pointer when
// metrics collection is disabled.
func (m *RateLimitMetrics) RecordAllow(allowed bool) {
	if m == nil {
		return
	}
	if allowed {
		m.Admitted.Add(1)
	} else {
		m.Rejected.Add(1)
	}
}
