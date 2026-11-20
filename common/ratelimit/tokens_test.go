/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ratelimit

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokens_NilLimiterReturnsMaxInt64(t *testing.T) {
	var l *Limiter
	require.Equal(t, int64(math.MaxInt64), l.Tokens(),
		"a nil Limiter (unlimited) must report MaxInt64 tokens")
}

func TestTokens_FullBurstAtStart(t *testing.T) {
	clk := &testClock{}
	clk.set(time.Second)
	const rate, burst = 1000, 5
	l := newWithClock(rate, burst, clk.now)

	// Before any Allow() calls the bucket is full.
	require.Equal(t, int64(burst), l.Tokens(),
		"fresh limiter must report a full bucket")
}

func TestTokens_DropsAfterAdmissions(t *testing.T) {
	clk := &testClock{}
	clk.set(time.Second)
	const rate, burst = 1000, 5
	l := newWithClock(rate, burst, clk.now)

	for i := 0; i < burst; i++ {
		require.True(t, l.Allow())
	}
	// Bucket should now be empty (or 0).
	require.Equal(t, int64(0), l.Tokens(),
		"after draining the burst the bucket must be empty")
}

func TestTokens_RefillsOverTime(t *testing.T) {
	clk := &testClock{}
	clk.set(time.Second)
	const rate, burst = 1000, 5
	interval := time.Second / rate
	l := newWithClock(rate, burst, clk.now)

	// Drain bucket.
	for i := 0; i < burst; i++ {
		l.Allow()
	}
	require.Equal(t, int64(0), l.Tokens())

	// Advance two intervals; two tokens should have refilled.
	clk.advance(2 * interval)
	got := l.Tokens()
	require.GreaterOrEqual(t, got, int64(1),
		"after 2 intervals at least 1 token must be available")
	require.LessOrEqual(t, got, int64(burst),
		"tokens must never exceed burst")
}

func TestTokens_NeverExceedsBurst(t *testing.T) {
	clk := &testClock{}
	clk.set(time.Second)
	const rate, burst = 100, 10
	l := newWithClock(rate, burst, clk.now)

	// Advance a very long time — tokens must still be capped at burst.
	clk.advance(time.Hour)
	require.Equal(t, int64(burst), l.Tokens(),
		"tokens must be capped at the burst capacity even after a long idle period")
}
