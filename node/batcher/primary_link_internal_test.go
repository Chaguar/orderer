/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Wait a backoff of 10 ms with a maximum of 1 s, and expect it to wait at least 10 ms and return 20 ms.
//  2. Wait a backoff of 10 ms with a maximum of 15 ms, and expect it to return the maximum.
//  3. Wait a backoff of an hour with the target already moved, and expect it to return at once.
//  4. Wait a backoff of an hour with the context already ended, and expect it to return at once.
func TestWaitBackoff(t *testing.T) {
	notMoved := make(chan struct{})
	start := time.Now()
	require.Equal(t, 20*time.Millisecond, waitBackoff(context.Background(), notMoved, 10*time.Millisecond, time.Second))
	require.GreaterOrEqual(t, time.Since(start), 10*time.Millisecond)
	require.Equal(t, 15*time.Millisecond, waitBackoff(context.Background(), notMoved, 10*time.Millisecond, 15*time.Millisecond))

	moved := make(chan struct{})
	close(moved)
	requireReturnsWithin10s(t, func() { waitBackoff(context.Background(), moved, time.Hour, time.Hour) }, "the backoff did not end when the target moved")

	ended, cancel := context.WithCancel(context.Background())
	cancel()
	requireReturnsWithin10s(t, func() { waitBackoff(ended, notMoved, time.Hour, time.Hour) }, "the backoff did not end with the context")
}

func requireReturnsWithin10s(t *testing.T, f func(), msg string) {
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		f()
	}()
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal(msg)
	}
}
