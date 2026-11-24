/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaguar/orderer/common/controlevent"
	"github.com/Chaguar/orderer/common/controlevent/mocks"
	"github.com/Chaguar/orderer/node/batcher"
	"github.com/Chaguar/orderer/node/consensus/state"
	"github.com/Chaguar/orderer/testutil"

	"github.com/stretchr/testify/require"
)

func TestBroadcastControlEventQuorumScenarios(t *testing.T) {
	type testCase struct {
		name        string
		f           int
		n           int
		succ        int
		fail        int
		expectError bool
	}

	tests := []testCase{
		{name: "n=4 f=1 all succeed", n: 4, f: 1, succ: 4, fail: 0, expectError: false},
		{name: "n=4 f=1 one fails", n: 4, f: 1, succ: 3, fail: 1, expectError: false},
		{name: "n=4 f=1 two fail (no quorum)", n: 4, f: 1, succ: 2, fail: 2, expectError: true},

		{name: "n=7 f=2 all succeed", n: 7, f: 2, succ: 7, fail: 0, expectError: false},
		{name: "n=7 f=2 two fail", n: 7, f: 2, succ: 5, fail: 2, expectError: false},
		{name: "n=7 f=2 three fail (no quorum)", n: 7, f: 2, succ: 4, fail: 3, expectError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := testutil.CreateLogger(t, 1)

			var senders []controlevent.Sender
			for i := 0; i < tt.succ; i++ {
				s := &mocks.FakeSender{}
				s.SendControlEventReturns(nil)
				senders = append(senders, s)
			}

			for i := 0; i < tt.fail; i++ {
				s := &mocks.FakeSender{}
				s.SendControlEventReturns(errors.New(""))
				senders = append(senders, s)
			}

			b := batcher.NewControlEventBroadcaster(senders, tt.n, tt.f, 10*time.Millisecond, 100*time.Millisecond, logger, context.Background(), func() {})
			defer b.Stop()

			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(300*time.Millisecond, cancel)
			defer cancel()
			err := b.BroadcastControlEvent(state.ControlEvent{}, ctx)
			if tt.expectError {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestControlEventBroadcasterSenderRetry(t *testing.T) {
	logger := testutil.CreateLogger(t, 1)
	var senders []controlevent.Sender

	// two senders succeed
	for i := 0; i < 2; i++ {
		s := &mocks.FakeSender{}
		s.SendControlEventReturns(nil)
		senders = append(senders, s)
	}

	// sender fails once, then succeeds
	sRetry := &mocks.FakeSender{}
	sRetry.SendControlEventReturnsOnCall(1, errors.New(""))
	sRetry.SendControlEventReturnsOnCall(2, nil)
	senders = append(senders, sRetry)

	// sender always fail
	sFail := &mocks.FakeSender{}
	sFail.SendControlEventReturns(errors.New(""))
	senders = append(senders, sFail)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	b := batcher.NewControlEventBroadcaster(senders, 4, 1, 10*time.Millisecond, 100*time.Millisecond, logger, ctx, cancel)
	defer b.Stop()

	err := b.BroadcastControlEvent(state.ControlEvent{}, ctx)
	require.NoError(t, err)
}

// blockingSender returns a sender whose sends block until release is closed, and then succeed.
func blockingSender(release chan struct{}) *mocks.FakeSender {
	s := &mocks.FakeSender{}
	s.SendControlEventStub = func(state.ControlEvent) error {
		<-release
		return nil
	}
	return s
}

func succeedingSender() *mocks.FakeSender {
	s := &mocks.FakeSender{}
	s.SendControlEventReturns(nil)
	return s
}

var errDidNotReturn = errors.New("the broadcast did not return")

// broadcastWithin broadcasts an event, and returns its error, or errDidNotReturn unless it returns within the deadline.
func broadcastWithin(b *batcher.ControlEventBroadcaster, ctx context.Context, deadline time.Duration) error {
	errs := make(chan error, 1)
	go func() { errs <- b.BroadcastControlEvent(state.ControlEvent{}, ctx) }()
	select {
	case err := <-errs:
		return err
	case <-time.After(deadline):
		return errDidNotReturn
	}
}

// Scenario:
//  1. Create a broadcaster to 4 consenters (f = 1), one of which blocks every send until released, as an unreachable one.
//  2. Broadcast an event, and expect it to return once the other 3 accepted it, without waiting for the blocked one.
//  3. Broadcast another, and expect it to return too without trying the blocked consenter again: one send is in flight.
//  4. Release the blocked consenter, broadcast again, and expect that consenter to be sent to again.
func TestBroadcastControlEventDoesNotWaitForASlowConsenter(t *testing.T) {
	release := make(chan struct{})
	releaseSlow := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseSlow)
	slow := blockingSender(release)
	senders := []controlevent.Sender{succeedingSender(), succeedingSender(), succeedingSender(), slow}
	b := batcher.NewControlEventBroadcaster(senders, 4, 1, 10*time.Millisecond, 100*time.Millisecond, testutil.CreateLogger(t, 1), context.Background(), func() {})

	require.NoError(t, broadcastWithin(b, context.Background(), 5*time.Second))
	require.NoError(t, broadcastWithin(b, context.Background(), 5*time.Second))
	require.Never(t, func() bool { return slow.SendControlEventCallCount() > 1 }, 50*time.Millisecond, time.Millisecond)

	releaseSlow()
	require.Eventually(t, func() bool {
		return broadcastWithin(b, context.Background(), 2*time.Second) == nil && slow.SendControlEventCallCount() > 1
	}, 10*time.Second, 10*time.Millisecond)
}

// Scenario:
//  1. Create a broadcaster to 4 consenters (f = 1), with retries from 10 ms up to 10 s: 2 accept, 1 blocks every send,
//     and 1 blocks its first send until released, fails it, and accepts the next.
//  2. Broadcast an event, and expect it not to return for 3 s, while the only missing consenters are busy.
//  3. Release the first send of the third consenter, which fails, and expect the broadcast to return within 500 ms:
//     the retries that found only busy consenters didn't grow the backoff, so the next one comes 10 ms later.
func TestBroadcastControlEventRetriesSoonAfterAStuckSendFails(t *testing.T) {
	stuck, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	var calls atomic.Int32
	failsOnce := &mocks.FakeSender{}
	failsOnce.SendControlEventStub = func(state.ControlEvent) error {
		if calls.Add(1) == 1 {
			<-release
			return errors.New("the stream ended")
		}
		return nil
	}
	senders := []controlevent.Sender{succeedingSender(), succeedingSender(), failsOnce, blockingSender(stuck)}
	b := batcher.NewControlEventBroadcaster(senders, 4, 1, 10*time.Millisecond, 10*time.Second, testutil.CreateLogger(t, 1), context.Background(), func() {})

	returned := make(chan error, 1)
	go func() { returned <- b.BroadcastControlEvent(state.ControlEvent{}, context.Background()) }()
	require.Never(t, func() bool { return len(returned) > 0 }, 3*time.Second, 10*time.Millisecond)

	close(release)
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("the broadcast did not retry soon after the stuck send failed")
	}
	require.Equal(t, int32(2), calls.Load())
}

// Scenario:
//  1. Create a broadcaster to 4 consenters (f = 1): 2 accept, 1 fails twice and then accepts, and 1 blocks every send.
//  2. Broadcast an event, and expect it to return once the failing consenter accepts on its third try, although the
//     blocked consenter's send is still in flight.
func TestBroadcastControlEventRetriesWhileASendIsStuck(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	flaky := &mocks.FakeSender{}
	flaky.SendControlEventReturnsOnCall(0, errors.New("unavailable"))
	flaky.SendControlEventReturnsOnCall(1, errors.New("unavailable"))
	flaky.SendControlEventReturnsOnCall(2, nil)
	senders := []controlevent.Sender{succeedingSender(), succeedingSender(), flaky, blockingSender(release)}
	b := batcher.NewControlEventBroadcaster(senders, 4, 1, 10*time.Millisecond, 100*time.Millisecond, testutil.CreateLogger(t, 1), context.Background(), func() {})

	require.NoError(t, broadcastWithin(b, context.Background(), 5*time.Second))
	require.Equal(t, 3, flaky.SendControlEventCallCount())
}

// Scenario:
//  1. Create a broadcaster to 4 consenters (f = 1), 2 of which block every send, so no quorum can accept an event.
//  2. Broadcast an event, cancel its context, and expect the broadcast to return an error at once.
//  3. Broadcast another, stop the broadcaster, and expect that broadcast to return an error at once too.
func TestBroadcastControlEventEndsWithItsContextOrStop(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	senders := []controlevent.Sender{succeedingSender(), succeedingSender(), blockingSender(release), blockingSender(release)}
	stopCtx, stop := context.WithCancel(context.Background())
	b := batcher.NewControlEventBroadcaster(senders, 4, 1, 10*time.Millisecond, 100*time.Millisecond, testutil.CreateLogger(t, 1), stopCtx, stop)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	err := broadcastWithin(b, ctx, 5*time.Second)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "input context")

	time.AfterFunc(100*time.Millisecond, b.Stop)
	err = broadcastWithin(b, context.Background(), 5*time.Second)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "object context")
}

// Scenario:
//  1. Create a broadcaster to 4 consenters (f = 1) with a 1 ms retry interval: 2 accept after about 1 ms, and 2 block
//     every send, so no quorum can accept an event.
//  2. Broadcast 20 events, each with a 20 ms context, so that retries run while accepted sends are being reported.
//  3. Expect every broadcast to end with an error, and each accepting consenter to be sent every event exactly once.
func TestBroadcastControlEventCountsAConsenterOnce(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	accepting := func() *mocks.FakeSender {
		s := &mocks.FakeSender{}
		s.SendControlEventStub = func(state.ControlEvent) error {
			time.Sleep(time.Millisecond)
			return nil
		}
		return s
	}
	fast1, fast2 := accepting(), accepting()
	senders := []controlevent.Sender{fast1, fast2, blockingSender(release), blockingSender(release)}
	b := batcher.NewControlEventBroadcaster(senders, 4, 1, time.Millisecond, time.Millisecond, testutil.CreateLogger(t, 1), context.Background(), func() {})

	for i := range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := broadcastWithin(b, ctx, 5*time.Second)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, i+1, fast1.SendControlEventCallCount())
		require.Equal(t, i+1, fast2.SendControlEventCallCount())
	}
}
