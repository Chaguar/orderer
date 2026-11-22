/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/Chaguar/orderer/common/controlevent"
	"github.com/Chaguar/orderer/common/controlevent/mocks"
	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/assembler"
	"github.com/Chaguar/orderer/node/consensus/state"
	"github.com/Chaguar/orderer/node/crypto"
	"github.com/Chaguar/orderer/testutil"
	"github.com/stretchr/testify/require"
)

const (
	reporterParty     = types.PartyID(2)
	reporterConfigSeq = types.ConfigSequence(5)
)

// Scenario:
//  1. Create a reporter of party 2 with step 100 and three consenters, whose ledger ends with decision 150.
//  2. Start it.
//  3. Expect every consenter to get one report of decision 150 under config sequence 5, signed so that the
//     consensus verifier accepts it as coming from the assembler of party 2.
//  4. Stop it, and expect every sender to be closed once.
func TestDecisionReporterReportsTheCurrentDecisionOnStart(t *testing.T) {
	signer, verifier := newReporterSigner(t)
	senders := newFakeSenders(3)
	r := newDecisionReporter(t, signer, senders, 100, completeBlock(150))

	r.Start()
	waitForReports(t, senders, 1)
	r.Stop()

	for _, s := range senders {
		require.Equal(t, 1, s.SendControlEventCallCount())
		report := s.SendControlEventArgsForCall(0).AssemblerReport
		require.NotNil(t, report)
		require.Equal(t, reporterParty, report.Party)
		require.Equal(t, types.DecisionNum(150), report.DecisionNum)
		require.Equal(t, reporterConfigSeq, report.ConfigSeq)
		require.NoError(t, verifier.VerifySignature(types.NewAssemblerIdentity(reporterParty), report.ToBeSigned(), report.Signature))
		require.Equal(t, 1, s.CloseCallCount())
	}
}

// Scenario:
//  1. Create a reporter with step 100 and two consenters, whose ledger ends with decision 99, and start it.
//  2. Append the blocks of decision 100, and expect every consenter to get a report of decision 100.
//  3. Append the blocks of decisions 101 to 200, and expect every consenter to get a report of decision 200.
//  4. Stop it, and expect every consenter to have got exactly the reports of decisions 100 and 200.
func TestDecisionReporterReportsEveryStep(t *testing.T) {
	signer, _ := newReporterSigner(t)
	senders := newFakeSenders(2)
	r := newDecisionReporter(t, signer, senders, 100, completeBlock(99))
	r.Start()

	r.Appended(completeBlock(100))
	waitForReports(t, senders, 1)
	for d := types.DecisionNum(101); d <= 200; d++ {
		r.Appended(completeBlock(d))
	}
	waitForReports(t, senders, 2)
	r.Stop()

	for _, s := range senders {
		require.Equal(t, []types.DecisionNum{100, 200}, reportedDecisions(s))
	}
}

// Scenario:
//  1. Create a reporter with step 1 and one consenter, whose ledger ends with the first of the three blocks of
//     decision 10, and start it. Expect a report of decision 9.
//  2. Append the second block of decision 10, then the third. Expect a report of decision 10.
//  3. Append nil, then the first of the two blocks of decision 12, decision 11 having no batches. Expect a
//     report of decision 11.
//  4. Append the second block of decision 12. Expect a report of decision 12.
//  5. Stop it, and expect the consenter to have got exactly the reports of decisions 9, 10, 11 and 12.
func TestDecisionReporterReportsOnlyCompleteDecisions(t *testing.T) {
	signer, _ := newReporterSigner(t)
	senders := newFakeSenders(1)
	r := newDecisionReporter(t, signer, senders, 1, block(10, 0, 3))
	r.Start()
	waitForReports(t, senders, 1)

	r.Appended(block(10, 1, 3))
	r.Appended(block(10, 2, 3))
	waitForReports(t, senders, 2)

	r.Appended(nil)
	r.Appended(block(12, 0, 2))
	waitForReports(t, senders, 3)

	r.Appended(block(12, 1, 2))
	waitForReports(t, senders, 4)
	r.Stop()

	require.Equal(t, []types.DecisionNum{9, 10, 11, 12}, reportedDecisions(senders[0]))
}

// Scenario:
//  1. Create a reporter with step 1 and two consenters, the first of which fails every send, and start it.
//  2. Append decision 1, and expect both consenters to be sent its report.
//  3. Append decision 2, and expect both consenters to be sent its report.
//  4. Stop it. Expect the healthy consenter to have got the reports of decisions 1 and 2, and the failing one
//     to have been sent each report once, with no retry.
func TestDecisionReporterDoesNotRetryAFailedSend(t *testing.T) {
	signer, _ := newReporterSigner(t)
	senders := newFakeSenders(2)
	failing, healthy := senders[0], senders[1]
	failing.SendControlEventReturns(errors.New("consenter unreachable"))
	r := newDecisionReporter(t, signer, senders, 1, completeBlock(0))
	r.Start()

	r.Appended(completeBlock(1))
	waitForReports(t, senders, 1)
	r.Appended(completeBlock(2))
	waitForReports(t, senders, 2)
	r.Stop()

	require.Equal(t, []types.DecisionNum{1, 2}, reportedDecisions(healthy))
	require.Equal(t, []types.DecisionNum{1, 2}, reportedDecisions(failing))
}

// Scenario:
//  1. Create a reporter with step 1 and two consenters, the first of which blocks in its first send until
//     released, and start it.
//  2. Append decision 1. Expect the blocked consenter to be inside the send of its report, and the other
//     consenter to get it.
//  3. Append decision 2, then decision 3, and expect the other consenter to get both reports.
//  4. Release the blocked consenter, and expect its next send to carry the report of decision 3.
//  5. Stop it. Expect the blocked consenter to have been sent the reports of decisions 1 and 3, and the other
//     one the reports of decisions 1, 2 and 3.
func TestDecisionReporterBlockedConsenterGetsTheLatestReport(t *testing.T) {
	signer, _ := newReporterSigner(t)
	senders := newFakeSenders(2)
	blocked, other := senders[0], senders[1]
	release := make(chan struct{})
	blocked.SendControlEventStub = func(state.ControlEvent) error {
		if blocked.SendControlEventCallCount() == 1 {
			<-release
		}
		return nil
	}
	r := newDecisionReporter(t, signer, senders, 1, completeBlock(0))
	r.Start()

	r.Appended(completeBlock(1))
	waitForReports(t, senders, 1)
	r.Appended(completeBlock(2))
	waitForReports(t, []*mocks.FakeSender{other}, 2)
	r.Appended(completeBlock(3))
	waitForReports(t, []*mocks.FakeSender{other}, 3)

	close(release)
	waitForReports(t, []*mocks.FakeSender{blocked}, 2)
	r.Stop()

	require.Equal(t, []types.DecisionNum{1, 3}, reportedDecisions(blocked))
	require.Equal(t, []types.DecisionNum{1, 2, 3}, reportedDecisions(other))
}

// Scenario:
//  1. Create a reporter with step 100 and two consenters, whose ledger ends with decision 100. The first
//     consenter's send blocks until its sender is closed.
//  2. Start it, and expect the blocked consenter to be inside the send of the report of decision 100.
//  3. Stop it, and expect Stop to return, which it does only once every sending goroutine has returned, and
//     every sender to be closed once.
//  4. Stop it again, and expect no sender to be closed again.
//  5. Stop a reporter that was never started, and expect its senders to be closed once.
func TestDecisionReporterStop(t *testing.T) {
	signer, _ := newReporterSigner(t)
	senders := newFakeSenders(2)
	closed := make(chan struct{})
	senders[0].SendControlEventStub = func(state.ControlEvent) error {
		<-closed
		return errors.New("sender is closed")
	}
	senders[0].CloseStub = func() { close(closed) }
	r := newDecisionReporter(t, signer, senders, 100, completeBlock(100))

	r.Start()
	waitForReports(t, senders, 1)
	r.Stop()
	for _, s := range senders {
		require.Equal(t, 1, s.CloseCallCount())
	}

	r.Stop()
	for _, s := range senders {
		require.Equal(t, 1, s.CloseCallCount())
	}

	unstarted := newFakeSenders(2)
	newDecisionReporter(t, signer, unstarted, 100, completeBlock(100)).Stop()
	for _, s := range unstarted {
		require.Equal(t, 1, s.CloseCallCount())
		require.Zero(t, s.SendControlEventCallCount())
	}
}

// newReporterSigner returns a signer for the assembler of the reporter's party, and a verifier that knows its key.
func newReporterSigner(t *testing.T) (crypto.ECDSASigner, crypto.ECDSAVerifier) {
	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	signer := crypto.ECDSASigner(*sk)
	verifier := make(crypto.ECDSAVerifier)
	verifier[types.NewAssemblerIdentity(reporterParty)] = signer.PublicKey
	return signer, verifier
}

// newDecisionReporter creates a reporter of the reporter's party and config sequence that sends through senders.
func newDecisionReporter(t *testing.T, signer crypto.ECDSASigner, senders []*mocks.FakeSender, step types.DecisionNum, last *state.OrderingInformation) *assembler.DecisionReporter {
	var ss []controlevent.Sender
	for _, s := range senders {
		ss = append(ss, s)
	}
	r := assembler.NewDecisionReporter(testutil.CreateLogger(t, int(reporterParty)), reporterParty, reporterConfigSeq, signer, ss, step, last)
	t.Cleanup(r.Stop)
	return r
}

// newFakeSenders returns n fake senders whose sends succeed.
func newFakeSenders(n int) []*mocks.FakeSender {
	senders := make([]*mocks.FakeSender, n)
	for i := range senders {
		senders[i] = &mocks.FakeSender{}
	}
	return senders
}

// waitForReports waits until every sender has been called with at least n reports.
func waitForReports(t *testing.T, senders []*mocks.FakeSender, n int) {
	require.Eventually(t, func() bool {
		for _, s := range senders {
			if s.SendControlEventCallCount() < n {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)
}

// reportedDecisions returns the decisions of the reports sender was called with, in order.
func reportedDecisions(sender *mocks.FakeSender) []types.DecisionNum {
	var decisions []types.DecisionNum
	for i := 0; i < sender.SendControlEventCallCount(); i++ {
		decisions = append(decisions, sender.SendControlEventArgsForCall(i).AssemblerReport.DecisionNum)
	}
	return decisions
}

// completeBlock returns the ordering information of the only block of decision d.
func completeBlock(d types.DecisionNum) *state.OrderingInformation {
	return block(d, 0, 1)
}

// block returns the ordering information of block index of the count blocks of decision d.
func block(d types.DecisionNum, index, count int) *state.OrderingInformation {
	return &state.OrderingInformation{DecisionNum: d, BatchIndex: index, BatchCount: count}
}
