/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chaguar/orderer/common/types"
	"github.com/Chaguar/orderer/node/consensus/state"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
)

//go:generate counterfeiter -o mocks/request_inspector.go . RequestInspector
type RequestInspector interface {
	RequestID(req []byte) string
}

//go:generate counterfeiter -o mocks/mem_pool.go . MemPool
type MemPool interface {
	NextRequests(ctx context.Context) ([][]byte, []string)
	RemoveRequests(requestIDs ...string)
	ReturnRequests(requestIDs ...string)
	Submit(request []byte) error
	Halt()
	Restart(bool)
	RequestCount() int64
	Close()
	Prune(predicate func([]byte) error)
	Contains(reqID string) bool
}

//go:generate counterfeiter -o mocks/term_provider.go . TermProvider

// TermProvider provides the current primary and term of the shard, which the batcher keeps
type TermProvider interface {
	CurrentPrimaryAndTerm() (types.PartyID, uint64)
}

//go:generate counterfeiter -o mocks/config_sequence_getter.go . ConfigSequenceGetter
type ConfigSequenceGetter interface {
	ConfigSequence() types.ConfigSequence
}

//go:generate counterfeiter -o mocks/complainer.go . Complainer
type Complainer interface {
	// Complain sends a complaint about the primary of the given term, giving up once ctx is done.
	Complain(ctx context.Context, term uint64, reason string)
}

//go:generate counterfeiter -o mocks/batched_requests_verifier.go . BatchedRequestsVerifier

// BatchedRequestsVerifier verifies batched requests
type BatchedRequestsVerifier interface {
	// VerifyBatchedRequests verifies the batch and returns the ids of its
	// requests, corresponding by index, so callers can reuse them without
	// recomputing.
	VerifyBatchedRequests(types.BatchedRequests) ([]string, error)
	// VerifyRequest verifies a single request unconditionally (it does not consult
	// the mem pool), unlike VerifyBatchedRequests which skips already-pooled requests.
	VerifyRequest(req []byte) error
}

//go:generate counterfeiter -o mocks/batch_acker.go . BatchAcker

// BatchAcker sends an ack over a specific batch
type BatchAcker interface {
	Ack(seq types.BatchSequence, to types.PartyID)
}

//go:generate counterfeiter -o mocks/baf_sender.go . BAFSender

// BAFSender sends the baf to the consenters
type BAFSender interface {
	// SendBAF returns once enough consenters accepted the baf, or with an error as soon as ctx is cancelled or the
	// sender is stopped.
	SendBAF(baf types.BatchAttestationFragment, ctx context.Context) error
}

//go:generate counterfeiter -o mocks/baf_creator.go . BAFCreator

// BAFCreator creates a baf
type BAFCreator interface {
	CreateBAF(seq types.BatchSequence, primary types.PartyID, shard types.ShardID, digest []byte, txCount uint64, primarySignature []byte) types.BatchAttestationFragment
}

type BatchLedgerWriter interface {
	// Append adds a batch to the ledger. The `digest` must be the digest of `batchedRequests`.
	Append(partyID types.PartyID, batchSeq types.BatchSequence, configSeq types.ConfigSequence, batchedRequests types.BatchedRequests, digest []byte, primarySignature []byte)
}

type BatchLedgeReader interface {
	Height(partyID types.PartyID) uint64
	RetrieveBatchByNumber(partyID types.PartyID, seq uint64) types.Batch
}

//go:generate counterfeiter -o mocks/batch_ledger.go . BatchLedger
type BatchLedger interface {
	BatchLedgerWriter
	BatchLedgeReader
}

type BatcherRole struct {
	BatchTimeout            time.Duration
	RequestInspector        RequestInspector
	ID                      types.PartyID
	Shard                   types.ShardID
	Threshold               int
	N                       uint16
	Logger                  *flogging.FabricLogger
	Ledger                  BatchLedger
	BatchPuller             BatchesPuller
	TermProvider            TermProvider
	ConfigSequenceGetter    ConfigSequenceGetter
	BAFCreator              BAFCreator
	BAFSender               BAFSender
	BatchAcker              BatchAcker
	Complainer              Complainer
	BatchedRequestsVerifier BatchedRequestsVerifier
	SigVerifier             SigVerifier
	MemPool                 MemPool
	BatchSequenceGap        types.BatchSequence
	running                 sync.WaitGroup
	stopChan                chan struct{}
	stopCtx                 context.Context
	cancelBatch             func()
	primary                 types.PartyID
	seq                     types.BatchSequence
	acting                  atomic.Pointer[acting] // set once the pool is restarted in the term
	termChan                chan struct{}
	resubmitChan            chan termChange
	ackerLock               sync.RWMutex
	acker                   SeqAcker
	Metrics                 *BatcherMetrics
	isStopped               bool
	isSoftStopped           bool
}

func (b *BatcherRole) Start() {
	b.isStopped = false
	b.isSoftStopped = false

	b.stopChan = make(chan struct{})
	b.stopCtx, b.cancelBatch = context.WithCancel(context.Background())
	b.termChan = make(chan struct{}, 1)
	b.resubmitChan = make(chan termChange, 1)

	b.running.Add(2)
	go b.resubmitOnTermChanges()
	go b.run()
}

func (b *BatcherRole) run() {
	defer b.running.Done()
	for {
		select {
		case <-b.stopChan:
			return
		default:
		}

		primary, term := b.TermProvider.CurrentPrimaryAndTerm()
		b.primary = primary
		b.seq = types.BatchSequence(b.Ledger.Height(b.primary))
		b.Logger.Infof("ID: %d, shard: %d, primary id: %d, term: %d, seq: %d", b.ID, b.Shard, b.primary, term, b.seq)

		if b.primary == b.ID {
			b.runPrimary(term)
		} else {
			b.runSecondary(term)
		}
	}
}

// acting is a term the role acts in, and the context of its run in that term. The context ends when the run
// returns, when the role stops, and once the role is notified that the term moved on.
type acting struct {
	term   uint64
	ctx    context.Context
	cancel context.CancelFunc
}

// Term returns the term the role acts in, which is the term in which the pool was last restarted.
// It can be behind the batcher's term until the role learns of a term change and restarts in the new term.
func (b *BatcherRole) Term() uint64 {
	term, _ := b.TermContext()
	return term
}

// TermContext returns the term the role acts in and the context of its run in that term, which a broadcast on behalf
// of that term, such as a complaint, gives up on once it ends. Before the role first acts in a term, the context is
// already done.
func (b *BatcherRole) TermContext() (uint64, context.Context) {
	if a := b.acting.Load(); a != nil {
		return a.term, a.ctx
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return 0, ctx
}

// restartPool restarts the pool for the role and only then sets the term the role acts in, and the context of its
// run, so that a second-strike complaint about a request the pool held in the previous term is never sent in the
// new term, and is given up with the previous run. If the term already moved on, the run's context ends at once.
func (b *BatcherRole) restartPool(batching bool, term uint64, runCtx context.Context, cancelRun context.CancelFunc) {
	b.MemPool.Restart(batching)
	b.acting.Store(&acting{term: term, ctx: runCtx, cancel: cancelRun})
	if b.termChangedFrom(term) {
		cancelRun()
	}
}

// termChangedFrom reports whether the batcher's term moved on from term, the term the role acts in.
// A notification can arrive for the term the role already acts in: the batcher can update the term
// after the role starts a term and before it notifies, and the role read the new term in between.
// The role then keeps acting, rather than restarting in the same term.
func (b *BatcherRole) termChangedFrom(term uint64) bool {
	_, current := b.TermProvider.CurrentPrimaryAndTerm()
	return current != term
}

func (b *BatcherRole) Stop() {
	if b.isStopped {
		return
	}

	b.Logger.Infof("Stopping batcher role")
	if !b.isSoftStopped {
		close(b.stopChan)
	}
	b.cancelBatch()
	b.MemPool.Close()
	b.running.Wait()

	b.isStopped = true
	b.isSoftStopped = true
}

// SoftStop stops the batcher role with mempool Halt
func (b *BatcherRole) SoftStop() {
	b.Logger.Infof("Soft Stopping batcher role")

	if b.isSoftStopped || b.isStopped {
		return
	}

	b.isSoftStopped = true

	close(b.stopChan)
	b.cancelBatch()
	b.running.Wait()
	b.MemPool.Halt()
}

// termChange is a term change whose pending BAFs are to be resubmitted
type termChange struct {
	state       *state.State
	prevPrimary types.PartyID
}

// NotifyTermChange tells the role that the term of its shard changed, after the TermProvider was updated,
// so the role stops acting in the old term and restarts in the new one. It also ends the context of the role's run
// in the old term, so that a broadcast on behalf of that term, such as a complaint, no longer waits for its quorum.
func (b *BatcherRole) NotifyTermChange() {
	// The role reads the new term from the TermProvider when it restarts, so a pending
	// notification already covers this one.
	select {
	case b.termChan <- struct{}{}:
	default:
	}
	if a := b.acting.Load(); a != nil && b.termChangedFrom(a.term) {
		a.cancel()
	}
}

// ResubmitOnTermChange hands a term change to the role, which resubmits the requests of this batcher's
// pending BAFs in state of prevPrimary, the primary of the old term. The resubmission runs on the role's
// own goroutine, so it does not delay the caller; this blocks only while a previous one is still waiting.
func (b *BatcherRole) ResubmitOnTermChange(state *state.State, prevPrimary types.PartyID) {
	select {
	case b.resubmitChan <- termChange{state: state, prevPrimary: prevPrimary}:
	case <-b.stopChan:
	}
}

func (b *BatcherRole) resubmitOnTermChanges() {
	defer b.running.Done()
	for {
		select {
		case <-b.stopChan:
			return
		case tc := <-b.resubmitChan:
			b.ResubmitPendingBAFs(tc.state, tc.prevPrimary, false)
			b.Metrics.memPoolSize.Set(float64(b.MemPool.RequestCount()))
		}
	}
}

// when the term is changed there might be a case where
// a batch with a certain tx was attested by not enough batchers (less than f+1 BAFs)
// and that tx is in the pool of other batchers but again not enough (less than f+1 batchers)
// to prevent a case where such a tx falls through the cracks, after a term change
// all batchers resubmit to their pools txs in batches with their BAFs still in pending state
// this will also be used after config update for resubmitting expired BAFs while ignoring the prev primary parameter
func (b *BatcherRole) ResubmitPendingBAFs(state *state.State, prevPrimary types.PartyID, ignorePrevPrimary bool) {
	for _, baf := range state.Pending {
		if baf.Shard() == b.Shard && baf.Signer() == b.ID {
			if !ignorePrevPrimary && baf.Primary() != prevPrimary {
				continue
			}
			b.Logger.Debugf("found pending BAF signed by me (id: %d) from primary: %d ; %s", b.ID, baf.Primary(), baf.String())
			b.reviveBAFRequests(baf)
		}
	}
}

// ResubmitStaleConfigBAFs revives the requests of this batcher's own BAFs that consensus surfaced as
// behind the current config sequence (state.StaleConfigBAFs). Unlike ResubmitPendingBAFs, it is checked on
// every delivered decision (the array lives for a single decision) and has no prevPrimary filter.
// Because decisions are applied in order, the batcher has already applied the config block by the time
// such a BAF is surfaced, so the revived requests re-batch under the new config sequence.
func (b *BatcherRole) ResubmitStaleConfigBAFs(state *state.State) {
	for _, baf := range state.StaleConfigBAFs {
		if baf.Shard() == b.Shard && baf.Signer() == b.ID {
			b.Logger.Infof("reviving requests of stale-config BAF signed by me (id: %d) from primary: %d; %s",
				b.ID, baf.Primary(), baf.String())
			b.reviveBAFRequests(baf)
		}
	}
}

// reviveBAFRequests re-reads, from the local ledger, the batch that baf attests and resubmits its
// requests to the mempool so they can be re-batched. When baf was created under an older config the
// channel policies may have changed since, so the requests are not guaranteed to still satisfy the
// current policy: they must be re-verified before re-entering the pool (the secondary pull path treats
// pool membership as "already verified under the current config" and skips re-verifying pooled
// requests). When baf is from the current config the requests were already verified under it, so
// re-verification is unnecessary.
func (b *BatcherRole) reviveBAFRequests(baf types.BatchAttestationFragment) {
	batch := b.Ledger.RetrieveBatchByNumber(baf.Primary(), uint64(baf.Seq()))
	if batch == nil {
		b.Logger.Panicf("Error: No such batch; BAF signed by me (id: %d) from primary: %d; %s",
			b.ID, baf.Primary(), baf.String())
	}
	reverify := baf.ConfigSequence() < b.ConfigSequenceGetter.ConfigSequence()
	for _, req := range batch.Requests() {
		if reverify {
			// Verify one request at a time so a single offending request is dropped
			// without discarding the rest of the batch.
			if err := b.BatchedRequestsVerifier.VerifyRequest(req); err != nil {
				b.Logger.Errorf("Dropping request that failed verification under the current config before resubmitting; err: %v",
					err)
				continue
			}
		}
		if err := b.MemPool.Submit(req); err != nil {
			if strings.Contains(err.Error(), "already inserted") {
				b.Logger.Debugf("Failed submitting request to pool; err: %v", err)
				continue
			}
			b.Logger.Errorf("Failed submitting request to pool; err: %v", err)
		}
	}
}

func (b *BatcherRole) Submit(request []byte) error {
	if err := b.MemPool.Submit(request); err != nil {
		return err
	}

	b.Metrics.memPoolSize.Set(float64(b.MemPool.RequestCount()))
	return nil
}

func (b *BatcherRole) HandleAck(seq types.BatchSequence, from types.PartyID) {
	b.ackerLock.RLock()
	defer b.ackerLock.RUnlock()
	if b.acker != nil {
		b.acker.HandleAck(seq, from)
	}
}

func (b *BatcherRole) runPrimary(term uint64) {
	b.Logger.Infof("Batcher %d acting as primary (shard %d)", b.ID, b.Shard)
	b.Metrics.currentRole.Set(1)

	defer func() {
		b.Logger.Infof("Batcher %d stopped acting as primary (shard %d)", b.ID, b.Shard)
		b.ackerLock.RLock()
		b.acker.Stop()
		b.ackerLock.RUnlock()
	}()

	b.ackerLock.Lock()
	b.acker = NewAcker(b.seq, b.BatchSequenceGap, b.N, uint16(b.Threshold), b.Logger)
	b.ackerLock.Unlock()
	runCtx, cancelRun := context.WithCancel(b.stopCtx)
	defer cancelRun()
	b.restartPool(true, term, runCtx, cancelRun)

	var currentBatch types.BatchedRequests
	var reqIDs []string
	var digest []byte

	for {
		for {
			b.ackerLock.RLock()
			ch := b.acker.WaitForSecondaries(b.seq)
			b.ackerLock.RUnlock()
			select {
			case <-b.termChan:
				if !b.termChangedFrom(term) {
					continue
				}
				b.Logger.Infof("Primary batcher %d (shard %d) term change from term %d", b.ID, b.Shard, term)
				return
			case <-b.stopChan:
				return
			case <-ch:
			}
			mempoolStart := time.Now()
			ctx, cancel := context.WithTimeout(b.stopCtx, b.BatchTimeout)
			currentBatch, reqIDs = b.MemPool.NextRequests(ctx)

			b.Metrics.batchMempoolNextRequestsLatency.Observe(time.Since(mempoolStart).Seconds())

			if len(currentBatch) == 0 {
				cancel()
				continue
			}

			// A stop, which cancels ctx, or a term change can end the wait in NextRequests. The role then no
			// longer acts in this term, so it does not store or send the batch, and returns its requests instead.
			if b.stoppedActingIn(term) {
				cancel()
				b.Logger.Infof("Primary batcher %d (shard %d) stopped acting in term %d; "+
					"returning the next batch's requests to the mem pool", b.ID, b.Shard, term)
				b.MemPool.ReturnRequests(reqIDs...)
				return
			}

			b.Logger.Infof("Batcher batched a total of %d requests for sequence %d", len(currentBatch), b.seq)
			digestStart := time.Now()
			digest = currentBatch.Digest()
			b.Metrics.batchHashingLatency.Observe(time.Since(digestStart).Seconds())
			cancel()
			break
		}

		baf := b.BAFCreator.CreateBAF(b.seq, b.ID, b.Shard, digest, uint64(len(currentBatch)), nil)

		// After the batch is appended to the ledger the batcher sends the BAF to the consenters
		// (this BAF is a declaration that the batch is stored in the ledger)
		// Once the BAF reached the consenters it is considered safe to remove the requests from the mem pool

		// The digest was already computed for the BAF above.
		b.Ledger.Append(b.ID, b.seq, b.ConfigSequenceGetter.ConfigSequence(), currentBatch, digest, baf.Signature())
		// The batch's requests are removed once a quorum of consenters accepted the BAF, and returned to the mem
		// pool otherwise. The send fails only when the run's context ends, on a stop or a term change, and it then
		// ends at once, reporting success only if a quorum already accepted the BAF. Sends the broadcast already
		// started, and the secondaries' BAFs, can still get the batch ordered, so returned requests may be ordered
		// twice.
		if err := b.BAFSender.SendBAF(baf, runCtx); err != nil {
			b.Logger.Warnf("Primary batcher %d (shard %d) did not send the BAF for seq %d; "+
				"returning its requests to the mem pool: %v", b.ID, b.Shard, b.seq, err)
			b.MemPool.ReturnRequests(reqIDs...)
			return
		}

		b.Metrics.batchedTxsTotal.Add(float64(len(currentBatch)))

		b.ackerLock.RLock()
		b.acker.HandleAck(b.seq, b.ID)
		b.ackerLock.RUnlock()

		b.seq++

		b.removeRequestsByIDs(reqIDs)
		if b.stoppedActingIn(term) {
			return
		}
	}
}

// removeRequestsByIDs removes the given request ids from the mem pool
func (b *BatcherRole) removeRequestsByIDs(reqIDs []string) {
	b.MemPool.RemoveRequests(reqIDs...)
	b.Metrics.memPoolSize.Set(float64(b.MemPool.RequestCount()))
}

// stoppedActingIn reports, without waiting, whether the role stops or the term moved on from term. It reads the
// term rather than a notification, since the batcher updates the term before it notifies the role.
func (b *BatcherRole) stoppedActingIn(term uint64) bool {
	select {
	case <-b.stopChan:
		return true
	default:
		return b.termChangedFrom(term)
	}
}

func (b *BatcherRole) runSecondary(term uint64) {
	b.Logger.Infof("Batcher %d acting as secondary (shard %d; primary %d)", b.ID, b.Shard, b.primary)
	runCtx, cancelRun := context.WithCancel(b.stopCtx)
	defer cancelRun()
	b.restartPool(false, term, runCtx, cancelRun)
	b.Metrics.currentRole.Set(2)

	for {
		out := b.BatchPuller.PullBatches(b.primary)
		for {
			var batch types.Batch
			var verifyStart time.Time
			select {
			case batch = <-out:
				verifyStart = time.Now()
			case <-b.termChan:
				if !b.termChangedFrom(term) {
					continue
				}
				b.Logger.Infof("Secondary batcher %d (shard %d) term change from term %d", b.ID, b.Shard, term)
				b.BatchPuller.Stop()
				return
			case <-b.stopChan:
				b.Logger.Infof("Batcher %d stopped acting as secondary (shard %d; primary %d)", b.ID, b.Shard, b.primary)
				b.BatchPuller.Stop()
				return
			}
			reqIDs, err := b.verifyBatch(batch)
			b.Metrics.batchVerifyLatency.Observe(time.Since(verifyStart).Seconds())

			if err != nil {
				b.Logger.Warnf("Secondary batcher %d (shard %d) sending a complaint (primary %d); verify batch err: %v", b.ID, b.Shard, b.primary, err)
				// complain in the term the fault was seen in, even if the term changed since
				reason := fmt.Sprintf("batcher %d (shard %d) complaining; primary %d; term %d; verify batch err: %v",
					b.ID, b.Shard, b.primary, term, err)
				b.Complainer.Complain(runCtx, term, reason)
				b.BatchPuller.Stop()
				break // TODO maybe add backoff
			}

			b.Metrics.batchesPulledTotal.Add(1)
			requests := batch.Requests()
			// verifyBatch verified that the digest the primary declared in the batch header is
			// indeed the digest of the requests. So reuse it for both the ledger append and the BAF.
			digest := batch.Digest()

			// After the batch is appended to the ledger the batcher sends the BAF to the consenters
			// (this BAF is a declaration that the batch is stored in the ledger)
			// Once the BAF reached the consenters it is considered safe to remove the requests from the mem pool

			b.Logger.Infof("Secondary batcher %d (shard %d; current primary %d) appending to ledger batch with seq %d and %d requests", b.ID, b.Shard, b.primary, b.seq, len(requests))
			b.Ledger.Append(b.primary, b.seq, b.ConfigSequenceGetter.ConfigSequence(), requests, digest, batch.PrimarySignature())
			baf := b.BAFCreator.CreateBAF(b.seq, b.primary, b.Shard, digest, uint64(len(requests)), batch.PrimarySignature())

			// The send fails only when the run's context ends, on a stop or a term change. The BAF may then not have
			// reached the consenters, so the secondary keeps the batch's requests and does not ack the batch.
			if err := b.BAFSender.SendBAF(baf, runCtx); err != nil {
				b.Logger.Infof("Secondary batcher %d (shard %d) did not send the BAF for seq %d of primary %d; "+
					"keeping its requests: %v", b.ID, b.Shard, b.seq, b.primary, err)
				b.BatchPuller.Stop()
				return
			}

			b.Metrics.batchedTxsTotal.Add(float64(len(requests)))
			b.removeRequestsByIDs(reqIDs)
			b.BatchAcker.Ack(baf.Seq(), b.primary)
			b.seq++
		}
	}
}

// verifyBatch verifies the pulled batch and returns the ids of its requests (corresponding by index)
func (b *BatcherRole) verifyBatch(batch types.Batch) ([]string, error) {
	if batch.ConfigSequence() != b.ConfigSequenceGetter.ConfigSequence() {
		b.Logger.Warnf("Batch config seq (%d) does not match batcher's current config seq (%d)", batch.ConfigSequence(), b.ConfigSequenceGetter.ConfigSequence())
	}
	if batch.Primary() != b.primary {
		return nil, fmt.Errorf("batch primary (%d) not equal to expected primary (%d)", batch.Primary(), b.primary)
	}
	if batch.Shard() != b.Shard {
		return nil, fmt.Errorf("batch shard (%d) not equal to expected shard (%d)", batch.Shard(), b.Shard)
	}
	if batch.Seq() != b.seq {
		return nil, fmt.Errorf("batch seq (%d) not equal to expected seq (%d)", batch.Seq(), b.seq)
	}
	if len(batch.Requests()) == 0 {
		return nil, errors.New("empty batch")
	}
	br := batch.Requests()
	digestStart := time.Now()
	computedDigest := br.Digest()
	b.Metrics.batchHashingLatency.Observe(time.Since(digestStart).Seconds())
	if !slices.Equal(batch.Digest(), computedDigest) {
		return nil, fmt.Errorf("batch digest (%x) is not equal to calculated digest (%x)", batch.Digest(), computedDigest)
	}
	primaryBAF := types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), batch.Primary(), batch.ConfigSequence(), uint64(len(batch.Requests())), nil)
	if err := b.SigVerifier.VerifySignature(types.NewBatcherIdentity(batch.Primary(), batch.Shard()), primaryBAF.ToBeSigned(), batch.PrimarySignature()); err != nil {
		return nil, fmt.Errorf("failed verifying primary signature for batch seq %d: %w", b.seq, err)
	}
	reqIDs, err := b.BatchedRequestsVerifier.VerifyBatchedRequests(batch.Requests())
	if err != nil {
		return nil, fmt.Errorf("failed verifying requests for batch seq %d; err: %w", b.seq, err)
	}
	return reqIDs, nil
}
