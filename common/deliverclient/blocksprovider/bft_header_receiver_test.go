/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blocksprovider_test

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/Chaguar/common/protoutil"
	"github.com/Chaguar/orderer/common/deliverclient/blocksprovider"
	"github.com/Chaguar/orderer/common/deliverclient/blocksprovider/fake"
	"github.com/Chaguar/orderer/common/deliverclient/orderers"
	"github.com/Chaguar/orderer/common/utils"
	"github.com/Chaguar/orderer/node/comm"
	"github.com/Chaguar/orderer/testutil"
)

func TestBftHeaderReceiver_NoBlocks_RecvError(t *testing.T) {
	fakeBlockVerifier := &fake.UpdatableBlockVerifier{}
	fakeBlockVerifier.VerifyBlockAttestationReturns(errors.New("fake-verify-error"))

	streamClientMock := &fake.DeliverClient{}
	streamClientMock.RecvReturns(nil, errors.New("oops"))
	streamClientMock.CloseSendReturns(nil)

	hr := blocksprovider.NewBFTHeaderReceiver("testchannel", "10.10.10.11:666", streamClientMock, func() {},
		fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, nil, flogging.MustGetLogger("test.BFTHeaderReceiver"))
	assert.NotNil(t, hr)
	assert.False(t, hr.IsStarted())
	assert.False(t, hr.IsStopped())
	_, _, err := hr.LastBlockNum()
	assert.EqualError(t, err, "not found")

	hr.DeliverHeaders() // it will get a Recv() error and exit

	assert.Eventually(t, hr.IsStarted, time.Second, time.Millisecond)
	assert.Eventually(t, hr.IsStopped, time.Second, time.Millisecond)
	_, _, err = hr.LastBlockNum()
	assert.EqualError(t, err, "not found")
	assert.Equal(t, fakeBlockVerifier.VerifyBlockAttestationCallCount(), 0)
	assert.Equal(t, 1, streamClientMock.RecvCallCount())
}

func TestBftHeaderReceiver_BadStatus(t *testing.T) {
	fakeBlockVerifier := &fake.UpdatableBlockVerifier{}
	fakeBlockVerifier.VerifyBlockAttestationReturns(errors.New("fake-verify-error"))

	streamClientMock := &fake.DeliverClient{}
	streamClientMock.RecvReturnsOnCall(0, &orderer.DeliverResponse{Type: &orderer.DeliverResponse_Status{Status: common.Status_SUCCESS}}, nil)
	streamClientMock.RecvReturnsOnCall(1, &orderer.DeliverResponse{Type: &orderer.DeliverResponse_Status{Status: common.Status_BAD_REQUEST}}, nil)
	streamClientMock.RecvReturnsOnCall(2, &orderer.DeliverResponse{Type: &orderer.DeliverResponse_Status{Status: common.Status_SERVICE_UNAVAILABLE}}, nil)
	streamClientMock.CloseSendReturns(nil)

	for i := 0; i < 3; i++ {
		hr := blocksprovider.NewBFTHeaderReceiver("testchannel", "10.10.10.11:666", streamClientMock, func() {},
			fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, nil, flogging.MustGetLogger("test.BFTHeaderReceiver"))
		assert.NotNil(t, hr)

		hr.DeliverHeaders() // it will get a bad status and exit
		assert.Eventually(t, hr.IsStarted, time.Second, time.Millisecond)
		assert.Eventually(t, hr.IsStopped, time.Second, time.Millisecond)
		_, _, err := hr.LastBlockNum()
		assert.EqualError(t, err, "not found")
		assert.Equal(t, fakeBlockVerifier.VerifyBlockAttestationCallCount(), 0)
	}
}

func TestBftHeaderReceiver_NilResponse(t *testing.T) {
	fakeBlockVerifier := &fake.UpdatableBlockVerifier{}
	fakeBlockVerifier.VerifyBlockAttestationReturns(errors.New("fake-verify-error"))

	streamClientMock := &fake.DeliverClient{}
	streamClientMock.RecvReturns(nil, nil)
	streamClientMock.CloseSendReturns(nil)

	hr := blocksprovider.NewBFTHeaderReceiver("testchannel", "10.10.10.11:666", streamClientMock, func() {},
		fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, nil, flogging.MustGetLogger("test.BFTHeaderReceiver"))
	assert.NotNil(t, hr)

	hr.DeliverHeaders() // it will get a bad status and exit
	assert.Eventually(t, hr.IsStarted, time.Second, time.Millisecond)
	assert.Eventually(t, hr.IsStopped, time.Second, time.Millisecond)
	_, _, err := hr.LastBlockNum()
	assert.EqualError(t, err, "not found")
	assert.Equal(t, fakeBlockVerifier.VerifyBlockAttestationCallCount(), 0)
}

// Scenario:
//  1. Start a header receiver whose stream returns a block with no header.
//  2. Expect the receiver to stop with an error stop time, without a header and without verifying.
func TestBftHeaderReceiver_BlockWithNoHeader(t *testing.T) {
	fakeBlockVerifier := &fake.UpdatableBlockVerifier{}

	streamClientMock := &fake.DeliverClient{}
	streamClientMock.RecvReturns(&orderer.DeliverResponse{
		Type: &orderer.DeliverResponse_Block{Block: &common.Block{}},
	}, nil)
	streamClientMock.CloseSendReturns(nil)

	hr := blocksprovider.NewBFTHeaderReceiver("testchannel", "10.10.10.11:666", streamClientMock, func() {},
		fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, nil, flogging.MustGetLogger("test.BFTHeaderReceiver"))

	hr.DeliverHeaders()
	require.True(t, hr.IsStarted())
	require.True(t, hr.IsStopped())
	require.False(t, hr.GetErrorStopTime().IsZero())
	_, _, err := hr.LastBlockNum()
	require.EqualError(t, err, "not found")
	require.Zero(t, fakeBlockVerifier.VerifyBlockAttestationCallCount())
	require.Zero(t, fakeBlockVerifier.VerifyBlockCallCount())
}

func TestBftHeaderReceiver_WithBlocks_Renew(t *testing.T) {
	fakeBlockVerifier := &fake.UpdatableBlockVerifier{}
	fakeBlockVerifier.VerifyBlockAttestationCalls(naiveBlockVerifier)
	fakeBlockVerifier.CloneReturns(fakeBlockVerifier)
	streamClientMock := &fake.DeliverClient{}
	hr := blocksprovider.NewBFTHeaderReceiver("testchannel", "10.10.10.11:666", streamClientMock, func() {},
		fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, nil, flogging.MustGetLogger("test.BFTHeaderReceiver"))

	seqCh := make(chan uint64)
	streamClientMock.RecvCalls(
		func() (*orderer.DeliverResponse, error) {
			time.Sleep(time.Millisecond)

			seqNew, ok := <-seqCh
			if ok {
				return prepareBlock(seqNew, orderer.SeekInfo_HEADER_WITH_SIG, uint32(1)), nil
			} else {
				return nil, errors.New("test closed")
			}
		},
	)
	streamClientMock.CloseSendReturns(nil)

	go hr.DeliverHeaders()

	var bNum uint64
	var bTime time.Time
	var err error

	seqCh <- uint64(1)
	require.Eventually(t, func() bool {
		bNum, bTime, err = hr.LastBlockNum()
		return err == nil && bNum == uint64(1) && !bTime.IsZero()
	}, time.Second, time.Millisecond)

	bTimeOld := bTime
	seqCh <- uint64(2)
	require.Eventually(t, func() bool {
		bNum, bTime, err = hr.LastBlockNum()
		return err == nil && bNum == uint64(2) && bTime.After(bTimeOld)
	}, time.Second, time.Millisecond)

	err = hr.Stop()
	assert.NoError(t, err)

	bTimeOld = bTime
	bNum, bTime, err = hr.LastBlockNum()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), bNum)
	assert.Equal(t, bTime, bTimeOld)

	assert.Equal(t, fakeBlockVerifier.VerifyBlockAttestationCallCount(), 2)
	assert.Equal(t, fakeBlockVerifier.VerifyBlockCallCount(), 0)

	// === Create a new BFTHeaderReceiver with the last good header of the previous receiver
	fakeBlockVerifier = &fake.UpdatableBlockVerifier{}
	streamClientMock = &fake.DeliverClient{}
	hr2 := blocksprovider.NewBFTHeaderReceiver("testchannel", "10.10.10.11:666", streamClientMock, func() {},
		fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, hr, flogging.MustGetLogger("test.BFTHeaderReceiver.2"))
	assert.False(t, hr2.IsStarted())
	assert.False(t, hr2.IsStopped())
	bNum, bTime, err = hr2.LastBlockNum()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), bNum)
	assert.Equal(t, bTime, bTimeOld)
}

func TestBftHeaderReceiver_WithBlocks_StopOnVerificationFailure(t *testing.T) {
	fakeBlockVerifier := &fake.UpdatableBlockVerifier{}
	fakeBlockVerifier.VerifyBlockAttestationCalls(naiveBlockVerifier)
	fakeBlockVerifier.CloneReturns(fakeBlockVerifier)
	streamClientMock := &fake.DeliverClient{}
	hr := blocksprovider.NewBFTHeaderReceiver("testchannel", "10.10.10.11:666",
		streamClientMock, func() {}, fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, nil, flogging.MustGetLogger("test.BFTHeaderReceiver"))

	seqCh := make(chan uint64)
	goodSig := uint32(1)
	streamClientMock.RecvCalls(
		func() (*orderer.DeliverResponse, error) {
			time.Sleep(time.Millisecond)

			seqNew, ok := <-seqCh
			if ok {
				return prepareBlock(seqNew, orderer.SeekInfo_HEADER_WITH_SIG, atomic.LoadUint32(&goodSig)), nil
			} else {
				return nil, errors.New("test closed")
			}
		},
	)
	streamClientMock.CloseSendReturns(nil)

	go hr.DeliverHeaders()

	var bNum uint64
	var bTime time.Time
	var err error

	seqCh <- uint64(1)
	require.Eventually(t, func() bool {
		bNum, bTime, err = hr.LastBlockNum()
		return err == nil && bNum == uint64(1) && !bTime.IsZero()
	}, time.Second, time.Millisecond)

	bTimeOld := bTime
	seqCh <- uint64(2)
	require.Eventually(t, func() bool {
		bNum, bTime, err = hr.LastBlockNum()
		return err == nil && bNum == uint64(2) && bTime.After(bTimeOld)
	}, time.Second, time.Millisecond)

	// Invalid block sig causes the receiver to close
	atomic.StoreUint32(&goodSig, 0)
	seqCh <- uint64(3)
	require.Eventually(t, hr.IsStopped, time.Second, time.Millisecond)

	// After the receiver closes, it returns the last good header
	bTimeOld = bTime
	bNum, bTime, err = hr.LastBlockNum()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), bNum)
	assert.Equal(t, bTime, bTimeOld)

	assert.Equal(t, fakeBlockVerifier.VerifyBlockAttestationCallCount(), 3)
	assert.Equal(t, fakeBlockVerifier.VerifyBlockCallCount(), 0)
}

func TestBftHeaderReceiver_WithBlocks_ConfigVerification(t *testing.T) {
	fakeBlockVerifier := &fake.UpdatableBlockVerifier{}
	fakeBlockVerifier.VerifyBlockAttestationCalls(naiveBlockVerifier)
	fakeBlockVerifier.CloneReturns(fakeBlockVerifier)
	streamClientMock := &fake.DeliverClient{}
	hr := blocksprovider.NewBFTHeaderReceiver("testchannel", "10.10.10.11:666", streamClientMock, func() {},
		fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, nil, flogging.MustGetLogger("test.BFTHeaderReceiver"))

	seqCh := make(chan uint64)
	streamClientMock.RecvCalls(
		func() (*orderer.DeliverResponse, error) {
			time.Sleep(time.Millisecond)

			seqNew, ok := <-seqCh
			if ok {
				if seqNew == 3 {
					res := prepareConfigBlock(seqNew, uint32(1))
					assert.True(t, protoutil.IsConfigBlock(res.GetBlock()))
					return res, nil
				}
				return prepareBlock(seqNew, orderer.SeekInfo_HEADER_WITH_SIG, uint32(1)), nil
			} else {
				return nil, errors.New("test closed")
			}
		},
	)
	streamClientMock.CloseSendReturns(nil)

	go hr.DeliverHeaders()

	var bNum uint64
	var bTime time.Time
	var err error

	seqCh <- uint64(1)
	require.Eventually(t, func() bool {
		bNum, bTime, err = hr.LastBlockNum()
		return err == nil && bNum == uint64(1) && !bTime.IsZero()
	}, time.Second, time.Millisecond)

	bTimeOld := bTime
	seqCh <- uint64(2)
	require.Eventually(t, func() bool {
		bNum, bTime, err = hr.LastBlockNum()
		return err == nil && bNum == uint64(2) && bTime.After(bTimeOld)
	}, time.Second, time.Millisecond)

	// config block should be verified in full
	bTimeOld = bTime
	seqCh <- uint64(3)
	require.Eventually(t, func() bool {
		bNum, bTime, err = hr.LastBlockNum()
		return err == nil && bNum == uint64(3) && bTime.After(bTimeOld)
	}, time.Second, time.Millisecond)

	err = hr.Stop()
	require.NoError(t, err)
	require.Eventually(t, hr.IsStopped, time.Second, time.Millisecond)

	assert.Equal(t, fakeBlockVerifier.VerifyBlockAttestationCallCount(), 2)
	assert.Equal(t, fakeBlockVerifier.VerifyBlockCallCount(), 1)
	assert.Equal(t, fakeBlockVerifier.UpdateConfigCallCount(), 1)
}

func TestBftHeaderReceiver_VerifyOnce(t *testing.T) {
	fakeBlockVerifier := &fake.UpdatableBlockVerifier{}
	fakeBlockVerifier.VerifyBlockAttestationCalls(naiveBlockVerifier)
	fakeBlockVerifier.CloneReturns(fakeBlockVerifier)
	streamClientMock := &fake.DeliverClient{}
	hr := blocksprovider.NewBFTHeaderReceiver("testchannel", "10.10.10.11:666", streamClientMock, func() {},
		fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, nil, flogging.MustGetLogger("test.BFTHeaderReceiver"))

	seqCh := make(chan uint64)
	goodSig := uint32(1)
	streamClientMock.RecvCalls(
		func() (*orderer.DeliverResponse, error) {
			time.Sleep(time.Millisecond)

			seqNew, ok := <-seqCh
			if ok {
				return prepareBlock(seqNew, orderer.SeekInfo_HEADER_WITH_SIG, atomic.LoadUint32(&goodSig)), nil
			} else {
				return nil, errors.New("test closed")
			}
		},
	)
	streamClientMock.CloseSendReturns(nil)

	go hr.DeliverHeaders()

	seqCh <- uint64(5)
	require.Eventually(t, func() bool {
		bNum, bTime, err := hr.LastBlockNum()
		return err == nil && bNum == uint64(5) && !bTime.IsZero()
	}, time.Second, time.Millisecond)

	for i := 0; i < 10; i++ {
		bNum, bTime, err := hr.LastBlockNum()
		assert.NoError(t, err)
		assert.Equal(t, uint64(5), bNum)
		assert.True(t, !bTime.IsZero())
	}
	assert.Equal(t, fakeBlockVerifier.VerifyBlockAttestationCallCount(), 1)

	err := hr.Stop()
	assert.NoError(t, err)
	assert.Eventually(t, hr.IsStopped, time.Second, time.Millisecond)
}

func prepareBlock(seq uint64, contentType orderer.SeekInfo_SeekContentType, goodSignature uint32) *orderer.DeliverResponse {
	const numTx = 10
	block := protoutil.NewBlock(seq, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	data := &common.BlockData{
		Data: make([][]byte, numTx),
	}
	for i := 0; i < numTx; i++ {
		data.Data[i] = []byte{byte(i), byte(seq)}
	}
	block.Header.DataHash = protoutil.ComputeBlockDataHash(data)
	if contentType == orderer.SeekInfo_BLOCK {
		block.Data = data
	}

	fakeSignature(block, goodSignature)

	return &orderer.DeliverResponse{Type: &orderer.DeliverResponse_Block{Block: block}}
}

func fakeSignature(block *common.Block, goodSignature uint32) {
	if goodSignature > 0 {
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = []byte("good")
	} else {
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = []byte("bad")
	}
}

func prepareConfigBlock(seq uint64, goodSignature uint32) *orderer.DeliverResponse {
	block := protoutil.NewBlock(seq, []byte{1, 2, 3, 4, 5, 6, 7, 8})

	env := &common.Envelope{
		Payload: protoutil.MarshalOrPanic(&common.Payload{
			Header: &common.Header{
				ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
					Type:      int32(common.HeaderType_CONFIG),
					ChannelId: "test-chain",
				}),
			},
			Data: protoutil.MarshalOrPanic(&common.ConfigEnvelope{}),
		}),
	}

	block.Data = &common.BlockData{Data: [][]byte{protoutil.MarshalOrPanic(env)}}
	block.Header.DataHash = protoutil.ComputeBlockDataHash(block.Data)
	fakeSignature(block, goodSignature)

	return &orderer.DeliverResponse{
		Type: &orderer.DeliverResponse_Block{
			Block: block,
		},
	}
}

func naiveBlockVerifier(signedBlock *common.Block) error {
	sigArray := signedBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES]
	sig := string(sigArray)
	if sig == "good" {
		return nil
	}
	return errors.New("test: bad signature")
}

// Scenario:
//  1. Create a header receiver whose stream fails on the first Recv, with a closer that counts its calls.
//  2. Deliver headers, and expect the receiver to stop on the error and close its connection once.
//  3. Stop it again, and expect the closer not to be called again.
func TestBftHeaderReceiver_StopClosesTheConnection(t *testing.T) {
	fakeBlockVerifier := &fake.UpdatableBlockVerifier{}
	streamClientMock := &fake.DeliverClient{}
	streamClientMock.RecvReturns(nil, errors.New("oops"))
	var closed atomic.Int32

	hr := blocksprovider.NewBFTHeaderReceiver("testchannel", "10.10.10.11:666", streamClientMock, func() { closed.Add(1) },
		fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, nil, flogging.MustGetLogger("test.BFTHeaderReceiver"))
	hr.DeliverHeaders()
	require.True(t, hr.IsStopped())
	require.Equal(t, int32(1), closed.Load())

	require.NoError(t, hr.Stop())
	require.Equal(t, int32(1), closed.Load())
}

// Scenario:
//  1. Serve a deliver service that, like an orderer serving a headers seek, reads the seek and then streams headers
//     without ever reading the stream again. Count its running handlers and its open connections.
//  2. Connect to it through a DeliveryRequester, as the censorship monitor does, and deliver headers until some arrive.
//  3. Stop the receiver, and expect the handler to end and the connection to close: the orderer doesn't stay blocked
//     sending headers that no one reads.
func TestBftHeaderReceiver_StopEndsTheStreamOnTheOrderer(t *testing.T) {
	_, listener := testutil.SharedTestPortAllocator().Allocate(t)
	conns := &countingListener{Listener: listener}
	streamer := &headerStreamer{}
	server := grpc.NewServer()
	orderer.RegisterAtomicBroadcastServer(server, streamer)
	go func() { _ = server.Serve(conns) }()
	t.Cleanup(server.Stop)

	requester := blocksprovider.NewDeliveryRequester("testchannel", &fake.Signer{}, nil,
		blocksprovider.DialerAdapter{ClientConfig: comm.ClientConfig{DialTimeout: 5 * time.Second}}, blocksprovider.DeliverAdapter{})
	seek, err := requester.SeekInfoHeadersFrom(0)
	require.NoError(t, err)
	client, closeConn, err := requester.Connect(seek, &orderers.Endpoint{Address: listener.Addr().String()})
	require.NoError(t, err)

	fakeBlockVerifier := &fake.UpdatableBlockVerifier{}
	fakeBlockVerifier.CloneReturns(&fake.UpdatableBlockVerifier{})
	hr := blocksprovider.NewBFTHeaderReceiver("testchannel", listener.Addr().String(), client, closeConn,
		fakeBlockVerifier, &utils.CommonConfigBlockOperations{}, nil, flogging.MustGetLogger("test.BFTHeaderReceiver"))
	go hr.DeliverHeaders()
	require.Eventually(t, func() bool {
		_, _, err := hr.LastBlockNum()
		return err == nil
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), streamer.handlers.Load())

	require.NoError(t, hr.Stop())
	require.Eventually(t, func() bool { return streamer.handlers.Load() == 0 && conns.open.Load() == 0 }, 10*time.Second, 10*time.Millisecond)
}

// headerStreamer is a deliver service that reads the seek and then sends headers until the stream ends, never
// reading again. It counts the handlers running.
type headerStreamer struct {
	orderer.UnimplementedAtomicBroadcastServer
	handlers atomic.Int32
}

func (h *headerStreamer) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	h.handlers.Add(1)
	defer h.handlers.Add(-1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	for n := uint64(0); ; n++ {
		header := &orderer.DeliverResponse{Type: &orderer.DeliverResponse_Block{Block: &common.Block{Header: &common.BlockHeader{Number: n}}}}
		if err := stream.Send(header); err != nil {
			return err
		}
	}
}

// countingListener counts the connections it accepted that are still open.
type countingListener struct {
	net.Listener
	open atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.open.Add(1)
	return &countedConn{Conn: conn, listener: l}, nil
}

// countedConn is a connection that a countingListener accepted.
type countedConn struct {
	net.Conn
	listener *countingListener
	closed   sync.Once
}

func (c *countedConn) Close() error {
	c.closed.Do(func() { c.listener.open.Add(-1) })
	return c.Conn.Close()
}
