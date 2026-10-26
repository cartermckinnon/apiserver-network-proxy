/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server/metrics"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server/proxystrategies"
	"sigs.k8s.io/apiserver-network-proxy/proto/header"
)

// pausableFrontendStream is a gRPC frontend whose Send blocks until the test
// reads the packet from sent, standing in for a client whose HTTP/2 window is
// exhausted. Recv blocks until the stream context ends.
type pausableFrontendStream struct {
	grpc.ServerStream
	ctx     context.Context
	sent    chan *client.Packet
	sendErr error
}

func newPausableFrontendStream(ctx context.Context) *pausableFrontendStream {
	return &pausableFrontendStream{ctx: ctx, sent: make(chan *client.Packet)}
}

func (s *pausableFrontendStream) Send(pkt *client.Packet) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	select {
	case s.sent <- pkt:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *pausableFrontendStream) Recv() (*client.Packet, error) {
	<-s.ctx.Done()
	return nil, io.EOF
}

func (s *pausableFrontendStream) Context() context.Context { return s.ctx }

// read returns the next packet the frontend transport accepted.
func (s *pausableFrontendStream) read(t *testing.T) *client.Packet {
	t.Helper()
	select {
	case pkt := <-s.sent:
		return pkt
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a packet to reach the gRPC frontend")
		return nil
	}
}

func (s *pausableFrontendStream) readData(t *testing.T, want string) {
	t.Helper()
	pkt := s.read(t)
	if pkt.Type != client.PacketType_DATA {
		t.Fatalf("frontend received %v, want DATA %q", pkt.Type, want)
	}
	if got := string(pkt.GetData().Data); got != want {
		t.Fatalf("frontend received DATA %q, want %q", got, want)
	}
}

type grpcWriterTestFrontend struct {
	transport *pausableFrontendStream
	stream    *grpcFrontendStream
	cancel    context.CancelFunc
}

// establishGRPC registers a pending dial for a queued gRPC frontend and
// completes it with a DIAL_RSP from the backend dispatcher.
func (s *writerTestServer) establishGRPC(t *testing.T, id int64, queueSize int) *grpcWriterTestFrontend {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	transport := newPausableFrontendStream(ctx)
	stream := newGRPCFrontendStream(transport, queueSize)
	t.Cleanup(func() {
		cancel()
		stream.stop()
	})
	s.proxy.PendingDial.Add(id, &ProxyClientConnection{
		frontend: &Frontend{stream: stream, streamUID: fmt.Sprintf("grpc-stream-%d", id)},
		dialID:   id, backend: s.backend, start: time.Now(),
	})
	s.send(t, dialRspPkt(id, id))
	if pkt := transport.read(t); pkt.Type != client.PacketType_DIAL_RSP {
		t.Fatalf("first packet to frontend = %v, want DIAL_RSP", pkt.Type)
	}
	return &grpcWriterTestFrontend{transport: transport, stream: stream, cancel: cancel}
}

// TestGRPCFrontendWriterDelaysHOLByQueueDepth verifies that a gRPC frontend
// which stops reading no longer stalls the backend dispatcher for other
// frontends on the same agent until its own queue is full, and that no packet
// is lost or reordered once it resumes.
func TestGRPCFrontendWriterDelaysHOLByQueueDepth(t *testing.T) {
	const depth = 3
	s := newWriterTestServer(t)
	slow := s.establishGRPC(t, 1, depth)
	healthy := s.establishGRPC(t, 2, depth)

	// The writer takes the first packet off the queue and blocks in Send on
	// the stalled transport; the next depth packets sit in the queue.
	var want []string
	for i := 0; i <= depth; i++ {
		payload := fmt.Sprintf("slow-%d", i)
		want = append(want, payload)
		s.send(t, dataPkt(1, []byte(payload)))
		s.send(t, dataPkt(2, []byte("healthy")))
		healthy.transport.readData(t, "healthy")
	}
	if got := len(slow.stream.writeCh); got != depth {
		t.Fatalf("queue length = %d, want %d", got, depth)
	}
	if got := promtest.ToFloat64(metrics.Metrics.FullFrontendWriteQueues()); got != 1 {
		t.Fatalf("full queue gauge = %v, want 1", got)
	}

	// One more packet for the slow frontend has nowhere to go, so the
	// dispatcher blocks and the healthy frontend is starved: this is the
	// residual head-of-line blocking a bounded queue cannot remove.
	want = append(want, "overflow")
	s.send(t, dataPkt(1, []byte("overflow")))
	select {
	case s.packets <- dataPkt(2, []byte("too-early")):
		t.Fatal("dispatch did not block when the slow queue filled")
	case <-time.After(50 * time.Millisecond):
	}

	// Once the slow frontend reads again, everything drains in order.
	for _, payload := range want {
		slow.transport.readData(t, payload)
	}
	s.send(t, dataPkt(2, []byte("resumed")))
	healthy.transport.readData(t, "resumed")
}

// TestGRPCFrontendWriterPreservesOrder verifies that control packets and DATA
// leave the queue in the order the dispatcher produced them.
func TestGRPCFrontendWriterPreservesOrder(t *testing.T) {
	s := newWriterTestServer(t)
	f := s.establishGRPC(t, 1, 4)

	s.send(t, dataPkt(1, []byte("a")))
	s.send(t, dataPkt(1, []byte("b")))
	s.send(t, closeRspPkt(1, ""))

	f.transport.readData(t, "a")
	f.transport.readData(t, "b")
	if pkt := f.transport.read(t); pkt.Type != client.PacketType_CLOSE_RSP {
		t.Fatalf("last packet = %v, want CLOSE_RSP", pkt.Type)
	}
	if _, err := s.proxy.getFrontend("agent", 1); err == nil {
		t.Fatal("connection still established after CLOSE_RSP")
	}
}

func TestGRPCFrontendWriterZeroQueueIsSynchronous(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newPausableFrontendStream(ctx)
	stream := newGRPCFrontendStream(transport, 0)
	if stream.writeCh != nil || stream.writerDone != nil {
		t.Fatal("queue size 0 must not start a writer")
	}

	done := make(chan error, 1)
	go func() { done <- stream.Send(dataPkt(1, []byte("sync"))) }()
	select {
	case <-done:
		t.Fatal("Send returned before the transport accepted the packet")
	case <-time.After(50 * time.Millisecond):
	}
	transport.readData(t, "sync")
	if err := <-done; err != nil {
		t.Fatalf("Send returned %v", err)
	}
	stream.stop() // must not block without a writer
}

func TestGRPCFrontendWriterNegativeQueueSizeUsesDefault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	transport := newPausableFrontendStream(ctx)
	stream := newGRPCFrontendStream(transport, -1)
	t.Cleanup(func() { cancel(); stream.stop() })
	if got := cap(stream.writeCh); got != defaultFrontendWriteChannelSize {
		t.Fatalf("queue capacity = %d, want %d", got, defaultFrontendWriteChannelSize)
	}
}

// TestGRPCFrontendWriterStopDrainsQueuedPackets verifies that stop delivers
// what was queued before the handler returns, and that Send fails fast once
// the writer is gone instead of queueing behind it.
func TestGRPCFrontendWriterStopDrainsQueuedPackets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newPausableFrontendStream(ctx)
	stream := newGRPCFrontendStream(transport, 4)

	for i := 0; i < 3; i++ {
		if err := stream.Send(dataPkt(1, []byte(fmt.Sprint(i)))); err != nil {
			t.Fatalf("Send(%d) returned %v", i, err)
		}
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stream.stop()
	}()
	select {
	case <-stopped:
		t.Fatal("stop returned with packets still queued")
	case <-time.After(50 * time.Millisecond):
	}
	for i := 0; i < 3; i++ {
		transport.readData(t, fmt.Sprint(i))
	}
	waitWriterSignal(t, stopped, "writer stop after drain")

	if err := stream.Send(dataPkt(1, []byte("late"))); !errors.Is(err, errFrontendWriterStopped) {
		t.Fatalf("Send after stop returned %v, want %v", err, errFrontendWriterStopped)
	}
}

// TestGRPCFrontendWriterContextCancelUnblocksFullQueue verifies that a Send
// waiting for queue space is released when the frontend stream ends, and that
// both pressure gauges are cleared.
func TestGRPCFrontendWriterContextCancelUnblocksFullQueue(t *testing.T) {
	metrics.Metrics.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	transport := newPausableFrontendStream(ctx)
	stream := newGRPCFrontendStream(transport, 1)

	// First packet is taken by the writer and blocks in Send; the second
	// fills the queue; the third has to wait.
	for i := 0; i < 2; i++ {
		if err := stream.Send(dataPkt(1, []byte("x"))); err != nil {
			t.Fatalf("Send returned %v", err)
		}
	}
	blocked := make(chan error, 1)
	go func() { blocked <- stream.Send(dataPkt(1, []byte("waiting"))) }()
	waitForGauge(t, metrics.Metrics.BlockedFrontendWriteChannels(), 1)
	if got := promtest.ToFloat64(metrics.Metrics.FullFrontendWriteQueues()); got != 1 {
		t.Fatalf("full queue gauge = %v, want 1", got)
	}

	cancel()
	select {
	case err := <-blocked:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked Send returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Send was not released by stream cancellation")
	}
	waitWriterSignal(t, stream.writerDone, "writer exit on cancellation")
	waitForGauge(t, metrics.Metrics.BlockedFrontendWriteChannels(), 0)
	waitForGauge(t, metrics.Metrics.FullFrontendWriteQueues(), 0)
	stream.stop()
}

// TestGRPCFrontendWriterSendErrorStopsWriter verifies that a failed transport
// send ends the writer and surfaces the error to later callers rather than
// letting them block on a queue nobody drains.
func TestGRPCFrontendWriterSendErrorStopsWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newPausableFrontendStream(ctx)
	sendErr := errors.New("transport is closing")
	transport.sendErr = sendErr
	stream := newGRPCFrontendStream(transport, 1)

	if err := stream.Send(dataPkt(1, []byte("first"))); err != nil {
		t.Fatalf("first Send returned %v, want nil (queued)", err)
	}
	waitWriterSignal(t, stream.writerDone, "writer exit on send error")
	for i := 0; i < 3; i++ {
		if err := stream.Send(dataPkt(1, []byte("more"))); !errors.Is(err, sendErr) {
			t.Fatalf("Send after writer failure returned %v, want %v", err, sendErr)
		}
	}
	stream.stop()
}

// TestServerProxyDeliversQueuedPacketsBeforeReturning verifies, through the
// public Proxy entry point, that a packet queued by the frontend's own
// processing (here a DIAL_RSP error) is delivered before the handler returns
// and the gRPC stream is finished underneath it.
func TestServerProxyDeliversQueuedPacketsBeforeReturning(t *testing.T) {
	p := NewProxyServer("", []proxystrategies.ProxyStrategy{proxystrategies.ProxyStrategyDefault}, 1, &AgentTokenAuthenticationOptions{}, xfrChannelSize)
	ctx, cancel := context.WithCancel(metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(header.UserAgent, "test")))
	defer cancel()
	transport := &proxyEntryStream{pausableFrontendStream: newPausableFrontendStream(ctx)}
	transport.recv = make(chan *client.Packet, 2)
	transport.recv <- dialReqPkt(111)
	close(transport.recv)

	returned := make(chan error, 1)
	go func() { returned <- p.Proxy(transport) }()
	select {
	case err := <-returned:
		t.Fatalf("Proxy returned %v before the queued DIAL_RSP reached the frontend", err)
	case <-time.After(50 * time.Millisecond):
	}
	pkt := transport.read(t)
	if pkt.Type != client.PacketType_DIAL_RSP || pkt.GetDialResponse().Error == "" {
		t.Fatalf("frontend received %v, want failed DIAL_RSP", pkt)
	}
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("Proxy returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Proxy did not return after the queue drained")
	}
}

// proxyEntryStream feeds a fixed sequence of frontend packets to Proxy and
// then reports EOF, while still blocking sends like pausableFrontendStream.
type proxyEntryStream struct {
	*pausableFrontendStream
	recv chan *client.Packet
}

func (s *proxyEntryStream) Recv() (*client.Packet, error) {
	if pkt, ok := <-s.recv; ok {
		return pkt, nil
	}
	return nil, io.EOF
}

func waitForGauge(t *testing.T, gauge prometheus.Gauge, want float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := promtest.ToFloat64(gauge)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gauge = %v, want %v", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
