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
	"errors"
	"sync"

	"k8s.io/klog/v2"

	"sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server/metrics"
)

// errFrontendWriterStopped is returned by Send once the stream's writer has
// exited, either because the stream ended or because an earlier send failed.
var errFrontendWriterStopped = errors.New("frontend writer stopped")

// grpcFrontendStream adapts a gRPC frontend stream to ProxyStream and gives it
// a bounded write queue drained by a dedicated goroutine.
//
// The backend receive loop in serveRecvBackend is shared by every connection
// an agent serves. Without the queue it calls stream.Send directly, so one
// frontend whose HTTP/2 flow-control window is exhausted stalls the delivery
// of packets to every other frontend using that agent. With the queue, Send
// hands the packet to the per-stream writer and returns; the receive loop only
// blocks once this stream's queue is full. This mirrors the queue the
// HTTP-CONNECT adapter uses (see httpConnectStream), so both frontend modes
// behave the same way.
//
// A queue size of zero disables queueing and Send writes synchronously.
type grpcFrontendStream struct {
	client.ProxyService_ProxyServer

	// writeCh carries packets in wire order to the writer. It is nil when
	// queueing is disabled and is never closed, so Send never panics.
	writeCh chan *client.Packet
	// stopCh is closed by stop to ask the writer to drain what is already
	// queued and then exit.
	stopCh   chan struct{}
	stopOnce sync.Once
	// writerDone is closed when the writer exits. Send selects on it so that
	// a packet is never queued behind a writer which will not run again.
	writerDone chan struct{}
	writerErr  error // set before writerDone is closed

	writeMetrics frontendWriteQueueMetrics
}

var _ ProxyStream = &grpcFrontendStream{}

// newGRPCFrontendStream wraps stream. When queueSize is positive the writer
// goroutine is started immediately; the caller must call stop when the stream
// handler returns.
func newGRPCFrontendStream(stream client.ProxyService_ProxyServer, queueSize int) *grpcFrontendStream {
	if queueSize < 0 {
		queueSize = defaultFrontendWriteChannelSize
	}
	g := &grpcFrontendStream{ProxyService_ProxyServer: stream}
	if queueSize == 0 {
		return g
	}
	g.writeCh = make(chan *client.Packet, queueSize)
	g.stopCh = make(chan struct{})
	g.writerDone = make(chan struct{})
	go g.serveWrites()
	return g
}

// Send queues pkt for the writer, or writes it synchronously when queueing is
// disabled. When the queue is full, Send blocks until there is room, the
// stream context ends, or the writer exits. It never drops a packet.
func (g *grpcFrontendStream) Send(pkt *client.Packet) error {
	if g.writeCh == nil {
		return g.ProxyService_ProxyServer.Send(pkt)
	}

	ctx := g.Context()
	select {
	case <-g.writerDone:
		return g.writerErr
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case g.writeCh <- pkt:
		g.updateFrontendWriteQueueMetric()
		return nil
	case <-g.writerDone:
		return g.writerErr
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	klog.V(2).InfoS("Frontend write channel is full", "packetType", pkt.Type, "connectionID", packetConnectID(pkt))
	blocked := metrics.Metrics.BlockedFrontendWriteChannels()
	blocked.Inc()
	defer blocked.Dec()

	select {
	case g.writeCh <- pkt:
		g.updateFrontendWriteQueueMetric()
		return nil
	case <-g.writerDone:
		return g.writerErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// serveWrites owns all sends on the underlying gRPC stream. It exits when the
// stream context ends, when a send fails (the stream is then unusable), or
// once stop has been called and the queue is empty.
func (g *grpcFrontendStream) serveWrites() {
	defer close(g.writerDone)
	defer g.stopFrontendWriteQueueMetric()

	ctx := g.Context()
	for {
		select {
		case pkt := <-g.writeCh:
			g.updateFrontendWriteQueueMetric()
			if err := g.ProxyService_ProxyServer.Send(pkt); err != nil {
				klog.V(2).InfoS("Queued send to gRPC frontend failed", "packetType", pkt.Type, "connectionID", packetConnectID(pkt), "error", err)
				g.writerErr = err
				return
			}
		case <-ctx.Done():
			g.writerErr = ctx.Err()
			return
		case <-g.stopCh:
			// Drain what was queued before the stop. Later Sends observe
			// writerDone once the drain finishes; a packet queued during the
			// drain is delivered if it lands before the queue is seen empty.
			for {
				select {
				case pkt := <-g.writeCh:
					g.updateFrontendWriteQueueMetric()
					if err := g.ProxyService_ProxyServer.Send(pkt); err != nil {
						klog.V(2).InfoS("Queued send to gRPC frontend failed", "packetType", pkt.Type, "connectionID", packetConnectID(pkt), "error", err)
						g.writerErr = err
						return
					}
				case <-ctx.Done():
					g.writerErr = ctx.Err()
					return
				default:
					g.writerErr = errFrontendWriterStopped
					return
				}
			}
		}
	}
}

// stop asks the writer to deliver what is already queued and then exit, and
// waits for it. Call it when the stream handler is about to return, so that a
// CLOSE_RSP or DIAL_RSP queued moments earlier still reaches the frontend. A
// writer blocked in Send is released when the stream context ends.
func (g *grpcFrontendStream) stop() {
	if g.writeCh == nil {
		return
	}
	g.stopOnce.Do(func() { close(g.stopCh) })
	<-g.writerDone
}

func (g *grpcFrontendStream) updateFrontendWriteQueueMetric() {
	g.writeMetrics.observe(g.writeCh)
}

func (g *grpcFrontendStream) stopFrontendWriteQueueMetric() {
	g.writeMetrics.stop()
}

// packetConnectID returns the connection a frontend-bound packet belongs to,
// or 0 for packets which carry no connection ID.
func packetConnectID(pkt *client.Packet) int64 {
	switch pkt.Type {
	case client.PacketType_DATA:
		return pkt.GetData().ConnectID
	case client.PacketType_CLOSE_RSP:
		return pkt.GetCloseResponse().ConnectID
	case client.PacketType_DIAL_RSP:
		return pkt.GetDialResponse().ConnectID
	default:
		return 0
	}
}
