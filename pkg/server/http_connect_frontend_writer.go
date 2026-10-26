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
	"net"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog/v2"

	commonmetrics "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/pkg/common/metrics"
	"sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server/metrics"
)

const defaultFrontendWriteChannelSize = 10

// frontendWriteQueueMetrics tracks one frontend write queue's contribution to
// the full-queue gauge. It is shared by the gRPC and HTTP-CONNECT adapters.
type frontendWriteQueueMetrics struct {
	mu   sync.Mutex
	full bool // guarded by mu

	fullGauge prometheus.Gauge // guarded by mu; keeps Inc and Dec on the same GaugeVec child
	stopped   bool             // guarded by mu
}

// observe samples whether queue is currently full and moves the gauge on each
// transition. It is a no-op once stop has been called.
func (state *frontendWriteQueueMetrics) observe(queue chan *client.Packet) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.stopped {
		return
	}
	// Sample under the lock, even when the caller's own operation did not
	// change fullness: a concurrent update which sampled earlier could
	// otherwise publish stale occupancy after we return.
	full := len(queue) == cap(queue)
	if state.full == full {
		return
	}

	state.full = full
	if full {
		state.fullGauge = metrics.Metrics.FullFrontendWriteQueues()
		state.fullGauge.Inc()
	} else {
		state.fullGauge.Dec()
		state.fullGauge = nil
	}
}

// stop releases the durable full-queue accounting and ignores later updates.
func (state *frontendWriteQueueMetrics) stop() {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.stopped {
		return
	}

	state.stopped = true
	if state.full {
		state.full = false
		state.fullGauge.Dec()
		state.fullGauge = nil
	}
}

// enqueueWrite gives each HTTP-CONNECT stream a bounded response buffer behind
// the common Frontend.Send path. When it fills, Send blocks without dropping
// packets or disconnecting a slow client. This delays, rather than eliminates,
// head-of-line blocking of the shared agent receive loop. gRPC frontends get
// the same treatment from grpcFrontendStream.
func (h *httpConnectStream) enqueueWrite(pkt *client.Packet) error {
	select {
	case <-h.closed:
		return net.ErrClosed
	default:
	}

	select {
	case h.writeCh <- pkt:
		h.updateFrontendWriteQueueMetric()
		return nil
	case <-h.closed:
		return net.ErrClosed
	default:
	}

	klog.V(2).InfoS("Frontend write channel is full", "host", h.host, "connectionID", h.connectID)
	blocked := metrics.Metrics.BlockedFrontendWriteChannels()
	blocked.Inc()
	defer blocked.Dec()

	select {
	case h.writeCh <- pkt:
		h.updateFrontendWriteQueueMetric()
		return nil
	case <-h.closed:
		return net.ErrClosed
	}
}

// serveFrontendWrites owns established socket writes. The HTTP handshake is
// completed before this goroutine starts, and CLOSE_RSP follows queued DATA.
// The socket is closed only on ordered close, an actual write error, or abort.
func (h *httpConnectStream) serveFrontendWrites() {
	defer close(h.writerDone)
	defer h.close()

	for {
		select {
		case <-h.closed:
			return
		default:
		}

		select {
		case pkt := <-h.writeCh:
			h.updateFrontendWriteQueueMetric()
			if pkt.Type == client.PacketType_CLOSE_RSP {
				return
			}
			if _, err := h.conn.Write(pkt.GetData().Data); err != nil {
				if !h.isClosed() {
					metrics.Metrics.ObserveStreamError(commonmetrics.SegmentToClient, err, pkt.Type)
					klog.ErrorS(err, "Queued send to HTTP-CONNECT frontend failed",
						"host", h.host, "dialID", h.dialID, "connectionID", h.connectID)
				}
				return
			}
		case <-h.closed:
			return
		}
	}
}

func (h *httpConnectStream) updateFrontendWriteQueueMetric() {
	h.writeMetrics.observe(h.writeCh)
}

// stopFrontendWriteQueueMetric releases the durable full-queue accounting.
// Stream closure releases it even when the socket writer is still blocked.
func (h *httpConnectStream) stopFrontendWriteQueueMetric() {
	h.writeMetrics.stop()
}
