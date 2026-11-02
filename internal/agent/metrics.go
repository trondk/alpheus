/*
Copyright The Alpheus Authors.

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

package agent

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"alpheus-dev.org/alpheus/internal/chunk"
)

// Metrics are the agent's live metrics, served on /metrics (GO-51,
// GO-116). They are best effort and start from zero when the agent
// restarts; the run's status has the exact, committed numbers. A nil
// *Metrics records nothing.
type Metrics struct {
	counts       *prometheus.CounterVec
	chunks       *prometheus.CounterVec
	bytes        *prometheus.CounterVec
	backpressure prometheus.Counter
	retries      prometheus.Counter
	entries      *prometheus.CounterVec
}

// NewMetrics registers the agent's metrics in reg, labelled with the run,
// step and replica of cfg.
func NewMetrics(reg prometheus.Registerer, cfg Config) *Metrics {
	labels := prometheus.Labels{"run": cfg.Run, "step": cfg.Step, "replica": strconv.Itoa(cfg.Replica)}
	reg = prometheus.WrapRegistererWith(labels, reg)
	m := &Metrics{
		counts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "alpheus_component_count_total",
			Help: "What the component counted, per unit, live (GO-116).",
		}, []string{"unit"}),
		chunks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "alpheus_chunks_total",
			Help: "Chunks downloaded (in) and uploaded (out).",
		}, []string{"direction"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "alpheus_bytes_total",
			Help: "Bytes of chunks downloaded (in) and uploaded (out).",
		}, []string{"direction"}),
		backpressure: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "alpheus_backpressure_seconds_total",
			Help: "Time the component's output waited for consumers to catch up (GO-35).",
		}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "alpheus_storage_retries_total",
			Help: "Storage requests retried because the storage was unreachable (GO-136).",
		}),
		entries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "alpheus_component_log_entries_total",
			Help: "Errors and warnings the component reported, per level (GO-117).",
		}, []string{"level"}),
	}
	reg.MustRegister(m.counts, m.chunks, m.bytes, m.backpressure, m.retries, m.entries)
	return m
}

func (m *Metrics) count(unit string, n int64) {
	// An invalid count fails the agent (GO-115); it is not a metric, and a
	// counter cannot go down.
	if m != nil && n > 0 && chunk.ValidUnit(unit) {
		m.counts.WithLabelValues(unit).Add(float64(n))
	}
}

func (m *Metrics) moved(direction string, bytes int64) {
	if m != nil {
		m.chunks.WithLabelValues(direction).Inc()
		m.bytes.WithLabelValues(direction).Add(float64(bytes))
	}
}

func (m *Metrics) waited(seconds float64) {
	if m != nil {
		m.backpressure.Add(seconds)
	}
}

// Retried counts a storage request that will be retried.
func (m *Metrics) Retried() {
	if m != nil {
		m.retries.Inc()
	}
}

func (m *Metrics) entry(level string) {
	if level != "error" && level != "warn" {
		level = "other" // the component chooses the level; keep the labels few
	}
	if m != nil {
		m.entries.WithLabelValues(level).Inc()
	}
}
