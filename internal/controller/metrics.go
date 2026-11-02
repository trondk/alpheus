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

package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// The controller's metrics, served with controller-runtime's own on
// /metrics (GO-51). Registering them is the one global state GO-62 allows.
// They change only after the status they describe is written, so a pass
// that is repeated does not count twice.
var (
	runsStarted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "alpheus_runs_started_total",
		Help: "Runs that started.",
	}, []string{"namespace"})
	runsFinished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "alpheus_runs_finished_total",
		Help: "Runs that finished, by phase and reason (GO-138).",
	}, []string{"namespace", "phase", "reason"})
	stepRestarts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "alpheus_step_restarts_total",
		Help: "Failed attempts that were restarted, by step and reason (GO-138).",
	}, []string{"namespace", "step", "reason"})
	edgeLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "alpheus_edge_lag",
		Help: "Chunks committed on an edge and not yet acked, while the run runs.",
	}, []string{"namespace", "run", "edge"})
)

func init() {
	metrics.Registry.MustRegister(runsStarted, runsFinished, stepRestarts, edgeLag)
}
