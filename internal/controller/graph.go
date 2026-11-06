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
	"errors"
	"fmt"
	"strings"
	"time"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
)

// graph is a validated StreamRun graph.
type graph struct {
	steps map[string]*v1alpha1.Step
	order []string // topological: producers before consumers
	in    map[string][]v1alpha1.Edge
	out   map[string][]v1alpha1.Edge
}

// newGraph validates the steps and edges (SPEC 6.1 step 1): names unique,
// edges between existing steps, at most one edge per output and step, and
// no cycles. The CRD defaults output and port.
func newGraph(spec *v1alpha1.StreamRunSpec) (*graph, error) {
	g := &graph{
		steps: map[string]*v1alpha1.Step{},
		in:    map[string][]v1alpha1.Edge{},
		out:   map[string][]v1alpha1.Edge{},
	}
	var errs []error
	for i := range spec.Steps {
		s := &spec.Steps[i]
		if g.steps[s.Name] != nil {
			errs = append(errs, fmt.Errorf("step %q is defined twice", s.Name))
		}
		g.steps[s.Name] = s
	}
	names := map[string]bool{}
	for _, e := range spec.Edges {
		e = withDefaults(e)
		name := edgeName(e)
		switch {
		case g.steps[e.From] == nil:
			errs = append(errs, fmt.Errorf("edge %s: no step %q", name, e.From))
			continue
		case g.steps[e.To] == nil:
			errs = append(errs, fmt.Errorf("edge %s: no step %q", name, e.To))
			continue
		case e.From == e.To:
			errs = append(errs, fmt.Errorf("edge %s: a step cannot read its own output", name))
			continue
		case names[name]:
			errs = append(errs, fmt.Errorf("edge %s is defined twice", name))
			continue
		}
		names[name] = true
		g.out[e.From] = append(g.out[e.From], e)
		g.in[e.To] = append(g.in[e.To], e)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	// Kahn's algorithm, in the order the steps are listed.
	indegree := map[string]int{}
	for to, edges := range g.in {
		indegree[to] = len(edges)
	}
	var queue []string
	for _, s := range spec.Steps {
		if indegree[s.Name] == 0 {
			queue = append(queue, s.Name)
		}
	}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		g.order = append(g.order, name)
		for _, e := range g.out[name] {
			if indegree[e.To]--; indegree[e.To] == 0 {
				queue = append(queue, e.To)
			}
		}
	}
	if len(g.order) != len(spec.Steps) {
		var cycle []string
		for _, s := range spec.Steps {
			if indegree[s.Name] > 0 {
				cycle = append(cycle, s.Name)
			}
		}
		return nil, fmt.Errorf("the edges form a cycle through %s", strings.Join(cycle, ", "))
	}
	return g, nil
}

// ValidateGraph checks a run's steps and edges as the controller does
// before it starts the run, for alpheus run and validate.
func ValidateGraph(spec *v1alpha1.StreamRunSpec) error {
	_, err := newGraph(spec)
	return err
}

// assigned reports whether chunk n of producer replica p on edge e goes to
// the consumer's replica: by GO-76, or always for a side input (GO-167).
func assigned(e v1alpha1.Edge, n int64, p, consumers, replica int) bool {
	return e.Side || chunk.Assign(n, p, consumers) == replica
}

// EdgeName is an edge's name in storage and in status.edges.
func EdgeName(e v1alpha1.Edge) string { return edgeName(withDefaults(e)) }

// Replicas is how many replicas a step runs.
func Replicas(s *v1alpha1.Step) int { return replicas(s) }

func withDefaults(e v1alpha1.Edge) v1alpha1.Edge {
	if e.Output == "" {
		e.Output = chunk.DefaultOutput
	}
	if e.Port == "" {
		e.Port = "default"
	}
	return e
}

func edgeName(e v1alpha1.Edge) string { return chunk.EdgeName(e.From, e.Output, e.To) }

func replicas(s *v1alpha1.Step) int {
	if s.Replicas == nil || *s.Replicas < 1 {
		return 1
	}
	return int(*s.Replicas)
}

func maxRestarts(s *v1alpha1.Step) int32 {
	if s.MaxRestarts == nil {
		return 3
	}
	return *s.MaxRestarts
}

// storageTimeout is how long agents wait for the storage (GO-136). The API
// server sets the default; a run stored before the field existed has none.
func storageTimeout(r *v1alpha1.StreamRun) time.Duration {
	if r.Spec.Storage.Timeout == nil {
		return 10 * time.Minute
	}
	return r.Spec.Storage.Timeout.Duration
}
