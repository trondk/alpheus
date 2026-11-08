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
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/replay"
	"alpheus-dev.org/alpheus/internal/store"
)

// captureFailure keeps what a replay of the failed attempt needs, as
// replay.Failure describes (GO-141).
// The input chunks are those assigned to the replica and not acked, the
// first maxOpen of each stream: the ones the attempt had open, and the
// ones a new attempt gets first. A failure of the storage itself keeps
// nothing, since it is not about the data. It is best effort, like the
// logs.
func (rc *Reconciler) captureFailure(ctx context.Context, r *v1alpha1.StreamRun, g *graph, s *v1alpha1.Step, rs *v1alpha1.ReplicaStatus, reason v1alpha1.Reason, msg string) {
	switch reason {
	case v1alpha1.ReasonStorageRefused, v1alpha1.ReasonStorageUnreachable, v1alpha1.ReasonStorageFull, v1alpha1.ReasonStorageError:
		return
	}
	st, err := rc.store(ctx, r)
	if err != nil {
		log.FromContext(ctx).Error(err, "capture failure: open storage")
		return
	}
	uid, replica := string(r.UID), int(rs.Index)
	prefix := chunk.FailurePrefix(r.Name, uid, s.Name, replica, int(rs.Attempt))
	f := replay.Failure{
		Version: 1, Run: r.Name, Namespace: r.Namespace, UID: uid, Step: s.Name,
		Replica: replica, Replicas: replicas(s), Attempt: int(rs.Attempt),
		Reason: reason, Message: msg, Time: time.Now().UTC(), Image: s.Image,
		Source: len(g.in[s.Name]) == 0, Inputs: []replay.Input{},
		Logs: fmt.Sprintf("s3://%s/%s", r.Spec.Storage.Bucket, chunk.LogsPrefix(r.Name, uid, s.Name, replica, int(rs.Attempt))),
	}
	if s.Settings != nil {
		f.Settings = json.RawMessage(s.Settings.Raw)
	}
	perStream := 8
	if s.MaxOpen != nil {
		perStream = int(*s.MaxOpen)
	}
	copySideInputs(ctx, st, r, g, s, prefix, &f)
	for _, e := range g.in[s.Name] {
		if e.Side {
			continue
		}
		port := e.Port
		if port == "" {
			port = "default"
		}
		for p := range replicas(g.steps[e.From]) {
			in := chunk.EdgeStream(uid, edgeName(e), p)
			m, err := readManifest(ctx, st, in)
			if err != nil || m == nil {
				continue
			}
			acks, err := readAcks(ctx, st, in.AckKey(s.Name, replica))
			if err != nil {
				continue
			}
			taken := 0
			for _, c := range m.Chunks {
				if taken >= perStream || len(f.Inputs) >= replay.MaxInputs {
					break
				}
				if chunk.Assign(c.N, p, replicas(s)) != replica || (acks != nil && acks.Has(c.N)) {
					continue
				}
				file := fmt.Sprintf("%s-%d-%06d.%s", edgeName(e), p, c.N, c.Ext)
				if err := store.Copy(ctx, st, in.ChunkKey(c.N, c.Ext), prefix+"input/"+file); err != nil {
					log.FromContext(ctx).Error(err, "capture failure: copy chunk", "chunk", file)
					continue
				}
				f.Inputs = append(f.Inputs, replay.Input{Port: port, Edge: edgeName(e), From: e.From, ProducerReplica: p, N: c.N, Bytes: c.Bytes, SHA256: c.SHA256, File: file})
				taken++
			}
		}
	}
	if err := putJSON(ctx, st, prefix+"failure.json", f); err != nil {
		log.FromContext(ctx).Error(err, "capture failure: write failure.json")
		return
	}
	for i := range r.Status.Steps {
		if r.Status.Steps[i].Name == s.Name {
			r.Status.Steps[i].Failures = fmt.Sprintf("s3://%s/%s", r.Spec.Storage.Bucket, chunk.FailurePrefix(r.Name, uid, s.Name, -1, 0))
		}
	}
}

// copySideInputs copies every chunk of the step's side inputs into a
// failure's input/, so a replay has all of its lookup data (GO-167). They
// do not count against the limits on inputs: a side input is small, and
// without all of it the replay would not be the attempt it replays.
func copySideInputs(ctx context.Context, st store.Store, r *v1alpha1.StreamRun, g *graph, s *v1alpha1.Step, prefix string, f *replay.Failure) {
	uid := string(r.UID)
	for _, e := range g.in[s.Name] {
		if !e.Side {
			continue
		}
		port := e.Port
		if port == "" {
			port = "default"
		}
		for p := range replicas(g.steps[e.From]) {
			in := chunk.EdgeStream(uid, edgeName(e), p)
			m, err := readManifest(ctx, st, in)
			if err != nil || m == nil {
				continue
			}
			chunks, err := allChunks(ctx, st, in, m)
			if err != nil {
				log.FromContext(ctx).Error(err, "capture failure: list a side input", "edge", edgeName(e))
				continue
			}
			for _, c := range chunks {
				file := fmt.Sprintf("%s-%d-%06d.%s", edgeName(e), p, c.N, c.Ext)
				if err := store.Copy(ctx, st, in.ChunkKey(c.N, c.Ext), prefix+"input/"+file); err != nil {
					log.FromContext(ctx).Error(err, "capture failure: copy chunk", "chunk", file)
					continue
				}
				f.Inputs = append(f.Inputs, replay.Input{Port: port, Edge: edgeName(e), From: e.From, ProducerReplica: p, N: c.N, Bytes: c.Bytes, SHA256: c.SHA256, File: file, Side: true})
			}
		}
	}
}

// keepSkipped keeps every input chunk that a replica which gave up with
// failurePolicy continue never processed, when the run has finished and
// no producer writes more (GO-97): copies in <replica>/skipped/input/,
// next to its failed attempts, and a failure.json that lists them, with
// the last attempt's reason, so alpheus replay --skipped runs the
// component on all of them. The chunks under runs/<uid>/ are deleted
// after ttlAfterFinished; these are kept for resultsTTL, like the
// results. Like captureFailure it is best effort: what cannot be copied
// is counted in notKept.
func (rc *Reconciler) keepSkipped(ctx context.Context, r *v1alpha1.StreamRun, g *graph) {
	var st store.Store
	uid := string(r.UID)
	for i := range r.Status.Steps {
		ss := &r.Status.Steps[i]
		s := g.steps[ss.Name]
		if s == nil || s.FailurePolicy != v1alpha1.FailureContinue {
			continue
		}
		for j := range ss.Replicas {
			rs := &ss.Replicas[j]
			if rs.Phase != v1alpha1.StepFailed || rs.FailedChunks == 0 {
				continue
			}
			if st == nil {
				var err error
				if st, err = rc.store(ctx, r); err != nil {
					log.FromContext(ctx).Error(err, "keep skipped chunks: open storage")
					return
				}
			}
			replica := int(rs.Index)
			prefix := chunk.SkippedPrefix(r.Name, uid, s.Name, replica)
			f := replay.Failure{
				Version: 1, Run: r.Name, Namespace: r.Namespace, UID: uid, Step: s.Name,
				Replica: replica, Replicas: replicas(s), Attempt: int(rs.Attempt),
				Reason: rs.Reason, Message: rs.Message, Time: time.Now().UTC(), Image: s.Image,
				Skipped: true, Inputs: []replay.Input{},
				Logs: fmt.Sprintf("s3://%s/%s", r.Spec.Storage.Bucket, chunk.LogsPrefix(r.Name, uid, s.Name, replica, int(rs.Attempt))),
			}
			if s.Settings != nil {
				f.Settings = json.RawMessage(s.Settings.Raw)
			}
			copySideInputs(ctx, st, r, g, s, prefix, &f)
			side := len(f.Inputs)
			var seen int64
			err := eachSkipped(ctx, st, r, g, s, replica, func(c skippedChunk) bool {
				seen++
				if len(f.Inputs)-side >= replay.MaxSkipped {
					return true
				}
				port := c.edge.Port
				if port == "" {
					port = "default"
				}
				file := fmt.Sprintf("%s-%d-%06d.%s", edgeName(c.edge), c.producer, c.chunk.N, c.chunk.Ext)
				if err := store.Copy(ctx, st, c.stream.ChunkKey(c.chunk.N, c.chunk.Ext), prefix+"input/"+file); err != nil {
					log.FromContext(ctx).Error(err, "keep skipped chunks: copy chunk", "chunk", file)
					return true
				}
				f.Inputs = append(f.Inputs, replay.Input{Port: port, Edge: edgeName(c.edge), From: c.edge.From, ProducerReplica: c.producer,
					N: c.chunk.N, Bytes: c.chunk.Bytes, SHA256: c.chunk.SHA256, File: file})
				return true
			})
			if err != nil {
				log.FromContext(ctx).Error(err, "keep skipped chunks: list them", "step", s.Name, "replica", replica)
			}
			f.NotKept = seen - int64(len(f.Inputs)-side)
			if err := putJSON(ctx, st, prefix+"failure.json", f); err != nil {
				log.FromContext(ctx).Error(err, "keep skipped chunks: write failure.json")
				continue
			}
			ss.Failures = fmt.Sprintf("s3://%s/%s", r.Spec.Storage.Bucket, chunk.FailurePrefix(r.Name, uid, s.Name, -1, 0))
			emit(ctx, corev1.EventTypeWarning, string(v1alpha1.ReasonFailedReplicas), "KeepSkipped",
				"step %s replica %d: kept %d chunks it never processed in %s", s.Name, replica, len(f.Inputs), prefix)
		}
	}
}
