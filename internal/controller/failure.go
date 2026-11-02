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
	for _, e := range g.in[s.Name] {
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
				f.Inputs = append(f.Inputs, replay.Input{Port: port, Edge: edgeName(e), ProducerReplica: p, N: c.N, Bytes: c.Bytes, SHA256: c.SHA256, File: file})
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
