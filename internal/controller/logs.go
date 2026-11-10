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
	"bytes"
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
)

// maxKeptLog is how much of each container's log is kept for a failed
// attempt: its end, at most this many bytes (GO-140).
const maxKeptLog = 4 << 20

// keepLogs saves the logs of a failed attempt's containers in the bucket,
// next to the step's error log, before the pod is replaced or deleted
// (GO-140): results/<run>/<uid>/_logs/<step>/<replica>/attempt-<n>/
// with component.log, agent.log and, when the agent restarted in place,
// agent-previous.log. It is best effort: a log that cannot be read or
// saved is logged, and the run goes on.
func (rc *Reconciler) keepLogs(ctx context.Context, r *v1alpha1.StreamRun, s *v1alpha1.Step, rs *v1alpha1.ReplicaStatus, pod *corev1.Pod) {
	if rc.Logs == nil {
		return
	}
	st, err := rc.store(ctx, r)
	if err != nil {
		log.FromContext(ctx).Error(err, "keep logs: open storage")
		return
	}
	prefix := chunk.LogsPrefix(r.Name, string(r.UID), s.Name, int(rs.Index), int(rs.Attempt))
	type logFile struct {
		container string
		previous  bool
		name      string
	}
	files := []logFile{{ComponentContainer, false, "component.log"}, {AgentContainer, false, "agent.log"}}
	if agentRestarts(pod) > 0 {
		files = append(files, logFile{AgentContainer, true, "agent-previous.log"})
	}
	kept := false
	for _, f := range files {
		data, err := rc.readLog(ctx, pod, f.container, f.previous)
		if err != nil {
			log.FromContext(ctx).Error(err, "keep logs: read", "pod", pod.Name, "container", f.container)
			continue
		}
		if err := st.Put(ctx, prefix+f.name, bytes.NewReader(data), int64(len(data))); err != nil {
			log.FromContext(ctx).Error(err, "keep logs: save", "pod", pod.Name, "container", f.container)
			continue
		}
		kept = true
	}
	if kept {
		for i := range r.Status.Steps {
			if r.Status.Steps[i].Name == s.Name {
				r.Status.Steps[i].Logs = fmt.Sprintf("s3://%s/%s", ResultsBucket(r), chunk.LogsPrefix(r.Name, string(r.UID), s.Name, -1, 0))
			}
		}
	}
}

// readLog reads the end of a container's log, at most maxKeptLog bytes.
func (rc *Reconciler) readLog(ctx context.Context, pod *corev1.Pod, container string, previous bool) ([]byte, error) {
	rd, err := rc.Logs(ctx, pod.Namespace, pod.Name, container, previous)
	if err != nil {
		return nil, err
	}
	defer rd.Close()
	// Keep the last maxKeptLog bytes: the end of a log says why it failed.
	var buf []byte
	chunkBuf := make([]byte, 64<<10)
	for {
		n, err := rd.Read(chunkBuf)
		buf = append(buf, chunkBuf[:n]...)
		if len(buf) > 2*maxKeptLog {
			buf = append(buf[:0], buf[len(buf)-maxKeptLog:]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if len(buf) > maxKeptLog {
		buf = buf[len(buf)-maxKeptLog:]
	}
	return buf, nil
}
