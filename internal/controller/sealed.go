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
	"fmt"
	"maps"
	"strings"
	"sync"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
)

// totals are the bytes and counters of a set of chunks.
type totals struct {
	bytes  int64
	counts map[string]int64
}

func (t *totals) add(c chunk.Chunk) {
	t.bytes += c.Bytes
	for u, n := range c.Counts {
		if t.counts == nil {
			t.counts = map[string]int64{}
		}
		t.counts[u] += n
	}
}

func (t *totals) addAll(o totals) {
	t.bytes += o.bytes
	for u, n := range o.counts {
		if t.counts == nil {
			t.counts = map[string]int64{}
		}
		t.counts[u] += n
	}
}

// sealedTotals keeps the totals of sealed segments (GO-32). A segment
// never changes once written, so each is read once, not on every resync.
// The keys hold the run's UID, which finalize drops.
type sealedTotals struct {
	mu   sync.Mutex
	segs map[string]totals
}

func (s *sealedTotals) get(key string) (totals, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.segs[key]
	return t, ok
}

func (s *sealedTotals) put(key string, t totals) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.segs == nil {
		s.segs = map[string]totals{}
	}
	s.segs[key] = t
}

// forget drops the segments of one run.
func (s *sealedTotals) forget(uid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	maps.DeleteFunc(s.segs, func(k string, _ totals) bool { return strings.Contains(k, "/"+uid+"/") })
}

// streamTotals sums every committed chunk of a stream: its sealed
// segments and the chunks in the manifest.
func (rc *Reconciler) streamTotals(ctx context.Context, st store.Store, s chunk.Stream, m *chunk.Manifest) (totals, error) {
	var t totals
	for i := range m.Segments {
		key := s.SegmentKey(i)
		seg, ok := rc.sealed.get(key)
		if !ok {
			data, err := store.ReadAll(ctx, st, key)
			if err != nil {
				return totals{}, fmt.Errorf("%s: %w", key, err)
			}
			parsed, err := chunk.ParseSegment(data, i)
			if err != nil {
				return totals{}, fmt.Errorf("%s: %w", key, err)
			}
			for _, c := range parsed.Chunks {
				seg.add(c)
			}
			rc.sealed.put(key, seg)
		}
		t.addAll(seg)
	}
	for _, c := range m.Chunks {
		t.add(c)
	}
	return t, nil
}
