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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// statusLimit remembers when each run's status was last written, so a
// run's status is written at most once per interval (GO-22).
type statusLimit struct {
	mu   sync.Mutex
	last map[types.NamespacedName]time.Time
}

// wait is how long a pass of run must wait before it may write the
// status again; 0 means now.
func (l *statusLimit) wait(run types.NamespacedName, now time.Time, interval time.Duration) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	t, ok := l.last[run]
	if !ok {
		return 0
	}
	return max(0, t.Add(interval).Sub(now))
}

// wrote records that run's status was written at now.
func (l *statusLimit) wrote(run types.NamespacedName, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.last == nil {
		l.last = map[types.NamespacedName]time.Time{}
	}
	l.last[run] = now
}

// forget drops a run that is gone.
func (l *statusLimit) forget(run types.NamespacedName) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.last, run)
}
