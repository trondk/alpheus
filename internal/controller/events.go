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

	"k8s.io/client-go/tools/events"

	"alpheus-dev.org/alpheus/api/v1alpha1"
)

// event is a Kubernetes event about a run (GO-139).
type event struct {
	typ, reason, action, note string
}

// eventBuffer holds the events of one reconcile pass. They are recorded
// only once the pass's status is written, so a pass whose write conflicts
// and is repeated does not record them twice.
type eventBuffer struct {
	events []event
	after  []func() // metrics, changed with the events
}

type eventsKey struct{}

func withEvents(ctx context.Context, b *eventBuffer) context.Context {
	return context.WithValue(ctx, eventsKey{}, b)
}

// emit adds an event to the pass's buffer; without one it does nothing.
func emit(ctx context.Context, typ, reason, action, format string, args ...any) {
	if b, ok := ctx.Value(eventsKey{}).(*eventBuffer); ok {
		b.events = append(b.events, event{typ: typ, reason: reason, action: action, note: fmt.Sprintf(format, args...)})
	}
}

// afterWrite runs f once the pass's status is written, as events are
// recorded; without a buffer it does nothing.
func afterWrite(ctx context.Context, f func()) {
	if b, ok := ctx.Value(eventsKey{}).(*eventBuffer); ok {
		b.after = append(b.after, f)
	}
}

func (b *eventBuffer) flush(rec events.EventRecorder, r *v1alpha1.StreamRun) {
	for _, f := range b.after {
		f()
	}
	b.after = nil
	if rec == nil {
		b.events = nil
		return
	}
	for _, e := range b.events {
		note := e.note
		if len(note) > 1024 { // the API's limit for a note
			note = note[:1021] + "..."
		}
		rec.Eventf(r, nil, e.typ, e.reason, e.action, "%s", note)
	}
	b.events = nil
}
