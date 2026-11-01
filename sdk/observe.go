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

package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"alpheus-dev.org/alpheus/internal/chunk"
)

// Limits of the error log (GO-120).
const (
	MaxLogEntryBytes = 4096
	MaxLogEntries    = 100000
)

// LogField is an extra field of an error log entry; see [Field].
type LogField struct {
	Key   string
	Value any
}

// Field adds a field to an error or warning, such as the row number.
// Refer to rows and fields rather than copying their content, which may
// hold personal data (GO-120).
func Field(key string, value any) LogField { return LogField{Key: key, Value: value} }

// Count adds n to the counter of a unit the component chooses, such as
// "rows" or "files" (lower-case letters, digits and _, at most 32; at most
// 16 units per step). The count is committed with the input chunk being
// handled, or for a source with the next output chunk it closes (GO-114,
// GO-115), so a source counts a chunk before it closes it. Count never
// blocks (GO-124).
func Count(ctx context.Context, unit string, n int64) {
	if o := observerFrom(ctx); o != nil {
		o.count(unit, n)
	}
}

// Error writes an error to the step's error log. Entries are committed
// with the input chunk being handled, so a restart never writes them twice
// (GO-117, GO-118).
func Error(ctx context.Context, code, message string, fields ...LogField) {
	if o := observerFrom(ctx); o != nil {
		o.log("error", code, message, fields)
	}
}

// Warn writes a warning to the step's error log; see [Error].
func Warn(ctx context.Context, code, message string, fields ...LogField) {
	if o := observerFrom(ctx); o != nil {
		o.log("warn", code, message, fields)
	}
}

// Progress reports a short text about how far the component has come. It
// is best effort and not committed (GO-122).
func Progress(ctx context.Context, text string) {
	if o := observerFrom(ctx); o != nil {
		o.progress(text)
	}
}

// reporter takes what a component reports: the observer in local mode, or
// the stream to the agent.
type reporter interface {
	count(unit string, n int64)
	log(level, code, message string, fields []LogField)
	progress(text string)
}

type observerKey struct{}

func withObserver(ctx context.Context, r reporter) context.Context {
	return context.WithValue(ctx, observerKey{}, r)
}

func observerFrom(ctx context.Context) reporter {
	r, _ := ctx.Value(observerKey{}).(reporter)
	return r
}

// checkCount validates a counter report against the units seen so far and
// adds its unit (GO-114).
func checkCount(units map[string]bool, unit string, n int64) error {
	switch {
	case !chunk.ValidUnit(unit):
		return fmt.Errorf("%w: counter unit %q: want lower-case letters, digits and _, at most 32", ErrUsage, unit)
	case n < 0:
		return fmt.Errorf("%w: counter %q: negative count %d", ErrUsage, unit, n)
	case !units[unit] && len(units) >= chunk.MaxUnits:
		return fmt.Errorf("%w: counter %q: a step can have at most %d units", ErrUsage, unit, chunk.MaxUnits)
	}
	units[unit] = true
	return nil
}

// LogEntry is one line of the error log, as JSON Lines (GO-117).
type LogEntry struct {
	Time    time.Time      `json:"time"`
	Level   string         `json:"level"`
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
	Step    string         `json:"step,omitempty"`
	Replica int            `json:"replica"`
	// Chunk is the input chunk being handled, as <port>/<producer>/<n>;
	// empty for a source.
	Chunk string `json:"chunk,omitempty"`
}

// observer collects what the component reports until it is committed. It
// is safe for use from several goroutines of the component.
type observer struct {
	mu       sync.Mutex
	step     string
	replica  int
	units    map[string]bool // every unit seen, for the limit of 16
	pending  map[string]int64
	entries  []LogEntry
	chunk    string
	err      error // the first invalid report; fails the next commit
	onReport func(text string)

	// Committed totals.
	counts        map[string]int64
	errors        int64
	warnings      int64
	errorsDropped int64
	logged        int64
}

func newObserver(step string, replica int, onProgress func(string)) *observer {
	return &observer{
		step:     step,
		replica:  replica,
		units:    map[string]bool{},
		pending:  map[string]int64{},
		counts:   map[string]int64{},
		onReport: onProgress,
	}
}

func (o *observer) count(unit string, n int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := checkCount(o.units, unit, n); err != nil {
		o.fail(err)
		return
	}
	o.pending[unit] += n
}

func (o *observer) log(level, code, message string, fields []LogField) {
	e := LogEntry{
		Time:    time.Now().UTC(),
		Level:   level,
		Code:    code,
		Message: message,
		Step:    o.step,
		Replica: o.replica,
	}
	if len(fields) > 0 {
		e.Fields = make(map[string]any, len(fields))
		for _, f := range fields {
			e.Fields[f.Key] = f.Value
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	e.Chunk = o.chunk
	o.entries = append(o.entries, e)
}

func (o *observer) progress(text string) {
	if o.onReport != nil {
		o.onReport(text)
	}
}

func (o *observer) fail(err error) {
	if o.err == nil {
		o.err = err
	}
}

// begin marks the input chunk that reports belong to.
func (o *observer) begin(chunk string) {
	o.mu.Lock()
	o.chunk = chunk
	o.mu.Unlock()
}

// commit takes what was reported since the last commit: the counts to put
// in a manifest entry and the log lines to write. Entries beyond the limits
// are dropped and counted (GO-120).
func (o *observer) commit() (counts map[string]int64, lines [][]byte, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return nil, nil, o.err
	}
	if len(o.pending) > 0 {
		counts = o.pending
		for u, n := range counts {
			o.counts[u] += n
		}
		o.pending = map[string]int64{}
	}
	for _, e := range o.entries {
		line, err := json.Marshal(e)
		if err != nil || len(line) > MaxLogEntryBytes || o.logged >= MaxLogEntries {
			o.errorsDropped++
			continue
		}
		if e.Level == "error" {
			o.errors++
		} else {
			o.warnings++
		}
		o.logged++
		lines = append(lines, line)
	}
	o.entries = nil
	o.chunk = ""
	return counts, lines, nil
}

// Summary is what a run's step reported in total, as local mode writes it
// to _summary.json.
type Summary struct {
	Counts        map[string]int64 `json:"counts"`
	Errors        int64            `json:"errors"`
	Warnings      int64            `json:"warnings"`
	ErrorsDropped int64            `json:"errorsDropped"`
}

func (o *observer) summary() Summary {
	o.mu.Lock()
	defer o.mu.Unlock()
	counts := make(map[string]int64, len(o.counts))
	for u, n := range o.counts {
		counts[u] = n
	}
	return Summary{Counts: counts, Errors: o.errors, Warnings: o.warnings, ErrorsDropped: o.errorsDropped}
}
