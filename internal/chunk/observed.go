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

package chunk

import "fmt"

// MaxLogEntries is how many errors and warnings a replica's error log keeps;
// later entries are counted as dropped (GO-120).
const MaxLogEntries = 100000

// Observed is a replica's committed counters and error log totals,
// cumulative over the run (GO-115, GO-118). It is written in the same write
// as consumed, in every outgoing manifest the commit changes, and in the
// ack files of a step whose ack files are its commit point (GO-72). Seq
// grows with every commit that changes it, so the copy with the highest
// Seq is the replica's committed state.
type Observed struct {
	Seq           int64            `json:"seq"`
	Counts        map[string]int64 `json:"counts"`
	Errors        int64            `json:"errors"`
	Warnings      int64            `json:"warnings"`
	ErrorsDropped int64            `json:"errorsDropped"`
	// Segments is how many error log segments are committed: log-000001
	// up to this one. Later segments are left from a crash and deleted.
	Segments int `json:"segments"`
}

// Newest returns the copy with the highest Seq, or nil when all are nil.
func Newest(all ...*Observed) *Observed {
	var best *Observed
	for _, o := range all {
		if o != nil && (best == nil || o.Seq > best.Seq) {
			best = o
		}
	}
	return best
}

// Validate checks totals read from storage before the agent continues from
// them: nothing negative, counters as in a chunk, at most MaxLogEntries
// entries, and at most one segment per entry, since a segment is written
// only with entries in it (GO-118).
func (o *Observed) Validate() error {
	if o.Seq < 0 || o.Errors < 0 || o.Warnings < 0 || o.ErrorsDropped < 0 || o.Segments < 0 {
		return fmt.Errorf("%w: observed has a negative total", ErrInvalid)
	}
	if o.Errors > MaxLogEntries || o.Warnings > MaxLogEntries-o.Errors {
		return fmt.Errorf("%w: observed has %d errors and %d warnings, at most %d", ErrInvalid, o.Errors, o.Warnings, MaxLogEntries)
	}
	if int64(o.Segments) > o.Errors+o.Warnings {
		return fmt.Errorf("%w: observed has %d log segments for %d entries", ErrInvalid, o.Segments, o.Errors+o.Warnings)
	}
	if len(o.Counts) > MaxUnits {
		return fmt.Errorf("%w: observed has %d counter units", ErrInvalid, len(o.Counts))
	}
	for unit, n := range o.Counts {
		if !ValidUnit(unit) || n < 0 {
			return fmt.Errorf("%w: observed counter %q = %d", ErrInvalid, unit, n)
		}
	}
	return nil
}
