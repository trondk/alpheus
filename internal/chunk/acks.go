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

import (
	"encoding/json"
	"fmt"
)

// AcksVersion is the ack file format this package reads and writes.
const AcksVersion = 1

// Acks is a consumer replica's ack file for one stream: the numbers of the
// chunks it has acked, in increasing order (GO-36).
type Acks struct {
	Version int     `json:"version"`
	Acked   []int64 `json:"acked"`
	// Epoch is the attempt of the agent that wrote the file, or above the
	// replica's last attempt when the controller closed it; an agent of an
	// older attempt stops at its next write (GO-67, GO-68).
	Epoch int64 `json:"epoch"`
	// Closed says the consumer replica failed for good with failurePolicy
	// continue. The controller closed the file, and the producer no
	// longer waits for it (GO-35, GO-97).
	Closed bool `json:"closed,omitempty"`
	// Observed is the consumer replica's committed counters and error log
	// totals, for a step without outgoing manifests (GO-72).
	Observed *Observed `json:"observed,omitempty"`
}

// NewAcks starts an empty ack file.
func NewAcks() *Acks { return &Acks{Version: AcksVersion, Acked: []int64{}} }

// Ack records chunk n. Chunks are acked in order (GO-71), so n must be
// larger than every chunk acked before; acking the last one again is a
// no-op, which makes a retried write after a crash harmless.
func (a *Acks) Ack(n int64) error {
	if k := len(a.Acked); k > 0 {
		last := a.Acked[k-1]
		if n == last {
			return nil
		}
		if n < last {
			return fmt.Errorf("%w: ack %d after %d", ErrInvalid, n, last)
		}
	}
	if n < 1 {
		return fmt.Errorf("%w: ack %d", ErrInvalid, n)
	}
	a.Acked = append(a.Acked, n)
	return nil
}

// Has reports whether chunk n is acked.
func (a *Acks) Has(n int64) bool {
	lo, hi := 0, len(a.Acked)
	for lo < hi {
		mid := (lo + hi) / 2
		switch {
		case a.Acked[mid] == n:
			return true
		case a.Acked[mid] < n:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return false
}

// Last is the highest acked chunk, or 0 when none is acked.
func (a *Acks) Last() int64 {
	if len(a.Acked) == 0 {
		return 0
	}
	return a.Acked[len(a.Acked)-1]
}

// ParseAcks decodes and validates an ack file.
func ParseAcks(data []byte) (*Acks, error) {
	var a Acks
	if err := decodeStrict(data, &a); err != nil {
		return nil, fmt.Errorf("parse acks: %w", err)
	}
	if a.Version != AcksVersion {
		return nil, fmt.Errorf("parse acks: %w: version %d", ErrVersion, a.Version)
	}
	if a.Acked == nil {
		a.Acked = []int64{}
	}
	for i, n := range a.Acked {
		if n < 1 || (i > 0 && n <= a.Acked[i-1]) {
			return nil, fmt.Errorf("parse acks: %w: %d at position %d", ErrInvalid, n, i)
		}
	}
	if a.Observed != nil {
		if err := a.Observed.Validate(); err != nil {
			return nil, fmt.Errorf("parse acks: %w", err)
		}
	}
	return &a, nil
}

// Marshal encodes the ack file.
func (a *Acks) Marshal() ([]byte, error) { return json.Marshal(a) }
