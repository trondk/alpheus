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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

// ManifestVersion is the manifest format this package reads and writes.
const ManifestVersion = 1

// MaxUnits is how many counter units a step can have (GO-114).
const MaxUnits = 16

var unitPattern = regexp.MustCompile(`^[a-z0-9_]{1,32}$`)

// ValidUnit reports whether unit can name a counter: lower-case letters,
// digits and _, at most 32 (GO-114).
func ValidUnit(unit string) bool { return unitPattern.MatchString(unit) }

// SegmentSize is how many chunks manifest.json holds before they are sealed
// into a segment (GO-32). Every sealed segment holds exactly this many.
const SegmentSize = 1000

var (
	// ErrVersion means a manifest, segment or ack file has a format version
	// this program does not know.
	ErrVersion = errors.New("unknown format version")
	// ErrInvalid means a manifest, segment or ack file breaks the format's
	// rules.
	ErrInvalid = errors.New("invalid file")
	// ErrEOS means a chunk was added to a stream that has ended.
	ErrEOS = errors.New("stream has ended")
	// ErrStaleEpoch means a manifest was written by an older attempt than
	// the reader's own (GO-68).
	ErrStaleEpoch = errors.New("manifest from an older attempt")
	// ErrNewerEpoch means a newer attempt owns the stream, so this one must
	// stop at once (GO-68).
	ErrNewerEpoch = errors.New("manifest from a newer attempt")
)

// Manifest lists the committed chunks of one stream. A chunk exists only
// once it is in the manifest (SPEC 7.3).
type Manifest struct {
	Version int    `json:"version"`
	Run     string `json:"run"`
	UID     string `json:"uid"`
	// Edge is the edge name, or <step>.<output> for a result.
	Edge    string `json:"edge"`
	Replica int    `json:"replica"`
	// Epoch is the attempt number of the agent that wrote the manifest
	// (GO-67).
	Epoch int64 `json:"epoch"`
	// Segments are the sealed segments, oldest first (GO-32).
	Segments []string `json:"segments"`
	// Chunks are the committed chunks that are not sealed yet.
	Chunks []Chunk `json:"chunks"`
	// Consumed is, per incoming stream ID, the highest input chunk whose
	// output to this stream is fully committed (SPEC 7.4).
	Consumed map[string]int64 `json:"consumed"`
	EOS      bool             `json:"eos"`
	// Last is the number of the stream's last chunk, set with EOS. It is 0
	// for a stream that ended without chunks.
	Last *int64 `json:"last"`
	// Observed is the producer replica's committed counters and error
	// log totals.
	Observed *Observed `json:"observed,omitempty"`
}

// Chunk is one committed chunk: a file Alpheus moves but never reads.
type Chunk struct {
	N int64 `json:"n"`
	// Ext is the file extension the component gave at Create (GO-104).
	Ext    string `json:"ext"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
	// Counts are the component's counters committed with this chunk, per
	// unit (GO-115). Alpheus does not interpret the units.
	Counts map[string]int64 `json:"counts,omitempty"`
	// Src is the input chunk this chunk was made from; nil for sources.
	Src *Source `json:"src,omitempty"`
}

// Source names an input chunk.
type Source struct {
	Edge    string `json:"edge"`
	Replica int    `json:"replica"`
	N       int64  `json:"n"`
}

// Segment is a sealed, immutable part of a manifest.
type Segment struct {
	Version int     `json:"version"`
	Chunks  []Chunk `json:"chunks"`
}

// NewManifest starts an empty manifest for a stream.
func NewManifest(run, uid, edge string, replica int, epoch int64) *Manifest {
	return &Manifest{
		Version:  ManifestVersion,
		Run:      run,
		UID:      uid,
		Edge:     edge,
		Replica:  replica,
		Epoch:    epoch,
		Segments: []string{},
		Chunks:   []Chunk{},
		Consumed: map[string]int64{},
	}
}

// Count is how many chunks the stream has committed, sealed or not.
func (m *Manifest) Count() int64 {
	return int64(len(m.Segments))*SegmentSize + int64(len(m.Chunks))
}

// NextN is the number the next chunk must have. Chunks are numbered from 1
// without gaps.
func (m *Manifest) NextN() int64 { return m.Count() + 1 }

// Append adds a committed chunk. Its number must be NextN. When the
// unsealed chunks reach SegmentSize, Append seals them and returns the
// segment, which the caller must write under the stream's SegmentKey for
// len(m.Segments)-1 before it writes the manifest.
func (m *Manifest) Append(c Chunk) (*Segment, error) {
	if m.EOS {
		return nil, ErrEOS
	}
	if c.N != m.NextN() {
		return nil, fmt.Errorf("%w: chunk %d, want %d", ErrInvalid, c.N, m.NextN())
	}
	m.Chunks = append(m.Chunks, c)
	if len(m.Chunks) < SegmentSize {
		return nil, nil
	}
	seg := &Segment{Version: ManifestVersion, Chunks: m.Chunks}
	m.Segments = append(m.Segments, SegmentName(len(m.Segments)))
	m.Chunks = []Chunk{}
	return seg, nil
}

// SetConsumed records that the output of input chunk n of the incoming
// stream is fully committed to this stream. It never moves backwards.
func (m *Manifest) SetConsumed(stream string, n int64) {
	if n > m.Consumed[stream] {
		m.Consumed[stream] = n
	}
}

// IsConsumed reports whether input chunk n of the incoming stream already
// has its output committed here (GO-37, GO-70).
func (m *Manifest) IsConsumed(stream string, n int64) bool {
	return m.Consumed[stream] >= n
}

// End marks the stream as ended. A stream may end without chunks (GO-80).
func (m *Manifest) End() {
	if m.EOS {
		return
	}
	last := m.Count()
	m.EOS = true
	m.Last = &last
}

// CheckEpoch compares the manifest's epoch with the reader's own attempt
// number (GO-68).
func (m *Manifest) CheckEpoch(own int64) error {
	switch {
	case m.Epoch < own:
		return fmt.Errorf("%w: epoch %d, own %d", ErrStaleEpoch, m.Epoch, own)
	case m.Epoch > own:
		return fmt.Errorf("%w: epoch %d, own %d", ErrNewerEpoch, m.Epoch, own)
	}
	return nil
}

// Validate checks the rules a manifest must follow.
func (m *Manifest) Validate() error {
	if m.Version != ManifestVersion {
		return fmt.Errorf("%w: manifest version %d", ErrVersion, m.Version)
	}
	if m.Replica < 0 || m.Epoch < 1 {
		return fmt.Errorf("%w: replica %d, epoch %d", ErrInvalid, m.Replica, m.Epoch)
	}
	for i, name := range m.Segments {
		if name != SegmentName(i) {
			return fmt.Errorf("%w: segment %d is named %q", ErrInvalid, i, name)
		}
	}
	if len(m.Chunks) >= SegmentSize {
		return fmt.Errorf("%w: %d unsealed chunks", ErrInvalid, len(m.Chunks))
	}
	first := int64(len(m.Segments))*SegmentSize + 1
	for i, c := range m.Chunks {
		if c.N != first+int64(i) {
			return fmt.Errorf("%w: chunk %d at position %d", ErrInvalid, c.N, i)
		}
		if err := c.validate(); err != nil {
			return err
		}
	}
	for stream, n := range m.Consumed {
		if n < 0 {
			return fmt.Errorf("%w: consumed[%s] = %d", ErrInvalid, stream, n)
		}
	}
	if m.EOS != (m.Last != nil) {
		return fmt.Errorf("%w: eos and last disagree", ErrInvalid)
	}
	if m.Last != nil && *m.Last != m.Count() {
		return fmt.Errorf("%w: last %d, %d chunks", ErrInvalid, *m.Last, m.Count())
	}
	if m.Observed != nil {
		return m.Observed.Validate()
	}
	return nil
}

func (c Chunk) validate() error {
	if !ValidExt(c.Ext) || c.Bytes < 0 || len(c.SHA256) != 64 {
		return fmt.Errorf("%w: chunk %d", ErrInvalid, c.N)
	}
	if len(c.Counts) > MaxUnits {
		return fmt.Errorf("%w: chunk %d has %d counter units", ErrInvalid, c.N, len(c.Counts))
	}
	for unit, n := range c.Counts {
		if !ValidUnit(unit) || n < 0 {
			return fmt.Errorf("%w: chunk %d: counter %q = %d", ErrInvalid, c.N, unit, n)
		}
	}
	if c.Src != nil && c.Src.N < 1 {
		return fmt.Errorf("%w: chunk %d has source chunk %d", ErrInvalid, c.N, c.Src.N)
	}
	return nil
}

// ParseManifest decodes and validates a manifest.
func ParseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := decodeStrict(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if m.Segments == nil {
		m.Segments = []string{}
	}
	if m.Chunks == nil {
		m.Chunks = []Chunk{}
	}
	if m.Consumed == nil {
		m.Consumed = map[string]int64{}
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return &m, nil
}

// Marshal encodes the manifest after checking it.
func (m *Manifest) Marshal() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}
	return json.Marshal(m)
}

// ParseSegment decodes and validates sealed segment i.
func ParseSegment(data []byte, i int) (*Segment, error) {
	var s Segment
	if err := decodeStrict(data, &s); err != nil {
		return nil, fmt.Errorf("parse segment %d: %w", i, err)
	}
	if s.Version != ManifestVersion {
		return nil, fmt.Errorf("parse segment %d: %w: version %d", i, ErrVersion, s.Version)
	}
	if len(s.Chunks) != SegmentSize {
		return nil, fmt.Errorf("parse segment %d: %w: %d chunks", i, ErrInvalid, len(s.Chunks))
	}
	first := int64(i)*SegmentSize + 1
	for j, c := range s.Chunks {
		if c.N != first+int64(j) {
			return nil, fmt.Errorf("parse segment %d: %w: chunk %d at position %d", i, ErrInvalid, c.N, j)
		}
		if err := c.validate(); err != nil {
			return nil, fmt.Errorf("parse segment %d: %w", i, err)
		}
	}
	return &s, nil
}

// Marshal encodes the segment.
func (s *Segment) Marshal() ([]byte, error) { return json.Marshal(s) }

// decodeStrict decodes one JSON value and rejects unknown fields and
// trailing data, so a file from a newer format is not half understood.
func decodeStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: trailing data", ErrInvalid)
	}
	return nil
}
