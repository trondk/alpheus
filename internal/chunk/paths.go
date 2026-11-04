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

// Package chunk holds the chunk protocol's names and files: where streams,
// chunks, manifests and ack files live in object storage, the manifest and
// ack file formats, which consumer replica gets which chunk, and SHA-256
// checksums of chunk objects. It does no I/O against the store itself, and
// it never reads a chunk's content: a chunk is a file in a format only the
// components know (GO-101).
package chunk

import (
	"fmt"
	"path"
	"regexp"
	"strconv"
)

// DefaultOutput is the output a component emits to when it names none.
const DefaultOutput = "default"

// EdgeName is the name of an edge in storage and status: <from>-<to>, or
// <from>.<output>-<to> for an output other than default (SPEC 7.3).
func EdgeName(from, output, to string) string {
	if output == "" || output == DefaultOutput {
		return from + "-" + to
	}
	return from + "." + output + "-" + to
}

// Stream identifies the chunks one producer replica writes: on an edge of a
// run, or as one of a step's results.
type Stream struct {
	prefix string
	// ID is how the stream is named in other manifests' consumed maps and
	// in chunk sources: <edge>/<replica> or <step>.<output>/<replica>.
	ID string
}

// EdgeStream is the stream producer replica writes on an edge:
// runs/<uid>/<edge>/<replica>/ (SPEC 7.3, GO-83).
func EdgeStream(uid, edge string, replica int) Stream {
	r := strconv.Itoa(replica)
	return Stream{
		prefix: path.Join("runs", uid, edge, r) + "/",
		ID:     edge + "/" + r,
	}
}

// ResultStream is the stream a replica writes for a result:
// results/<run>/<uid>/<step>/<output>/<replica>/ (SPEC 5.2, GO-83).
func ResultStream(run, uid, step, output string, replica int) Stream {
	r := strconv.Itoa(replica)
	return Stream{
		prefix: path.Join("results", run, uid, step, output, r) + "/",
		ID:     step + "." + output + "/" + r,
	}
}

// Prefix is the object key prefix of everything in the stream, ending in /.
func (s Stream) Prefix() string { return s.prefix }

// ChunkKey is the object key of chunk n with file extension ext:
// <prefix>part-000017.<ext> (GO-104).
func (s Stream) ChunkKey(n int64, ext string) string {
	return fmt.Sprintf("%spart-%06d.%s", s.prefix, n, ext)
}

// ManifestKey is the object key of the stream's manifest.
func (s Stream) ManifestKey() string { return s.prefix + "manifest.json" }

// SegmentKey is the object key of sealed manifest segment i (GO-32).
func (s Stream) SegmentKey(i int) string {
	return s.prefix + SegmentName(i)
}

// SegmentName is the file name of sealed manifest segment i, as listed in
// the manifest's segments.
func SegmentName(i int) string { return fmt.Sprintf("manifest-%04d.json", i) }

// AckKey is the object key of the ack file a consumer replica writes for
// this stream: <prefix>acks-<step>-<replica>.json (GO-36).
func (s Stream) AckKey(consumerStep string, consumerReplica int) string {
	return fmt.Sprintf("%sacks-%s-%d.json", s.prefix, consumerStep, consumerReplica)
}

var extPattern = regexp.MustCompile(`^[a-z0-9]{1,16}$`)

// ValidExt reports whether ext can be a chunk's file extension: lower-case
// letters and digits, at most 16 (GO-104).
func ValidExt(ext string) bool { return extPattern.MatchString(ext) }

// RunPrefix is everything a run keeps until it is cleaned up. Results are
// not under it (GO-78).
func RunPrefix(uid string) string { return path.Join("runs", uid) + "/" }

// RunInfoKey is the object that describes a run's results, so they can be
// found and cleaned up after the StreamRun is gone:
// results/<run>/<uid>/run.json (GO-98).
func RunInfoKey(run, uid string) string { return path.Join("results", run, uid, "run.json") }

// ResultsPrefix is where a step's results are: results/<run>/<uid>/<step>/.
func ResultsPrefix(run, uid, step string) string {
	return path.Join("results", run, uid, step) + "/"
}

// ErrorLogPrefix is where a replica's error log and committed counters
// are: results/<run>/<uid>/_errors/<step>/<replica>/ (GO-118). Step names
// cannot start with _, so no step's results collide with it.
func ErrorLogPrefix(run, uid, step string, replica int) string {
	return path.Join("results", run, uid, "_errors", step, strconv.Itoa(replica)) + "/"
}

// LogsPrefix is where the logs of a failed attempt are kept:
// results/<run>/<uid>/_logs/<step>/<replica>/attempt-<n>/ (GO-140). With a
// replica below 0 it is the step's prefix, results/<run>/<uid>/_logs/<step>/.
func LogsPrefix(run, uid, step string, replica, attempt int) string {
	if replica < 0 {
		return path.Join("results", run, uid, "_logs", step) + "/"
	}
	return path.Join("results", run, uid, "_logs", step, strconv.Itoa(replica), "attempt-"+strconv.Itoa(attempt)) + "/"
}

// FailurePrefix is where a failed attempt is kept for a replay:
// results/<run>/<uid>/_failures/<step>/<replica>/attempt-<n>/ (GO-141).
// With a replica below 0 it is the step's prefix.
func FailurePrefix(run, uid, step string, replica, attempt int) string {
	if replica < 0 {
		return path.Join("results", run, uid, "_failures", step) + "/"
	}
	return path.Join("results", run, uid, "_failures", step, strconv.Itoa(replica), "attempt-"+strconv.Itoa(attempt)) + "/"
}

// SkippedPrefix is where the input chunks a replica never processed are
// kept, after it gave up with failurePolicy continue (GO-97):
// results/<run>/<uid>/_failures/<step>/<replica>/skipped/.
func SkippedPrefix(run, uid, step string, replica int) string {
	return path.Join("results", run, uid, "_failures", step, strconv.Itoa(replica), "skipped") + "/"
}

// Assign returns the consumer replica that gets chunk n of the stream from
// producer replica p, when the consuming step has the given number of
// replicas: (n + p) mod replicas (GO-76).
func Assign(n int64, producerReplica, replicas int) int {
	if replicas <= 1 {
		return 0
	}
	return int((n + int64(producerReplica)) % int64(replicas))
}
