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
	"errors"
	"io"
	"strings"
	"testing"
)

// TestChecksumRoundTrip checks that a chunk written with its size and
// SHA-256 reads back unchanged through the verifying reader.
func TestChecksumRoundTrip(t *testing.T) {
	content := strings.Repeat("order_id,country,amount\n", 1000)
	var buf bytes.Buffer
	hw := NewHashingWriter(&buf)
	if _, err := io.Copy(hw, strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	c := Chunk{N: 1, Bytes: hw.Size(), SHA256: hw.Sum()}
	if c.Bytes != int64(len(content)) {
		t.Fatalf("size %d, want %d", c.Bytes, len(content))
	}

	got, err := io.ReadAll(NewVerifyingReader(&buf, c))
	if err != nil {
		t.Fatalf("verifying the same content: %v", err)
	}
	if string(got) != content {
		t.Error("content changed")
	}
}

// TestChecksumDetectsCorruption checks that a chunk with a changed byte, a
// missing byte or an extra byte fails with ErrChecksum.
func TestChecksumDetectsCorruption(t *testing.T) {
	content := []byte("some chunk content")
	var buf bytes.Buffer
	hw := NewHashingWriter(&buf)
	hw.Write(content)
	c := Chunk{N: 7, Bytes: hw.Size(), SHA256: hw.Sum()}

	flipped := bytes.Clone(content)
	flipped[3] ^= 1
	for name, data := range map[string][]byte{
		"changed byte": flipped,
		"truncated":    content[:len(content)-1],
		"longer":       append(bytes.Clone(content), 'x'),
	} {
		_, err := io.ReadAll(NewVerifyingReader(bytes.NewReader(data), c))
		if !errors.Is(err, ErrChecksum) {
			t.Errorf("%s: %v, want ErrChecksum", name, err)
		}
	}
}
