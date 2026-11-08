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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// A run's status is written at most once a second; other runs are not
// held back by it (GO-22).
func TestStatusLimit(t *testing.T) {
	var l statusLimit
	a, b := types.NamespacedName{Namespace: "n", Name: "a"}, types.NamespacedName{Namespace: "n", Name: "b"}
	t0 := time.Unix(1000, 0)
	if w := l.wait(a, t0, time.Second); w != 0 {
		t.Errorf("first write waits %v", w)
	}
	l.wrote(a, t0)
	if w := l.wait(a, t0.Add(300*time.Millisecond), time.Second); w != 700*time.Millisecond {
		t.Errorf("a write 300ms later waits %v, want 700ms", w)
	}
	if w := l.wait(b, t0.Add(300*time.Millisecond), time.Second); w != 0 {
		t.Errorf("another run waits %v", w)
	}
	if w := l.wait(a, t0.Add(time.Second), time.Second); w != 0 {
		t.Errorf("a write a second later waits %v", w)
	}
	l.forget(a)
	if w := l.wait(a, t0.Add(300*time.Millisecond), time.Second); w != 0 {
		t.Errorf("a forgotten run waits %v", w)
	}
}
