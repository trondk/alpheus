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

package main

import (
	"slices"
	"testing"
)

func TestNetworkOptions(t *testing.T) {
	o, err := networkOptions(true, ":7070", "3900, 9000", `[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"storage"}}}]`)
	if err != nil {
		t.Fatal(err)
	}
	if o.NoNetworkPolicy || o.HubPort != 7070 || !slices.Equal(o.StoragePorts, []int32{3900, 9000}) ||
		len(o.StorageEgressTo) != 1 || o.StorageEgressTo[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "storage" {
		t.Errorf("%+v", o)
	}
	if o, err := networkOptions(false, "", "", ""); err != nil || !o.NoNetworkPolicy || o.HubPort != 0 || o.StoragePorts != nil {
		t.Errorf("%+v %v", o, err)
	}
	if o, err := networkOptions(true, "[::]:7071", "", ""); err != nil || o.HubPort != 7071 {
		t.Errorf("%+v %v", o, err)
	}
	for _, bad := range [][3]string{{":x", "", ""}, {":7070", "0", ""}, {":7070", "70000", ""}, {":7070", "", "{"}} {
		if _, err := networkOptions(true, bad[0], bad[1], bad[2]); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}
