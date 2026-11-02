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

package agent

import (
	"errors"
	"fmt"
	"testing"

	"github.com/minio/minio-go/v7"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/store"
)

// The agent fails with a reason code for each kind of error (GO-138).
func TestReason(t *testing.T) {
	for _, c := range []struct {
		err  error
		want v1alpha1.Reason
	}{
		{fmt.Errorf("input x: chunk of 4 bytes %w of 1 bytes", ErrScratchFull), v1alpha1.ReasonScratchFull},
		{fmt.Errorf("put x: %w for 10m0s: refused", store.ErrUnreachable), v1alpha1.ReasonStorageUnreachable},
		{fmt.Errorf("put x: %w", minio.ErrorResponse{StatusCode: 403, Code: "AccessDenied", Message: "Bucket size quota is reached"}), v1alpha1.ReasonStorageFull},
		{fmt.Errorf("get x: %w", minio.ErrorResponse{StatusCode: 403, Code: "SignatureDoesNotMatch"}), v1alpha1.ReasonStorageRefused},
		{fmt.Errorf("get x: %w", minio.ErrorResponse{StatusCode: 500, Code: "InternalError"}), v1alpha1.ReasonStorageError},
		{errors.New("the component did not connect"), v1alpha1.ReasonAgentFailed},
	} {
		if got := Reason(c.err); got != c.want {
			t.Errorf("%v: %s, want %s", c.err, got, c.want)
		}
	}
}
