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

// LocalIndex is a file an input folder in local mode can have, next to the
// chunks: a JSON object from a chunk's file name to its LocalChunk. A
// replay writes it, so the component sees each chunk's producer and
// number as in the run. Without it, or for a file it does not name, the
// chunk is named after its port and numbered by its place in the folder.
const LocalIndex = "_chunks.json"

// LocalChunk is a chunk's producer and its number in the producer's
// stream.
type LocalChunk struct {
	Step    string `json:"step"`
	Replica int    `json:"replica"`
	N       int64  `json:"n"`
}
