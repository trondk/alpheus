# Copyright The Alpheus Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""generate is the source of the log-report example: it writes chunks of
log lines, every seventh line an ERROR.

Settings: {"chunks": 20, "lines": 5000}
"""

import alpheus


def handle(inp, out):
    s = inp.settings()
    for n in range(s["chunks"]):
        with out.create("default", "log") as f:
            for i in range(s["lines"]):
                level = "ERROR" if (n * s["lines"] + i) % 7 == 0 else "INFO"
                f.write(f"{level} line {i + 1} of chunk {n + 1}\n".encode())
            # Counted before the file closes, so the count is committed
            # with this chunk.
            alpheus.count("lines", s["lines"])


if __name__ == "__main__":
    alpheus.run(handle)
