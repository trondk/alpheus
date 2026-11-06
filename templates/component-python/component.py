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

"""grep is the template for an Alpheus component in Python.

It keeps the lines of each input chunk that contain a text, and writes them
as one output chunk per input chunk, with the input's file extension. A
chunk without matching lines gives no output. Lines are copied byte for
byte, so with an empty match the output is the input.

Settings: {"match": "ERROR", "invert": false}

It counts the units lines_in and lines_out, and warns about an empty input
chunk with the code empty_chunk. At debug level it logs its settings and
each chunk: set logLevel: debug on the step, or run
alpheus run --log-level <step>=debug; alpheus replay always does.

Copy this folder to start a component: replace handle with your own, keep
alpheus.run at the end.
"""

import logging

import alpheus

# alpheus.run sets Python's logging to ALPHEUS_LOG_LEVEL, so debug lines
# cost nothing unless the step's logLevel is debug. Log what helps find a
# bug, not the data itself, which may be personal.
log = logging.getLogger("grep")


def handle(inp, out):
    s = inp.settings()
    unknown = set(s) - {"match", "invert"}
    if unknown:
        # A misspelt setting is an error, not silently ignored.
        raise alpheus.UsageError(f"unknown settings: {', '.join(sorted(unknown))}")
    match = s.get("match", "").encode()
    invert = bool(s.get("invert", False))
    log.debug("settings: match=%r invert=%s replica=%d of %d", match, invert, inp.replica(), inp.replicas())

    # chunks() yields the input chunks of this replica, one file at a time.
    # A chunk is done when the loop asks for the next one: what was written
    # and counted for it is then committed together.
    for chunk in inp.chunks():
        if chunk.bytes == 0:
            alpheus.warn("empty_chunk", "the input chunk is empty", n=chunk.n)
            continue
        dst = None  # created at the first matching line
        lines_in = lines_out = 0
        with open(chunk.path, "rb") as src:
            for line in src:
                lines_in += 1
                if (match in line) != invert:
                    if dst is None:
                        dst = out.create("default", chunk.ext)
                    dst.write(line)
                    lines_out += 1
        if dst is not None:
            # close hands the file to the agent, which uploads it when the
            # input chunk is done.
            dst.close()
        alpheus.count("lines_in", lines_in)
        alpheus.count("lines_out", lines_out)
        log.debug("chunk done: from=%s replica=%d n=%d bytes=%d lines_in=%d lines_out=%d",
                  chunk.step, chunk.replica, chunk.n, chunk.bytes, lines_in, lines_out)


if __name__ == "__main__":
    alpheus.run(handle)
