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

"""A small Python implementation of the Alpheus component contract.

A component reads input chunks as files and writes output chunks as files.
This module talks to the agent over its Unix socket with the gRPC contract
in component.proto, as the Go SDK does. It is a reference for components in
other languages than Go, not a maintained SDK.

    import alpheus

    def handle(inp, out):
        for chunk in inp.chunks():
            with out.create("default", chunk.ext) as dst, open(chunk.path, "rb") as src:
                dst.write(src.read())
            alpheus.count("chunks", 1)

    alpheus.run(handle)

With ALPHEUS_MODE=local it runs without Kubernetes: input chunks are the
files in ALPHEUS_LOCAL_INPUT, in name order, except manifest.json, so
another step's output folder can be the input; outputs go to
ALPHEUS_LOCAL_OUTPUT/<output>/part-000001.<ext>, with _summary.json and
_errors.jsonl next to them.
"""

import json
import logging
import os
import queue
import re
import signal
import sys
import threading
import time
from dataclasses import dataclass

DEFAULT_OUTPUT = "default"
EXIT_OK, EXIT_FAILED, EXIT_USAGE, EXIT_STOPPED = 0, 1, 2, 143


class UsageError(Exception):
    """The component was started or used wrongly; run exits with 2."""


class Stopped(Exception):
    """The pod is being stopped (SIGTERM); run exits with 143."""


@dataclass
class Chunk:
    """An input chunk: a file the agent deletes when the chunk is done."""
    port: str
    step: str
    replica: int
    n: int
    path: str
    ext: str
    bytes: int


_current = None  # the running component, for count, error, warn, progress


def count(unit, n):
    """Adds n to the counter unit; committed with the current input chunk.

    A source has no input chunk: its counts go with the next chunk it
    closes, so it counts a chunk before closing it.
    """
    _current.observe(count=(unit, n))


def error(code, message, **fields):
    """Logs an error in the step's error log; the run goes on."""
    _current.observe(log=("error", code, message, fields))


def warn(code, message, **fields):
    _current.observe(log=("warn", code, message, fields))


def progress(text):
    """A short status text for alpheus watch; not committed."""
    _current.observe(progress=text)


class File:
    """An output chunk. Write to it, or let a tool write to .path, and close it."""

    def __init__(self, owner, id, path):
        self._owner, self.id, self.path = owner, id, path
        self._f = open(path, "wb")
        self.closed = False

    def write(self, data):
        return self._f.write(data)

    def close(self):
        if not self.closed:
            self.closed = True
            self._f.close()
            self._owner._open.remove(self)
            self._owner._close(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class _Component:
    """What agent and local mode share: the chunk loop, settings and EOS."""

    def __init__(self, settings, replica, replicas):
        self._settings, self._replica, self._replicas = settings, replica, replicas
        self._eos, self._open, self._broken = [], [], False
        # Set when the agent says an earlier attempt already ended all
        # output, so the on_eos functions must not run on an empty state.
        self._ended = False

    def settings(self):
        """The step's settings from the StreamRun, as a dict."""
        try:
            return json.loads(self._settings) if self._settings else {}
        except ValueError as e:
            raise UsageError(f"settings: {e}")

    def replica(self):
        return self._replica

    def replicas(self):
        return self._replicas

    def on_eos(self, fn):
        """fn(out) runs once all input is read: where stateful steps write."""
        self._eos.append(fn)

    def chunks(self, port=""):
        """Yields input chunks. A chunk is done when the next one is asked for:
        its output, counts and log entries are then committed together."""
        while True:
            got = self._next(port)
            if got is None:
                return
            chunk, id = got
            try:
                yield chunk
            except GeneratorExit:
                self._broken = True
                raise
            for f in list(self._open):
                f.close()
            self._done(id)

    def finish(self):
        if self._broken:
            raise RuntimeError("component returned in the middle of an input chunk")
        for fn in self._eos:
            if self._ended:
                break
            fn(self)
        for f in list(self._open):
            f.close()


class _Agent(_Component):
    """Agent mode: the gRPC contract over the agent's Unix socket."""

    def __init__(self, socket, settings, replica, replicas):
        super().__init__(settings, replica, replicas)
        import grpc
        from . import component_pb2 as pb, component_pb2_grpc as pb_grpc
        self._grpc, self._pb = grpc, pb
        self._stub = pb_grpc.AgentStub(grpc.insecure_channel("unix://" + socket))
        self._lock, self._seq, self._input = threading.Lock(), 0, 0
        self._queue = queue.Queue()
        # Reports go on one stream and never block the component; Done,
        # Close and Finish name the last one, and the agent waits for it.
        self._stream = self._stub.Observe.future(iter(self._queue.get, None), wait_for_ready=True)

    def _call(self, name, req):
        try:
            return getattr(self._stub, name)(req, wait_for_ready=True)
        except self._grpc.RpcError as e:
            if e.code() in (self._grpc.StatusCode.INVALID_ARGUMENT, self._grpc.StatusCode.FAILED_PRECONDITION):
                raise UsageError(f"{name}: {e.details()}") from None
            raise RuntimeError(f"{name}: {e.details()}") from None

    def observe(self, count=None, log=None, progress=None):
        pb = self._pb
        if count:
            report = {"count": pb.Count(unit=count[0], n=count[1])}
        elif log:
            level, code, message, fields = log
            report = {"log": pb.LogEntry(level=level, code=code, message=message, time=time.time_ns(),
                                         fields=json.dumps(fields).encode() if fields else b"")}
        else:
            report = {"progress": progress}
        with self._lock:
            self._seq += 1
            self._queue.put(pb.ObserveRequest(seq=self._seq, input=self._input, **report))

    def _next(self, port):
        resp = self._call("Next", self._pb.NextRequest(port=port, stateful=bool(self._eos)))
        if resp.eos:
            self._ended = self._ended or resp.ended
            return None
        c = resp.chunk
        self._input = c.id
        return Chunk(c.port, c.step, c.replica, c.n, c.path, c.ext, c.bytes), c.id

    def _done(self, id):
        self._input = 0
        self._call("Done", self._pb.DoneRequest(id=id, observe_seq=self._seq))

    def create(self, output, ext):
        """Starts an output chunk; may block while consumers catch up."""
        resp = self._call("Create", self._pb.CreateRequest(output=output or DEFAULT_OUTPUT, ext=ext, input=self._input))
        f = File(self, resp.id, resp.path)
        self._open.append(f)
        return f

    def _close(self, f):
        self._call("Close", self._pb.CloseRequest(id=f.id, observe_seq=self._seq))

    def finish(self):
        super().finish()
        self._call("Finish", self._pb.FinishRequest(observe_seq=self._seq))

    def stop(self):
        self._queue.put(None)
        self._stream.result(timeout=10)


class _Local(_Component):
    """Local mode: input chunks from a folder, outputs to another."""

    def __init__(self, input_dir, output_dir, settings, replica, replicas):
        super().__init__(settings, replica, replicas)
        if not output_dir:
            raise UsageError("local mode needs ALPHEUS_LOCAL_OUTPUT")
        # Every file is a chunk, except a manifest local mode in Go writes,
        # so another step's output folder can be the input.
        names = sorted(n for n in os.listdir(input_dir) if not re.fullmatch(r"manifest(-\d+)?\.json", n)
                       and os.path.isfile(os.path.join(input_dir, n))) if input_dir else []
        # Chunk n goes to replica n mod replicas, as in a run.
        self._inputs = [(i + 1, os.path.join(input_dir, name)) for i, name in enumerate(names)
                        if (i + 1) % replicas == replica]
        self._out, self._numbers = output_dir, {}
        self._summary = {"counts": {}, "errors": 0, "warnings": 0, "errorsDropped": 0}
        self._log, self._chunk = [], ""
        os.makedirs(output_dir, exist_ok=True)

    def observe(self, count=None, log=None, progress=None):
        if count:
            unit, n = count
            self._summary["counts"][unit] = self._summary["counts"].get(unit, 0) + n
        elif log:
            level, code, message, fields = log
            self._summary["errors" if level == "error" else "warnings"] += 1
            self._log.append({"time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "level": level,
                              "code": code, "message": message, "fields": fields or None,
                              "step": "local", "replica": self._replica, "chunk": self._chunk})
        else:
            print(json.dumps({"level": "INFO", "msg": "progress", "text": progress}), flush=True)

    def _next(self, port):
        if not self._inputs:
            return None
        n, path = self._inputs.pop(0)
        self._chunk = os.path.basename(path)
        ext = os.path.splitext(path)[1].lstrip(".")
        return Chunk("default", "local", 0, n, path, ext, os.path.getsize(path)), n

    def _done(self, id):
        self._chunk = ""

    def create(self, output, ext):
        output = output or DEFAULT_OUTPUT
        n = self._numbers[output] = self._numbers.get(output, 0) + 1
        os.makedirs(os.path.join(self._out, output), exist_ok=True)
        f = File(self, n, os.path.join(self._out, output, f"part-{n:06d}.{ext}"))
        self._open.append(f)
        return f

    def _close(self, f):
        pass

    def stop(self):
        with open(os.path.join(self._out, "_summary.json"), "w") as f:
            json.dump(self._summary, f)
        with open(os.path.join(self._out, "_errors.jsonl"), "w") as f:
            f.writelines(json.dumps(e) + "\n" for e in self._log)


def _termination_message(msg):
    """Leaves why the component failed where Kubernetes keeps it after the
    pod is gone, so the run's status can say it (GO-139). The file exists
    only in a container."""
    path = "/dev/termination-log"
    if not os.path.exists(path):
        return
    try:
        with open(path, "w") as f:
            f.write(msg[:2048])
    except OSError:
        pass


def _stop(signum, frame):
    raise Stopped()


def run(handle):
    """Runs handle(inp, out) and exits with its code: 0 when it returned,
    2 for a UsageError, 143 when stopped, and 1 for any other error."""
    global _current
    signal.signal(signal.SIGTERM, _stop)
    # ALPHEUS_LOG_LEVEL sets the level of Python's logging; alpheus replay
    # sets debug (GO-141).
    level = os.environ.get("ALPHEUS_LOG_LEVEL", "")
    if level:
        levels = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}
        logging.basicConfig(level=levels.get(level.lower(), logging.INFO), stream=sys.stdout,
                            format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    code = EXIT_OK
    try:
        settings = os.environ.get("ALPHEUS_SETTINGS", "")
        replica = int(os.environ.get("ALPHEUS_REPLICA", "0"))
        replicas = int(os.environ.get("ALPHEUS_REPLICAS", "1"))
        mode = os.environ.get("ALPHEUS_MODE", "")
        if mode == "local":
            _current = _Local(os.environ.get("ALPHEUS_LOCAL_INPUT", ""), os.environ.get("ALPHEUS_LOCAL_OUTPUT", ""),
                              settings, replica, replicas)
        elif mode == "" and os.environ.get("ALPHEUS_SOCKET"):
            _current = _Agent(os.environ["ALPHEUS_SOCKET"], settings, replica, replicas)
        else:
            raise UsageError("no agent (ALPHEUS_SOCKET is not set); for local mode set "
                             "ALPHEUS_MODE=local, ALPHEUS_LOCAL_INPUT and ALPHEUS_LOCAL_OUTPUT")
        handle(_current, _current)
        _current.finish()
        _current.stop()
    except BaseException as e:
        code = {UsageError: EXIT_USAGE, Stopped: EXIT_STOPPED, KeyboardInterrupt: EXIT_STOPPED}.get(type(e), EXIT_FAILED)
        print(json.dumps({"time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "level": "ERROR",
                          "msg": "component failed", "error": str(e) or type(e).__name__, "exit_code": code}), flush=True)
        _termination_message(f"{type(e).__name__}: {e}" if str(e) else type(e).__name__)
    sys.exit(code)
