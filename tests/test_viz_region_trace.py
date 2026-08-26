"""Region failover, recorded as a replayable trace for `dev/scripts/viz`.

Two stories, both walked against the real binary over real HTTP with a real
MinIO behind a bucket-aware fault proxy — the same fixtures and the same
assertions `tests/test_read_affinity.py` already proves:

1. **The region a node reads from goes dark.** Reads fall back to the write
   region, the write pin never moves, a real `uv pip install` keeps working, a
   publish during the outage leaves a queued repair note owing the dark region,
   the region returns, the note drains, and only then do reads return.
2. **The preferred region goes dark** — the first bucket in config order, the
   fleet-wide write home — while this node reads from a different region. New
   writes move to the next healthy region; the node's read pin never budges.

Every recorded frame is a measurement, never a narration: bucket pins, health
and selection generation are sampled from `GET /metrics` on a fixed 300 ms tick,
`/ready` is polled on the same tick, the node's own JSON log and the fault
proxy's request log are tailed and folded in, bucket contents are read straight
from MinIO (length + sha256 prefix per object), and every fault injection and
recovery instant is stamped as a phase boundary.

Recording is opt-in: with `PYPIRON_VIZ_OUT` unset the whole module skips, so the
ordinary suite pays nothing for it. `PYPIRON_VIZ_OUT` names the directory the
traces are written into (`<scenario-id>.jsonl`); a path ending in `.jsonl` is
accepted too and its parent directory is used. The file format is the frozen
JSONL contract in the visualizer spec: line 0 is `meta` (carrying the caveat
band verbatim), the last line is `summary`, and `i` is gapless.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import threading
import time
from pathlib import Path

import pytest

from .conftest import (
    READ_AFFINITY_NODE_REGION,
    _start_read_affinity_server,
    minio_delete_key_in,
    minio_get_key_bytes_in,
    minio_key_exists_in,
    minio_list_keys_in,
    minio_put_key_in,
    s3_repl_tag,
)
from .helpers import http_get, make_wheel, wait_for_file_in_index
from .test_read_affinity import (
    _artifact_gets,
    _bucket_health,
    _eventually,
    _install,
    _metrics,
    _read_bucket,
    _three_region_buckets_uri,
    _upload,
    _write_bucket,
)

#: Set to the output directory to record; unset means "do not record", and the
#: module skips. The behaviour under test is asserted unconditionally by
#: tests/test_read_affinity.py — this module exists to capture it as a trace.
_VIZ_OUT = os.environ.get("PYPIRON_VIZ_OUT")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.s3,
    pytest.mark.skipif(not _VIZ_OUT, reason="recording is opt-in: set PYPIRON_VIZ_OUT=<dir>"),
]

#: Sampling cadence for /metrics and /ready, in seconds.
_TICK = 0.3

_READ_SEL_RE = re.compile(r'^pypiron_bucket_read_selected\{bucket="[^"]+",index="(\d+)"\} ([01])$')
_WRITE_SEL_RE = re.compile(r'^pypiron_bucket_selected\{bucket="[^"]+",index="(\d+)"\} ([01])$')
_HEALTH_IDX_RE = re.compile(
    r'^pypiron_bucket_health_state\{bucket="[^"]+",index="(\d+)"\} (-?1|0)$'
)
_GENERATION_RE = re.compile(r"^pypiron_bucket_selection_generation (\d+)$")

#: One line of the fault proxy's request log (tests/conftest.py `_S3FaultProxyHandler._log`).
_PROXY_RE = re.compile(
    r"^\S+ (?P<method>\S+) bucket=(?P<bucket>\S+) path=(?P<path>\S+) "
    r"clen=\S+ forwarded=\d+B upstream=(?P<upstream>\S+)$"
)

#: Node log messages worth a frame, and the name they are recorded under. Each
#: is a real signal the product emits: the four startup read-affinity verdicts
#: (src/app.rs), the two selection-change lines and the deferred-return warning
#: (src/worker.rs). Matched as a prefix of the log message.
_LOG_SIGNALS = (
    ("node region detected", "node_region_detected"),
    ("read affinity: serving reads from region bucket", "startup_reads_pinned_to_the_region"),
    ("read affinity: region bucket still converging", "startup_region_still_converging"),
    ("read affinity: region bucket unreachable at startup", "startup_region_unreachable"),
    ("read affinity: no configured bucket is labeled", "startup_no_region_bucket_configured"),
    ("preferred bucket unavailable at startup", "startup_preferred_bucket_unavailable"),
    ("read bucket changed", "read_pin_moved"),
    ("selected bucket changed", "write_selection_moved"),
    ("could not confirm region bucket caught up", "read_return_deferred_pending_proof"),
    ("runtime bucket topology mismatch", "writes_fenced_on_topology_mismatch"),
)

#: Caveats every region recording carries. These are the difference between an
#: honest asset and a false one; the player renders them in an always-visible
#: band, so anything the run does not measure has to say so here.
_COMMON_CAVEATS = (
    "Both 'regions' are @region labels on one MinIO endpoint. No real geography, "
    "no cross-region latency.",
    "Timings are test-compressed: this run uses leave-after 1 failure and a 2-second "
    "return window; the shipped defaults are 3 failures and 300 seconds.",
    "The recorder is not a passive observer: it polls /metrics and /ready every 300 ms, "
    "and every /ready is a real HEAD against the read bucket, so the node always sees "
    "recent traffic and never decays to its idle probe cadence.",
    "One node, one process. Nothing here measures a fleet, a load balancer, DNS, or "
    "client-side retry.",
    "The node's region is declared with PYPIRON_NODE_REGION. Real cloud-metadata region "
    "detection (IMDS / GCP / Azure) is not exercised.",
    "Bucket contents are snapshotted at phase boundaries only, read straight from MinIO "
    "so the snapshot bypasses the fault proxy. Nothing between two snapshots is recorded, "
    "the regions are not snapshotted at the same instant, and the node keeps writing "
    "while a snapshot is taken — so leader-lease and index churn shows up in the deltas.",
    "summary.trace_hash, state_hash, trace_events, audit_view_repairs, repairs_by_class "
    "and ack_totality are deterministic-simulator fields with no meaning for this "
    "producer; they are recorded as null.",
)


def _trace_path(scenario: str) -> Path:
    """Where `scenario`'s trace is written. `PYPIRON_VIZ_OUT` is the output
    directory; a `.jsonl` path is accepted and its parent is used, so a caller
    that names one scenario's file still gets its siblings beside it."""
    out = Path(_VIZ_OUT or "")
    directory = out.parent if out.suffix == ".jsonl" else out
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{scenario}.jsonl"


def _commit() -> str:
    """The tree's short commit, or `"unknown"` — a pinned observation is only
    meaningful as (commit, config, flags)."""
    try:
        done = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return done.stdout.strip() if done.returncode == 0 else "unknown"


def _pin_sample(server) -> dict:
    """One measurement of the node's bucket state from `GET /metrics`: the
    one-hot read and write selections as bucket indices, the shared selection
    generation, and per-index health (1 healthy / 0 unknown / -1 unhealthy)."""
    read_index = write_index = None
    generation = None
    health: dict[int, int] = {}
    for line in _metrics(server):
        if match := _READ_SEL_RE.match(line):
            if match.group(2) == "1":
                read_index = int(match.group(1))
        elif match := _WRITE_SEL_RE.match(line):
            if match.group(2) == "1":
                write_index = int(match.group(1))
        elif match := _HEALTH_IDX_RE.match(line):
            health[int(match.group(1))] = int(match.group(2))
        elif match := _GENERATION_RE.match(line):
            generation = int(match.group(1))
    assert read_index is not None and write_index is not None, "no one-hot bucket selection"
    return {"read": read_index, "write": write_index, "gen": generation, "health": health}


class _Tail:
    """Incremental reader over an append-only log: returns whole lines written
    since the last call, holding back a partial trailing line."""

    def __init__(self, path) -> None:
        self._path = Path(path) if path else None
        self._offset = 0

    def lines(self) -> list[str]:
        if self._path is None:
            return []
        try:
            with self._path.open("rb") as handle:
                handle.seek(self._offset)
                chunk = handle.read()
        except FileNotFoundError:
            return []
        cut = chunk.rfind(b"\n") + 1
        self._offset += cut
        return chunk[:cut].decode("utf-8", "replace").splitlines()


class _Recorder:
    """Writes the JSONL trace the player replays.

    Envelope per the frozen contract: every line carries `t`, a gapless `i`,
    `sim` (always null — this producer has no simulated clock) and `ts`,
    milliseconds since the recording started. Line 0 is `meta`, the last line is
    `summary`. Both the test thread and the sampler thread emit, so `i` is
    handed out under a lock.
    """

    def __init__(self, *, scenario: str, title: str, narration: str, cmd: str, caveats) -> None:
        self._lock = threading.Lock()
        self._lines: list[str] = []
        self._next = 0
        self._t0 = time.monotonic()
        self._violations = 0
        self._acked = 0
        self._executed: dict[str, int] = {}
        self._world: dict[int, dict[str, str]] = {}
        self.scenario = scenario
        self._meta = {
            "kind": "trace",
            "producer": "region",
            "scenario": scenario,
            "title": title,
            "narration": narration,
            "cmd": cmd,
            "commit": _commit(),
            "seed": None,
            "packages": None,
            "files": None,
            "ops": None,
            "fail_percent": None,
            "partition_percent": None,
            "brk": "none",
            "homes": [None],
            "caveats": [*_COMMON_CAVEATS, *caveats],
        }

    # --- the envelope ---------------------------------------------------

    def emit(self, event_type: str, body: dict) -> None:
        with self._lock:
            line = {
                "t": event_type,
                "i": self._next,
                "sim": None,
                "ts": round((time.monotonic() - self._t0) * 1000, 1),
                **body,
            }
            self._next += 1
            self._lines.append(json.dumps(line, separators=(",", ":")))

    def start(self, *, bucket_names: list[str], node_regions: list[str | None]) -> None:
        """Emit `meta` — the header band, the topology, and the caveats."""
        assert self._next == 0, "meta must be line 0"
        self.emit(
            "meta",
            {
                **self._meta,
                "nodes": len(node_regions),
                "buckets": len(bucket_names),
                "bucket_names": bucket_names,
                "node_regions": node_regions,
            },
        )

    def finish(self) -> Path:
        """Emit `summary` and write the file."""
        self.emit(
            "summary",
            {
                "violations": self._violations,
                "acked": self._acked,
                "trace_hash": None,
                "trace_events": None,
                "state_hash": None,
                "audit_view_repairs": None,
                "repairs_by_class": None,
                "ack_totality": None,
                "events_emitted": self._next + 1,
                "truncated": False,
                "elapsed_ms": round((time.monotonic() - self._t0) * 1000, 1),
                "exit_code": 1 if self._violations else 0,
            },
        )
        path = _trace_path(self.scenario)
        path.write_text("".join(f"{line}\n" for line in self._lines))
        return path

    # --- frames the test thread stamps ----------------------------------

    def phase(self, name: str, at: str) -> None:
        self.emit("phase", {"name": name, "at": at})

    def held(self, name: str, detail: str) -> None:
        """One assertion that just passed, at the instant it passed."""
        count = self._executed.get(name, 0) + 1
        self._executed[name] = count
        self.emit("oracle", {"name": name, "executed": count, "verdict": "held", "detail": detail})

    def violated(self, name: str, detail: str) -> None:
        self._violations += 1
        self.emit("oracle", {"name": name, "executed": 1, "verdict": "violated", "detail": detail})

    def ack(self, *, pkg: str, file: str, bucket: int, body: bytes) -> None:
        self._acked += 1
        self.emit(
            "ack",
            {
                "kind": "publish",
                "pkg": pkg,
                "file": file,
                "bucket": bucket,
                "sha8": hashlib.sha256(body).hexdigest()[:8],
                "variant": None,
                "mirror": False,
                "ok": True,
            },
        )

    def note(self, *, bucket: int, key: str, present: bool) -> None:
        self.emit("note", {"bucket": bucket, "key": key, "present": present})

    def world(self, minio, buckets: list[str], *, full: bool = False) -> None:
        """Fold every bucket's real contents in as a `world` delta. The value is
        `"<len>:<sha8>"` per object, read direct from MinIO so the snapshot is
        ground truth rather than something the node reported. The sha is the
        whole point: byte-identity across regions is the convergence claim, and
        length alone cannot tell that story."""
        for index, bucket in enumerate(buckets):
            current = {}
            for key in minio_list_keys_in(minio, bucket):
                body = minio_get_key_bytes_in(minio, bucket, key)
                current[key] = f"{len(body)}:{hashlib.sha256(body).hexdigest()[:8]}"
            previous = self._world.get(index, {})
            self._world[index] = current
            if full:
                put, removed = current, []
            else:
                put = {k: v for k, v in current.items() if previous.get(k) != v}
                removed = sorted(set(previous) - set(current))
                if not put and not removed:
                    continue
            self.emit("world", {"b": index, "full": full, "put": put, "del": removed})


class _Sampler:
    """Background measurement loop: `/metrics` and `/ready` on a fixed tick, plus
    the node's JSON log and the fault proxy's request log folded in as they are
    written. Nothing here interprets — it records what it read."""

    def __init__(self, recorder: _Recorder, server, buckets: list[str]) -> None:
        self._rec = recorder
        self._server = server
        self._index_of = {name: index for index, name in enumerate(buckets)}
        self._node_log = _Tail(server["log_path"])
        faults = server.get("faults")
        self._proxy_log = _Tail(getattr(faults, "log_path", None) if faults else None)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="viz-region-sampler", daemon=True)

    def __enter__(self) -> _Sampler:
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self._stop.set()
        self._thread.join(timeout=10)
        # One last pass so the tail of both logs makes it into the trace.
        self._fold_logs()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except (AssertionError, OSError, ValueError, json.JSONDecodeError):
                # A sample that could not be taken is not a frame. The story's
                # own assertions run on the test thread and are what fail.
                pass
            self._stop.wait(_TICK)

    def _tick(self) -> None:
        sample = _pin_sample(self._server)
        self._rec.emit(
            "pin",
            {
                "node": 0,
                "read": sample["read"],
                "write": sample["write"],
                "gen": sample["gen"],
                "health": [sample["health"].get(i, 0) for i in range(len(self._index_of))],
            },
        )
        code, body, _ = http_get(f"{self._server['base_url']}/ready", timeout=3)
        self._rec.emit(
            "ready", {"node": 0, "status": code, "body": body.decode("utf-8", "replace")}
        )
        self._fold_logs()

    def _fold_logs(self) -> None:
        for line in self._node_log.lines():
            self._fold_node_line(line)
        for line in self._proxy_log.lines():
            self._fold_proxy_line(line)

    def _fold_node_line(self, line: str) -> None:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            return
        fields = record.get("fields") or {}
        message = fields.get("message") or record.get("message") or ""
        for prefix, name in _LOG_SIGNALS:
            if message.startswith(prefix):
                extra = " ".join(f"{k}={v}" for k, v in fields.items() if k != "message")
                self._rec.held(name, f"{message} {extra}".strip())
                return

    def _fold_proxy_line(self, line: str) -> None:
        match = _PROXY_RE.match(line)
        if not match:
            return
        index = self._index_of.get(match.group("bucket"))
        if index is None:
            return
        upstream = match.group("upstream")
        status = int(digits.group(0)) if (digits := re.match(r"\d+", upstream)) else 0
        self._rec.emit(
            "probe",
            {
                "bucket": index,
                "path": f"{match.group('method')} {match.group('path')}",
                "status": status,
                "injected": "injected" in upstream,
            },
        )


def _hold_still(recorder, server, *, seconds: float, read: str, write: str, name: str, detail: str):
    """Stay in the current phase for a while, asserting both pins the whole
    time. Two jobs at once: it turns an instant into a band the player can
    animate, and it upgrades "the pin moved" into "the pin moved and stayed" —
    the claim a reader actually cares about during an outage. Not load-sensitive:
    nothing may move a pin while the underlying condition is unchanged."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        assert _read_bucket(server) == read, f"the read pin left {read}"
        assert _write_bucket(server) == write, f"the write pin left {write}"
        time.sleep(0.2)
    recorder.held(name, detail)


def _record(recorder: _Recorder, server, buckets: list[str], story, *args) -> Path:
    """Sample throughout one story, record a violated oracle if an assertion
    fails, and write the trace either way — a recording that stopped early is
    still an honest record of where it stopped. The sampler is shut down before
    `summary` is emitted so the last frames it read land in the file."""
    try:
        with _Sampler(recorder, server, buckets):
            story(recorder, server, *args)
    except AssertionError as exc:
        recorder.violated(f"{recorder.scenario}:recording", str(exc))
        raise
    finally:
        recorder.finish()
    return _trace_path(recorder.scenario)


# ---------------------------------------------------------------------------
# Story 1 — the region a node reads from goes dark
# ---------------------------------------------------------------------------

_READ_SIDE_CAVEATS = (
    "The drain gate is held open by a repair note this recording plants and that can "
    "never be satisfied. The real note the outage publish leaves drains in well under a "
    "second — far too fast to watch the gate hold reads off the region.",
    "This is the node's own read region going dark. The preferred (write) region going "
    "dark is a separate story, recorded as region-write-home-failover.jsonl.",
    "No client request is in flight across the instant a pin moves, so nothing here "
    "measures what a client would have seen at that exact moment.",
)

_READ_SIDE_CMD = (
    "PYPIRON_VIZ_OUT=.local/viz uv run -- pytest tests/test_viz_region_trace.py "
    "-x -k region_read_failover"
)


def test_record_region_read_failover(
    tmp_path_factory, pypiron_bin, minio_two_proxy, tmp_path, uv_path, uv_venv
):
    """Record the measured read-side arc: steady-state locality, a sustained
    region outage, failover to the write region with the write pin untouched, a
    publish that owes the dark region a repair note, the region's return, the
    drain gate holding reads off until nothing is owed, and the return home."""
    minio = minio_two_proxy
    buckets = list(minio["buckets"][:2])
    server_gen = _start_read_affinity_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        node_region=READ_AFFINITY_NODE_REGION,
        leave_failures=1,
        return_healthy_secs=2,
        extra_env={"PYPIRON_LOG_FORMAT": "json"},
    )
    recorder = _Recorder(
        scenario="region-read-failover",
        title="The region a node reads from goes dark",
        narration=(
            "The region this node reads from goes dark: reads fall back to the write region "
            "in seconds, the write pin never moves, a real uv pip install keeps working, and "
            "reads return only once that region is healthy and owes nothing."
        ),
        cmd=_READ_SIDE_CMD,
        caveats=_READ_SIDE_CAVEATS,
    )
    try:
        server = next(server_gen)
        recorder.start(bucket_names=buckets, node_regions=[READ_AFFINITY_NODE_REGION])
        path = _record(
            recorder, server, buckets, _read_side_story, minio, buckets, tmp_path, uv_path, uv_venv
        )
    finally:
        server_gen.close()
    _assert_trace_is_well_formed(path, "region-read-failover")


def _read_side_story(recorder, server, minio, buckets, tmp_path, uv_path, uv_venv) -> None:
    a, b = buckets
    faults = server["faults"]

    recorder.phase("setup", "node up; region-labeled buckets, reads pinned in-region")
    _eventually(lambda: _read_bucket(server) == b, what="reads pin to the region bucket B")
    recorder.held("reads_are_served_from_the_nodes_own_region", f"read pin = {b}")
    _eventually(lambda: _write_bucket(server) == a, what="writes home to the preferred bucket A")
    recorder.held("writes_home_to_the_preferred_region", f"write pin = {a}")
    _eventually(lambda: _bucket_health(server, b) == 1, what="region bucket known healthy")
    recorder.held("the_region_is_known_healthy", f"health({b}) = 1")

    base_pkg = "failbaseline"
    base_wheel = make_wheel(base_pkg, "1.0", tmp_path)
    _upload(server, base_wheel)
    wait_for_file_in_index(server["simple"], base_pkg, base_wheel.name)
    recorder.ack(pkg=base_pkg, file=base_wheel.name, bucket=0, body=base_wheel.read_bytes())
    _eventually(
        lambda: minio_key_exists_in(minio, b, f"packages/{base_pkg}/{base_wheel.name}"),
        what="baseline fanned out to B",
    )
    recorder.held(
        "a_publish_reaches_every_region_before_the_client_hears_200",
        f"{base_wheel.name} present in both {a} and {b}",
    )
    recorder.world(minio, buckets, full=True)

    # The outage. Reads abandon the region for the write home; the write pin does
    # not move, because a read preference never perturbs the write selection.
    recorder.phase("outage", f"the region bucket {b} stops answering")
    faults.fail(b)
    _eventually(
        lambda: _read_bucket(server) == a,
        what="reads fail over to the write bucket when the region goes dark",
    )
    recorder.held("reads_fail_over_to_the_write_region", f"read pin moved to {a}")
    assert _write_bucket(server) == a, "the write pin never moved"
    recorder.held("the_write_pin_never_moves", f"write pin still {a}")
    _hold_still(
        recorder,
        server,
        seconds=3.0,
        read=a,
        write=a,
        name="the_failed_over_pins_stay_put_while_the_region_stays_dark",
        detail=f"read and write both held on {a} for 3s of continuous outage",
    )
    base_key = f"packages/{base_pkg}/{base_wheel.name}"
    before_a = _artifact_gets(faults, a, base_key)
    code, body, _ = http_get(f"{server['base_url']}/files/{base_pkg}/{base_wheel.name}", timeout=30)
    assert code == 200
    assert body == base_wheel.read_bytes()
    _eventually(
        lambda: _artifact_gets(faults, a, base_key) > before_a,
        what="the download is served from the write region while the node's region is dark",
    )
    recorder.held(
        "downloads_are_served_from_the_write_region_during_the_outage",
        f"{a} served {base_key} itself while {b} was dark",
    )
    _install(server, uv_path, uv_venv, base_pkg)
    recorder.held(
        "a_real_uv_pip_install_keeps_working_during_the_outage",
        f"uv pip install {base_pkg}==1.0 resolved and imported off {a}",
    )

    # A publish during the outage owes the dark region a repair note.
    out_pkg = "duringoutage"
    out_wheel = make_wheel(out_pkg, "1.0", tmp_path)
    out_key = f"packages/{out_pkg}/{out_wheel.name}"
    _upload(server, out_wheel)
    recorder.ack(pkg=out_pkg, file=out_wheel.name, bucket=0, body=out_wheel.read_bytes())
    owed = _eventually(
        lambda: [
            k
            for k in minio_list_keys_in(minio, a)
            if k.startswith(f"_repl/{s3_repl_tag(b)}/{out_pkg}/")
        ],
        what="outage publish owes B a note",
    )
    for key in owed:
        recorder.note(bucket=0, key=key, present=True)
    recorder.held(
        "a_publish_during_the_outage_leaves_a_queued_repair_owing_the_dark_region",
        f"{a} holds {owed[0]}",
    )
    assert not minio_key_exists_in(minio, b, out_key)
    recorder.held("the_dark_region_does_not_hold_the_outage_publish", f"{b} lacks {out_key}")
    recorder.world(minio, buckets)

    # A second note the region can never satisfy on its own, so the drain gate is
    # provably what holds reads off it, isolated from the healthy-return window.
    # (Its source record's sidecar is unreadable — an operator problem nothing may
    # fabricate over — so the copy fails and the sweep retains the note.)
    hold_pkg = "holdopen"
    hold_file = "holdopen-1.0-py3-none-any.whl"
    hold_akey = f"packages/{hold_pkg}/{hold_file}"
    hold_note = f"_repl/{s3_repl_tag(b)}/{hold_pkg}/{hold_file}!hold"
    minio_put_key_in(
        minio,
        a,
        f"packages/{hold_pkg}/.origin",
        json.dumps({"origin": "private", "nonce": "b" * 32}),
    )
    minio_put_key_in(minio, a, f"{hold_akey}.meta.json", "not json")
    minio_put_key_in(minio, a, hold_akey, "bytes")
    minio_put_key_in(minio, a, hold_note, "")
    recorder.note(bucket=0, key=hold_note, present=True)

    recorder.phase("recovery", f"the region bucket {b} answers again")
    faults.recover(b)
    _eventually(lambda: _bucket_health(server, b) == 1, timeout=15, what="B observed healthy again")
    recorder.held("the_region_answers_again", f"health({b}) = 1")
    _eventually(
        lambda: minio_key_exists_in(minio, b, out_key), timeout=30, what="real note drains to B"
    )
    for key in owed:
        recorder.note(bucket=0, key=key, present=False)
    recorder.held("the_queued_repair_drains_into_the_returning_region", f"{b} now holds {out_key}")
    recorder.world(minio, buckets)

    # Healthy, past the return window, and holding the outage record — the only
    # thing left is the note it still owes. Reads must stay away.
    settle = time.monotonic() + 4.0
    while time.monotonic() < settle:
        assert _read_bucket(server) == a, "reads returned to B while a repair note was still owed"
        time.sleep(0.2)
    recorder.held(
        "reads_stay_away_while_the_region_is_still_owed_a_repair",
        f"read pin held on {a} for 4s while {hold_note} was outstanding",
    )

    recorder.phase("recovery", "the last owed repair note is removed; the gate opens")
    for key in (hold_note, hold_akey, f"{hold_akey}.meta.json", f"packages/{hold_pkg}/.origin"):
        minio_delete_key_in(minio, a, key)
    recorder.note(bucket=0, key=hold_note, present=False)
    _eventually(
        lambda: _read_bucket(server) == b,
        timeout=15,
        what="reads return to B once it is caught up",
    )
    recorder.held("reads_return_only_once_nothing_is_owed", f"read pin returned to {b}")

    assert minio_key_exists_in(minio, b, out_key)
    before = _artifact_gets(server["faults"], b, out_key)
    code, body, _ = http_get(f"{server['base_url']}/files/{out_pkg}/{out_wheel.name}", timeout=30)
    assert code == 200
    assert body == out_wheel.read_bytes()
    _eventually(
        lambda: _artifact_gets(server["faults"], b, out_key) > before,
        what="post-return reads hit B",
    )
    recorder.held(
        "reads_provably_come_from_the_region_again",
        f"{b} served {out_key} itself after the return",
    )
    recorder.world(minio, buckets)
    assert _write_bucket(server) == a, "the write pin never moved across the whole arc"
    recorder.held("the_write_pin_never_moved_across_the_whole_arc", f"write pin = {a}")


# ---------------------------------------------------------------------------
# Story 2 — the preferred region goes dark; reads stay in their region
# ---------------------------------------------------------------------------

_WRITE_SIDE_CAVEATS = (
    "Three regions, one node, and the node's own region is LAST in preference order. "
    "That ordering is the point: when the preferred region dies the write selection "
    "takes an unrelated region, so 'reads stayed home' and 'reads followed the write "
    "pin' are different observations. A two-region topology cannot tell them apart.",
    "The recording stops while the preferred region is still dark. Whether writes "
    "return to it once it recovers is not measured here.",
)

_WRITE_SIDE_CMD = (
    "PYPIRON_VIZ_OUT=.local/viz uv run -- pytest tests/test_viz_region_trace.py "
    "-x -k region_write_home_failover"
)


def test_record_region_write_home_failover(
    tmp_path_factory, pypiron_bin, minio_three_proxy, tmp_path, uv_path, uv_venv
):
    """Record the other direction: the preferred region — first in config order,
    the fleet-wide write home — goes dark while this node reads from a different
    region. New writes move to the next healthy region in preference order and
    the node's read pin never budges."""
    minio = minio_three_proxy
    a, c, b = minio["buckets"][:3]
    buckets = [a, c, b]
    server_gen = _start_read_affinity_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        node_region=READ_AFFINITY_NODE_REGION,
        leave_failures=1,
        return_healthy_secs=2,
        extra_env={
            "PYPIRON_BUCKETS": _three_region_buckets_uri(minio),
            "PYPIRON_LOG_FORMAT": "json",
        },
    )
    recorder = _Recorder(
        scenario="region-write-home-failover",
        title="The preferred region goes dark; reads stay in their region",
        narration=(
            "The preferred region — the fleet-wide write home — goes dark while this node "
            "reads from a different region: new writes move to the next healthy region in "
            "preference order and the node's reads never leave home."
        ),
        cmd=_WRITE_SIDE_CMD,
        caveats=_WRITE_SIDE_CAVEATS,
    )
    try:
        server = next(server_gen)
        recorder.start(bucket_names=buckets, node_regions=[READ_AFFINITY_NODE_REGION])
        path = _record(
            recorder, server, buckets, _write_side_story, minio, buckets, tmp_path, uv_path, uv_venv
        )
    finally:
        server_gen.close()
    _assert_trace_is_well_formed(path, "region-write-home-failover")


def _write_side_story(recorder, server, minio, buckets, tmp_path, uv_path, uv_venv) -> None:
    a, c, b = buckets
    faults = server["faults"]

    recorder.phase("setup", "three regions; this node reads from the last one in preference order")
    _eventually(lambda: _read_bucket(server) == b, what="reads pin to the region bucket B")
    recorder.held("reads_are_served_from_the_nodes_own_region", f"read pin = {b}")
    _eventually(lambda: _write_bucket(server) == a, what="writes home to the preferred bucket A")
    recorder.held("writes_home_to_the_preferred_region", f"write pin = {a}")

    base_pkg = "writehomebase"
    base_wheel = make_wheel(base_pkg, "1.0", tmp_path)
    base_key = f"packages/{base_pkg}/{base_wheel.name}"
    _upload(server, base_wheel)
    wait_for_file_in_index(server["simple"], base_pkg, base_wheel.name)
    recorder.ack(pkg=base_pkg, file=base_wheel.name, bucket=0, body=base_wheel.read_bytes())
    _eventually(lambda: minio_key_exists_in(minio, b, base_key), what="baseline fanned out to B")
    recorder.held(
        "a_publish_reaches_every_region_before_the_client_hears_200",
        f"{base_wheel.name} present in {a}, {c} and {b}",
    )
    recorder.world(minio, buckets, full=True)

    recorder.phase("outage", f"the preferred region {a} — the write home — stops answering")
    faults.fail(a)
    _eventually(
        lambda: _write_bucket(server) == c,
        what="writes move to the next healthy bucket in preference order",
    )
    recorder.held("writes_move_to_the_next_healthy_region", f"write pin moved to {c}")
    assert _bucket_health(server, a) == -1, "the preferred bucket is known unhealthy"
    recorder.held("the_dark_preferred_region_is_known_unhealthy", f"health({a}) = -1")
    assert _read_bucket(server) == b, "the read pin left its own healthy region bucket"
    recorder.held("the_read_pin_stays_in_its_own_region", f"read pin still {b}, not {c}")
    assert "read bucket changed" not in server["log_path"].read_text(), (
        "a write-side failover perturbed the read pin"
    )
    recorder.held(
        "no_read_pin_move_is_logged_at_all",
        "the node never logged 'read bucket changed' across the write-side failover",
    )
    _hold_still(
        recorder,
        server,
        seconds=3.0,
        read=b,
        write=c,
        name="the_split_pins_stay_split_while_the_preferred_region_stays_dark",
        detail=f"3s of continuous outage with writes on {c} and reads still on {b}",
    )

    out_pkg = "writehomeoutage"
    out_wheel = make_wheel(out_pkg, "1.0", tmp_path)
    out_key = f"packages/{out_pkg}/{out_wheel.name}"
    _upload(server, out_wheel)
    wait_for_file_in_index(server["simple"], out_pkg, out_wheel.name)
    recorder.ack(pkg=out_pkg, file=out_wheel.name, bucket=1, body=out_wheel.read_bytes())
    assert minio_key_exists_in(minio, c, out_key), "the new write home holds the record"
    recorder.held("the_new_write_region_holds_the_outage_publish", f"{c} holds {out_key}")
    _eventually(
        lambda: minio_key_exists_in(minio, b, out_key),
        what="the outage publish still fanned out to the region bucket B",
    )
    recorder.held("the_outage_publish_still_reaches_this_nodes_region", f"{b} holds {out_key}")
    owed = _eventually(
        lambda: [
            k
            for k in minio_list_keys_in(minio, c)
            if k.startswith(f"_repl/{s3_repl_tag(a)}/{out_pkg}/")
        ],
        what="the dead preferred bucket is owed a repair note",
    )
    for key in owed:
        recorder.note(bucket=0, key=key, present=True)
    recorder.held(
        "the_dark_preferred_region_is_owed_a_queued_repair",
        f"{c} holds {owed[0]}",
    )
    recorder.world(minio, buckets)

    assert _write_bucket(server) == c
    assert _read_bucket(server) == b
    before = _artifact_gets(faults, b, out_key)
    code, body, _ = http_get(f"{server['base_url']}/files/{out_pkg}/{out_wheel.name}", timeout=30)
    assert code == 200
    assert body == out_wheel.read_bytes()
    _eventually(
        lambda: _artifact_gets(faults, b, out_key) > before,
        what="reads are still served from the node's own region bucket",
    )
    recorder.held(
        "a_brand_new_file_is_still_read_in_region",
        f"{b} served {out_key} itself while the preferred region was dark",
    )
    _install(server, uv_path, uv_venv, out_pkg)
    recorder.held(
        "a_real_uv_pip_install_keeps_working_during_the_outage",
        f"uv pip install {out_pkg}==1.0 resolved and imported with {a} dark",
    )


# ---------------------------------------------------------------------------
# The trace is a contract, so check it like one
# ---------------------------------------------------------------------------


def _assert_trace_is_well_formed(path: Path, scenario: str) -> None:
    """The player is a separate program reading a frozen schema, so prove the
    envelope here rather than discovering it downstream: every line is JSON, `i`
    is gapless from 0, line 0 is `meta` with the mandatory caveats, and the last
    line is `summary`."""
    lines = path.read_text().splitlines()
    assert len(lines) > 2, f"{path} recorded nothing"
    events = [json.loads(line) for line in lines]
    for index, event in enumerate(events):
        assert event["i"] == index, f"trace index gap at line {index}: {event['i']}"
        assert event["sim"] is None, "this producer has no simulated clock"
        assert isinstance(event["ts"], (int, float)), "every frame carries a real elapsed time"
    meta, summary = events[0], events[-1]
    assert meta["t"] == "meta" and meta["kind"] == "trace" and meta["producer"] == "region"
    assert meta["scenario"] == scenario
    assert len(meta["bucket_names"]) == meta["buckets"]
    assert len(meta["node_regions"]) == meta["nodes"]
    caveats = meta["caveats"]
    assert any("one MinIO endpoint" in c for c in caveats), (
        "the no-real-geography caveat is missing"
    )
    assert any("test-compressed" in c for c in caveats), "the compressed-timings caveat is missing"
    assert summary["t"] == "summary"
    assert summary["events_emitted"] == len(events)
    assert summary["violations"] == 0, f"the recording itself failed: {summary}"
    # A recording with no measurements is a narration; these are the frames the
    # player animates, and each one is a sample or a passed assertion.
    kinds = {event["t"] for event in events}
    assert {"phase", "pin", "ready", "oracle", "world", "note", "ack", "probe"} <= kinds, (
        f"the recording is missing measured frames: {kinds}"
    )
