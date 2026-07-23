#!/usr/bin/env python3
"""Turn the VOPR soak's journal stream into deduped findings + live status in S3.

The soak (`examples/vopr.rs --forever --rotate`) never exits on a finding: it
prints a `FAILED` block — the violations plus the exact deterministic repro
command — and explores the next seed. A single real bug fails a large fraction
of seeds, so the raw stream is thousands of near-identical blocks. This reader
collapses them to one object per *distinct* failure, keyed by a seed-agnostic
signature hash, and drops the exact repro in each.

It reads the merged soak journal on stdin as `journalctl -o json` lines (see
pypiron-soak-reporter.service), so it runs once per box and can attribute every
line to its soak unit — the FAILED-block parser is per-unit, so two cores
failing at once can't interleave into one garbled block. Design rules:

  - Dedup by S3 key. The finding key is the signature hash, so N distinct bugs =
    N objects no matter how many seeds or boxes hit them; listing the prefix is
    the deduped finding set. No GitHub, no PAT — the instance's IAM role writes
    to two S3 prefixes and nothing else.
  - Event-driven: a new distinct failure writes its object (and, if configured,
    an SNS notification) the moment it streams in.
  - Fail-open on surfacing: an S3/network hiccup never kills the finder. A
    finding we could not write is appended to a local fallback file for retry;
    the soak keeps running regardless.
  - Rate-capped: a bad binary can turn every seed red with many distinct-looking
    signatures. Past a per-hour cap the reader stops writing and shouts.

Status: each reporter process is one *segment* with a fresh uuid. It follows
the soak's once-a-minute progress lines, accumulates per-unit seed counts
(reset-safe: the vopr counter restarts at zero when a soak process restarts),
and every STATUS_INTERVAL puts one JSON object to
`<status prefix><commit>-<uuid>.json`. One writer per key + S3's atomic
whole-object PUT = safe concurrent writes with zero coordination; the client
lists the prefix and aggregates. On SIGTERM/SIGINT (systemd stop, bundle
refresh, spot reclaim's shutdown) a final snapshot is flushed with
`final: true`, so a segment's last write is its total. Segment totals never
overlap — a new segment baselines on the current journal gauges and counts
only deltas it observes — so summing every object undercounts slightly
(restart gaps), never overcounts.

stdlib only; shells out to the `aws` CLI already on the box.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid as uuidlib
from dataclasses import dataclass, field
from datetime import datetime, timezone

BUCKET = os.environ.get("PYPIRON_SOAK_S3_BUCKET", "")
PREFIX = os.environ.get("PYPIRON_SOAK_S3_PREFIX", "soak/findings/")
STATUS_PREFIX = os.environ.get("PYPIRON_SOAK_STATUS_PREFIX", "soak/status/")
STATUS_INTERVAL = int(os.environ.get("PYPIRON_SOAK_STATUS_INTERVAL", "60"))
SNS_TOPIC = os.environ.get("PYPIRON_SOAK_SNS_TOPIC", "")  # optional ARN
AWS = os.environ.get("PYPIRON_SOAK_AWS", "aws")
REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", ""))
DRY_RUN = bool(os.environ.get("PYPIRON_SOAK_DRY_RUN"))
MAX_PER_HOUR = int(os.environ.get("PYPIRON_SOAK_MAX_ISSUES_PER_HOUR", "50"))
# Runtime file, outside any checkout, surviving restarts for retry.
FALLBACK = os.environ.get("PYPIRON_SOAK_FALLBACK", "/var/tmp/pypiron-soak-findings.fallback.jsonl")
# The bundle ships a `commit` file next to the binary (written by fleet.sh
# push-bundle and by CI); env override for local testing.
COMMIT_FILE = os.environ.get("PYPIRON_SOAK_COMMIT_FILE", "commit")
TITLE_MAX = 120

# A FAILED block:
#   vopr: seed 1784515453 FAILED (1 violations):
#     AUDIT_ORDERING: bucket0 vopr-beta — truth mutation ...whl.tombstone@191 ...
#     ...possibly multi-line `changed: [ ... ]` dump...
#   reproduce: cargo run --release --example vopr -- --seed 1784515453 --nodes 3 ...
FAILED_RE = re.compile(r"^vopr: seed (\d+) FAILED \((\d+) violation")
REPRODUCE_RE = re.compile(r"^reproduce: (.+)$")
DETERMINISM_RE = re.compile(r"^vopr: DETERMINISM VIOLATION seed=(\d+): (.+)$")
# The soak's once-a-minute heartbeat; counters are cumulative per process.
PROGRESS_RE = re.compile(
    r"^vopr: progress — (\d+) seeds, (\d+) storage-op interleavings, (\d+) acked uploads"
)
# Volatile bits to erase so the same bug yields one stable signature across
# seeds: per-op sequence tags (@191), per-seed nonces (!991), heal-round
# indices, the per-seed leftover lists, concrete bucket indices, and the
# `| changed:` dump tail.
SEQ_TAG_RE = re.compile(r"@\d+")
NONCE_RE = re.compile(r"!\d+")
LIST_RE = re.compile(r"\[[^\]]*\]")
ROUND_RE = re.compile(r"\bround \d+\b")
BUCKET_RE = re.compile(r"\bbucket\d+\b")
TAIL_RE = re.compile(r"\s*(\|\s*changed:|changed:).*$", re.DOTALL)
WS_RE = re.compile(r"\s+")


def signature(violation: str) -> str:
    """Collapse one violation line to a seed-agnostic signature."""
    s = TAIL_RE.sub("", violation)  # drop the multi-line per-seed diff dump
    s = SEQ_TAG_RE.sub("", s)
    s = NONCE_RE.sub("", s)
    s = ROUND_RE.sub("round N", s)
    s = LIST_RE.sub("[…]", s)
    s = BUCKET_RE.sub("bucketN", s)
    s = WS_RE.sub(" ", s).strip().strip("-").strip()
    return s


def sighash(sig: str) -> str:
    return hashlib.sha1(sig.encode("utf-8")).hexdigest()


def title_for(sig: str) -> str:
    """Human-readable one-liner for the finding payload."""
    t = f"[vopr] {sig}"
    return t if len(t) <= TITLE_MAX else t[: TITLE_MAX - 1] + "…"


def log(msg: str) -> None:
    print(f"report.py: {msg}", file=sys.stderr, flush=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def read_commit() -> str:
    try:
        with open(COMMIT_FILE, encoding="utf-8") as f:
            return f.read().strip() or "unknown"
    except OSError:
        return "unknown"


def imds(path: str) -> str | None:
    """One IMDSv2 lookup; fail-open (None off-EC2 or on any hiccup)."""
    base = "http://169.254.169.254/latest"
    try:
        req = urllib.request.Request(
            f"{base}/api/token", method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "300"},
        )
        with urllib.request.urlopen(req, timeout=2) as r:
            token = r.read().decode("utf-8")
        req = urllib.request.Request(
            f"{base}/meta-data/{path}", headers={"X-aws-ec2-metadata-token": token}
        )
        with urllib.request.urlopen(req, timeout=2) as r:
            return r.read().decode("utf-8")
    except (urllib.error.URLError, OSError, TimeoutError):
        return None


class Shutdown(Exception):
    """Raised out of the stdin loop by the SIGTERM/SIGINT handler."""


@dataclass
class StatusTracker:
    """Reset-safe accumulation of the soak's per-unit progress gauges.

    vopr's counters are cumulative per process and restart at zero with it, so
    each new gauge value contributes its delta — or, after a reset (value went
    down), its full value — to this segment's totals.
    """

    commit: str
    uuid: str = field(default_factory=lambda: uuidlib.uuid4().hex)
    started_at: str = field(default_factory=now_iso)
    lock: threading.Lock = field(default_factory=threading.Lock)
    units: dict[str, dict] = field(default_factory=dict)
    totals: dict[str, int] = field(
        default_factory=lambda: {"seeds": 0, "interleavings": 0, "acked_uploads": 0}
    )
    instance_id: str | None = None
    instance_type: str | None = None

    def baseline(self, unit: str, gauges: tuple[int, int, int]) -> None:
        """Set a unit's starting gauges without counting them (startup pre-pass)."""
        with self.lock:
            self.units[unit] = {"gauges": gauges, "seeds": 0, "last_seen": None}

    def observe(self, unit: str, gauges: tuple[int, int, int]) -> None:
        with self.lock:
            u = self.units.setdefault(unit, {"gauges": (0, 0, 0), "seeds": 0, "last_seen": None})
            for key, prev, cur in zip(("seeds", "interleavings", "acked_uploads"), u["gauges"], gauges):
                delta = cur - prev if cur >= prev else cur  # counter reset => process restarted
                self.totals[key] += delta
                if key == "seeds":
                    u["seeds"] += delta
            u["gauges"] = gauges
            u["last_seen"] = now_iso()

    def snapshot(self, findings: int, final: bool, exit_reason: str | None) -> dict:
        with self.lock:
            return {
                "uuid": self.uuid,
                "commit": self.commit,
                "instance_id": self.instance_id,
                "instance_type": self.instance_type,
                "cores": os.cpu_count() or 1,
                "started_at": self.started_at,
                "updated_at": now_iso(),
                **self.totals,
                "findings": findings,
                "units": {
                    unit: {"seeds": u["seeds"], "gauge": u["gauges"][0], "last_seen": u["last_seen"]}
                    for unit, u in sorted(self.units.items())
                },
                "final": final,
                "exit_reason": exit_reason,
            }


@dataclass
class Reporter:
    seen: set[str] = field(default_factory=set)  # signature hashes handled this run
    filed_times: list[float] = field(default_factory=list)

    def _aws(self, args: list[str]) -> bool:
        cmd = [AWS, *(["--region", REGION] if REGION else []), *args]
        try:
            subprocess.run(cmd, capture_output=True, text=True, timeout=60, check=True)
            return True
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as e:
            log(f"aws {args[0]} {args[1] if len(args) > 1 else ''} failed: "
                f"{getattr(e, 'stderr', '') or e}")
            return False

    def _rate_ok(self) -> bool:
        cutoff = time.monotonic() - 3600
        self.filed_times = [t for t in self.filed_times if t > cutoff]
        return len(self.filed_times) < MAX_PER_HOUR

    def finding(self, sig: str, repro: str, seed: str, raw: str, kind: str) -> None:
        h = sighash(sig)
        if h in self.seen:
            return  # already handled this distinct bug in this process
        self.seen.add(h)
        payload = {
            "signature": sig,
            "title": title_for(sig),
            "repro": repro,
            "first_seed": seed,
            "raw": raw.strip(),
            "kind": kind,
            "ts": time.time(),
        }
        if DRY_RUN:
            print(f"[dry-run] WOULD WRITE {PREFIX}{h}.json\n{json.dumps(payload, indent=2)}\n{'-' * 60}")
            return
        if not self._rate_ok():
            log(f"RATE CAP hit ({MAX_PER_HOUR}/h) — not writing {h}: {payload['title']}")
            self._fallback(payload)
            return
        self.filed_times.append(time.monotonic())
        if not BUCKET:
            self._fallback(payload)  # no bucket configured: keep findings locally
            return
        if self.put_json(f"{PREFIX}{h}.json", payload):
            log(f"wrote finding {PREFIX}{h}.json — {payload['title']}")
            self._notify(payload)
        else:
            self._fallback(payload)

    def put_json(self, key: str, payload: dict) -> bool:
        # Finding keys are signature hashes: idempotent, so concurrent boxes
        # writing the same new bug just overwrite identical content. Status keys
        # are per-segment uuids: single writer. Either way S3's atomic PUT means
        # no coordination is needed.
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(payload, f)
            tmp = f.name
        try:
            return self._aws([
                "s3api", "put-object", "--bucket", BUCKET, "--key", key,
                "--body", tmp, "--content-type", "application/json",
            ])
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def _notify(self, payload: dict) -> None:
        if not SNS_TOPIC:
            return
        self._aws([
            "sns", "publish", "--topic-arn", SNS_TOPIC,
            "--subject", payload["title"][:100],
            "--message", f"{payload['title']}\n\nRepro:\n{payload['repro']}\n\nSignature: {payload['signature']}",
        ])

    def _fallback(self, payload: dict) -> None:
        try:
            with open(FALLBACK, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload) + "\n")
        except OSError as e:
            log(f"fallback write failed: {e}")


def write_status(rep: Reporter, tracker: StatusTracker, final: bool = False,
                 exit_reason: str | None = None) -> None:
    payload = tracker.snapshot(findings=len(rep.seen), final=final, exit_reason=exit_reason)
    key = f"{STATUS_PREFIX}{tracker.commit}-{tracker.uuid}.json"
    if DRY_RUN:
        print(f"[dry-run] WOULD WRITE {key}\n{json.dumps(payload, indent=2)}\n{'-' * 60}")
        return
    if not BUCKET:
        return
    if not rep.put_json(key, payload):
        log(f"status write failed (will retry in {STATUS_INTERVAL}s)")


def baseline_from_journal(tracker: StatusTracker) -> None:
    """Seed each unit's gauges from its latest journal progress line, so this
    segment counts only deltas it observes — no double count after a
    reporter-only restart, and a joint restart reads as a reset (counted in
    full). Best-effort: no journal (local runs) just means baselines of zero."""
    for i in range(os.cpu_count() or 1):
        unit = f"pypiron-soak@{i}.service"
        try:
            out = subprocess.run(
                ["journalctl", "-u", unit, "-n", "2000", "-o", "cat", "--no-pager"],
                capture_output=True, text=True, timeout=30, check=True,
            ).stdout
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
            continue
        for line in reversed(out.splitlines()):
            m = PROGRESS_RE.match(line)
            if m:
                tracker.baseline(unit, (int(m.group(1)), int(m.group(2)), int(m.group(3))))
                break


def journal_events():
    """Yield (unit, message) from `journalctl -o json` lines on stdin."""
    for raw in sys.stdin:
        try:
            ev = json.loads(raw)
        except ValueError:
            continue
        msg = ev.get("MESSAGE")
        if isinstance(msg, list):  # journald encodes non-UTF8 payloads as byte arrays
            try:
                msg = bytes(msg).decode("utf-8", "replace")
            except (ValueError, TypeError):
                continue
        if not isinstance(msg, str):
            continue
        yield ev.get("_SYSTEMD_UNIT", "?"), msg


def main() -> int:
    rep = Reporter()
    tracker = StatusTracker(commit=read_commit())
    tracker.instance_id = imds("instance-id")
    tracker.instance_type = imds("instance-type")
    baseline_from_journal(tracker)
    log(f"started; segment {tracker.commit}-{tracker.uuid}; "
        f"sink={'s3://' + BUCKET + '/' + PREFIX if BUCKET else 'local-fallback'}"
        f"{' [DRY-RUN]' if DRY_RUN else ''}")

    def on_signal(signum, frame):
        raise Shutdown(signal.Signals(signum).name)

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    stop = threading.Event()

    def status_loop():
        while not stop.wait(STATUS_INTERVAL):
            write_status(rep, tracker)

    threading.Thread(target=status_loop, daemon=True).start()

    # Per-unit FAILED-block state, so concurrent cores can't interleave blocks.
    pending: dict[str, tuple[str, list[str]]] = {}  # unit -> (seed, violations)
    exit_reason = "stdin-eof"  # journalctl died; systemd restarts the pipeline
    try:
        for unit, line in journal_events():
            prog = PROGRESS_RE.match(line)
            if prog:
                tracker.observe(unit, (int(prog.group(1)), int(prog.group(2)), int(prog.group(3))))
                continue

            det = DETERMINISM_RE.match(line)
            if det:
                dseed, detail = det.group(1), det.group(2)
                rep.finding(
                    signature(f"DETERMINISM {detail}"),
                    f"cargo run --release --example vopr -- --seed {dseed}",
                    dseed,
                    f"DETERMINISM VIOLATION: {detail}",
                    "determinism",
                )
                continue

            m = FAILED_RE.match(line)
            if m:
                pending[unit] = (m.group(1), [])
                continue

            if unit in pending:
                seed, violations = pending[unit]
                repro = REPRODUCE_RE.match(line)
                if repro:
                    for v in violations:
                        sig = signature(v)
                        if sig:
                            rep.finding(sig, repro.group(1), seed, v, "violation")
                    del pending[unit]
                elif re.match(r"^  \S", line):
                    # Start of a violation entry (2-space indent). Keep only this
                    # first line; the optional multi-line `changed:`/`findings:`
                    # dump that follows is per-seed noise the signature strips.
                    violations.append(line.strip())
                # else: deeper-indented dump lines and blanks — ignore.
    except Shutdown as s:
        exit_reason = str(s).lower()
        log(f"shutting down ({exit_reason})")
    finally:
        stop.set()
        write_status(rep, tracker, final=True, exit_reason=exit_reason)

    return 0


if __name__ == "__main__":
    sys.exit(main())
