#!/usr/bin/env python3
"""Turn the VOPR soak's failure stream into deduped findings in S3.

The soak (`examples/vopr.rs --forever --rotate`) never exits on a finding: it
prints a `FAILED` block — the violations plus the exact deterministic repro
command — and explores the next seed. A single real bug fails a large fraction
of seeds, so the raw stream is thousands of near-identical blocks. This reader
collapses them to one object per *distinct* failure, keyed by a seed-agnostic
signature hash, and drops the exact repro in each.

It reads the merged soak journal on stdin (see pypiron-soak-reporter.service),
so it runs once per box. Design rules:

  - Dedup by S3 key. The object key is the signature hash, so N distinct bugs =
    N objects no matter how many seeds or boxes hit them; listing the prefix is
    the deduped finding set. No GitHub, no PAT — the instance's IAM role writes
    to one S3 prefix and nothing else.
  - Event-driven: a new distinct failure writes its object (and, if configured,
    an SNS notification) the moment it streams in.
  - Fail-open on surfacing: an S3/network hiccup never kills the finder. A
    finding we could not write is appended to a local fallback file for retry;
    the soak keeps running regardless.
  - Rate-capped: a bad binary can turn every seed red with many distinct-looking
    signatures. Past a per-hour cap the reader stops writing and shouts.

stdlib only; shells out to the `aws` CLI already on the box.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field

BUCKET = os.environ.get("PYPIRON_SOAK_S3_BUCKET", "")
PREFIX = os.environ.get("PYPIRON_SOAK_S3_PREFIX", "soak/findings/")
SNS_TOPIC = os.environ.get("PYPIRON_SOAK_SNS_TOPIC", "")  # optional ARN
AWS = os.environ.get("PYPIRON_SOAK_AWS", "aws")
REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", ""))
DRY_RUN = bool(os.environ.get("PYPIRON_SOAK_DRY_RUN"))
MAX_PER_HOUR = int(os.environ.get("PYPIRON_SOAK_MAX_ISSUES_PER_HOUR", "50"))
# Runtime file, outside any checkout, surviving restarts for retry.
FALLBACK = os.environ.get("PYPIRON_SOAK_FALLBACK", "/var/tmp/pypiron-soak-findings.fallback.jsonl")
TITLE_MAX = 120

# A FAILED block:
#   vopr: seed 1784515453 FAILED (1 violations):
#     AUDIT_ORDERING: bucket0 vopr-beta — truth mutation ...whl.tombstone@191 ...
#     ...possibly multi-line `changed: [ ... ]` dump...
#   reproduce: cargo run --release --example vopr -- --seed 1784515453 --nodes 3 ...
FAILED_RE = re.compile(r"^vopr: seed (\d+) FAILED \((\d+) violation")
REPRODUCE_RE = re.compile(r"^reproduce: (.+)$")
DETERMINISM_RE = re.compile(r"^vopr: DETERMINISM VIOLATION seed=(\d+): (.+)$")
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
        if self._put(h, payload):
            log(f"wrote finding {PREFIX}{h}.json — {payload['title']}")
            self._notify(payload)
        else:
            self._fallback(payload)

    def _put(self, h: str, payload: dict) -> bool:
        # Key by signature hash: idempotent, so concurrent boxes writing the same
        # new bug just overwrite identical content — no coordination needed.
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(payload, f)
            tmp = f.name
        try:
            return self._aws([
                "s3api", "put-object", "--bucket", BUCKET, "--key", f"{PREFIX}{h}.json",
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


def main() -> int:
    rep = Reporter()
    log(f"started; sink={'s3://' + BUCKET + '/' + PREFIX if BUCKET else 'local-fallback'}"
        f"{' [DRY-RUN]' if DRY_RUN else ''}")

    seed: str | None = None
    violations: list[str] = []
    for raw in sys.stdin:
        line = raw.rstrip("\n")

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
            seed, violations = m.group(1), []
            continue

        if seed is not None:
            repro = REPRODUCE_RE.match(line)
            if repro:
                for v in violations:
                    sig = signature(v)
                    if sig:
                        rep.finding(sig, repro.group(1), seed, v, "violation")
                seed, violations = None, []
            elif re.match(r"^  \S", line):
                # Start of a violation entry (2-space indent). Keep only this
                # first line; the optional multi-line `changed:`/`findings:` dump
                # that follows is per-seed noise the signature strips anyway.
                violations.append(line.strip())
            # else: deeper-indented dump lines and blanks — ignore.

    return 0


if __name__ == "__main__":
    sys.exit(main())
