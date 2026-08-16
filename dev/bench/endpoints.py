"""The canonical endpoint table and measurement core for the micro-benchmarks.

One table covers every route in src/app.rs (dev/bench/MICROBENCH.md constraint
1); a blackbox test parses the router source and fails when they drift. The
same table drives both lanes: the CI op-count asserts (tests/) and the tracked
latency harness (microbench.py).

Stdlib only.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import socket
import subprocess
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
REPO = BENCH_DIR.parent.parent
APP_RS = REPO / "src" / "app.rs"

ADMIN = ("admin", "secret")
UPLOADER = ("uploader", "uploadersecret")
# Bench-only signing key; enables POST /tokens. Never a real deployment's key.
TOKEN_SIGNING_KEY = "0" * 64

OPS = ("read", "write", "list", "delete")


# ------------------------------ endpoint table ------------------------------


@dataclass(frozen=True)
class Endpoint:
    name: str
    method: str
    path: str  # format string over {pkg} {version} {filename} {i}
    routes: tuple  # ((METHOD, "/route/path"), ...) covered in src/app.rs
    auth: tuple | None = None  # (user, pass) basic auth
    body: str | None = None  # body builder name (see build_body) or None
    expect: tuple = (200,)
    mutates: bool = False
    # Exact per-request storage-op pins {op: count}; None = measured, not
    # asserted. Cold = first request ever for this endpoint's target; warm =
    # steady state. Missing op keys pin to 0.
    cold_ops: dict | None = None
    warm_ops: dict | None = None
    # Loose sanity range for the warm response body size at the CI tier.
    bytes_range: tuple | None = None
    target: str = "pkg"  # "pkg" dedicated package | "global" | "none" | "probe"


def _e(**kw) -> Endpoint:
    return Endpoint(**kw)


ENDPOINTS: list = [
    # --- read paths, package-targeted (dedicated virgin package each: their
    # cold pins stay meaningful in a shared-process CI walk) ---
    _e(
        name="simple-pkg-html",
        method="GET",
        path="/simple/{pkg}/",
        routes=(("GET", "/simple/:package"), ("GET", "/simple/:package/")),
        cold_ops={"read": 1},
        warm_ops={},
        bytes_range=(200, 500000),
    ),
    _e(
        name="simple-pkg-json",
        method="GET",
        path="/simple/{pkg}/index.json",
        routes=(("GET", "/simple/:package/index.json"),),
        cold_ops={"read": 1},
        warm_ops={},
        bytes_range=(200, 100000),
    ),
    _e(
        name="project-page",
        method="GET",
        path="/project/{pkg}/",
        routes=(("GET", "/project/:package"), ("GET", "/project/:package/")),
        cold_ops={"read": 42, "list": 31},
        warm_ops={},
        bytes_range=(500, 500000),
    ),
    _e(
        name="project-version-page",
        method="GET",
        path="/project/{pkg}/{version}/",
        routes=(("GET", "/project/:package/:version"), ("GET", "/project/:package/:version/")),
        cold_ops={"read": 68, "list": 31},
        warm_ops={},
        bytes_range=(500, 500000),
    ),
    _e(
        name="artifact-download",
        method="GET",
        path="/files/{pkg}/{filename}",
        routes=(("GET", "/files/:package/:filename"),),
        cold_ops={"read": 1},
        warm_ops={"read": 1},
        bytes_range=(0, 10),
    ),
    _e(
        name="stats-pkg",
        method="GET",
        path="/stats/downloads/{pkg}",
        routes=(("GET", "/stats/:metric/:package"),),
        cold_ops={"read": 30, "list": 30},
        warm_ops={},  # (metric, package)-keyed TTL cache; warm hits are 0 storage ops
        bytes_range=(2, 5000),
    ),
    _e(
        name="sync-local-index",
        method="GET",
        path="/sync/local-index/{pkg}",
        routes=(("GET", "/sync/local-index/:package"),),
        auth=ADMIN,
        cold_ops={"read": 2},
        warm_ops={"read": 2},
        bytes_range=(2, 100000),
    ),
    # --- read paths, global ---
    _e(
        name="simple-root-html",
        method="GET",
        path="/simple/",
        routes=(("GET", "/simple"), ("GET", "/simple/")),
        target="global",
        cold_ops={"read": 1},
        warm_ops={},
        bytes_range=(5000, 200000),
    ),
    _e(
        name="simple-root-json",
        method="GET",
        path="/simple/index.json",
        routes=(("GET", "/simple/index.json"),),
        target="global",
        cold_ops={"read": 1},
        warm_ops={},
        bytes_range=(5000, 200000),
    ),
    _e(
        name="root-page",
        method="GET",
        path="/",
        routes=(("GET", "/"),),
        target="global",
        cold_ops={"read": 30, "list": 4},
        warm_ops={},
        bytes_range=(5000, 500000),
    ),
    _e(
        name="favicon",
        method="GET",
        path="/favicon.ico",
        routes=(("GET", "/favicon.ico"),),
        target="global",
        cold_ops={},
        warm_ops={},
        bytes_range=(1000, 100000),
    ),
    _e(
        name="projects-page",
        method="GET",
        path="/projects/",
        routes=(("GET", "/projects"), ("GET", "/projects/")),
        target="global",
        cold_ops={},
        warm_ops={},
        bytes_range=(5000, 500000),
    ),
    _e(
        name="downloads-page",
        method="GET",
        path="/downloads/",
        routes=(("GET", "/downloads"), ("GET", "/downloads/")),
        target="global",
        cold_ops={},
        warm_ops={},
        bytes_range=(5000, 500000),
    ),
    _e(
        name="stats-summary",
        method="GET",
        path="/stats/downloads",
        routes=(("GET", "/stats/:metric"),),
        target="global",
        cold_ops={"read": 30, "list": 4},
        warm_ops={},
        bytes_range=(2, 5000),
    ),
    _e(
        name="audit-page",
        method="GET",
        path="/audit/",
        routes=(("GET", "/audit"), ("GET", "/audit/")),
        auth=ADMIN,
        target="global",
        cold_ops={"read": 1},
        warm_ops={"read": 1},
        bytes_range=(1000, 500000),
    ),
    _e(
        name="audit-json",
        method="GET",
        path="/audit.json",
        routes=(("GET", "/audit.json"),),
        auth=ADMIN,
        target="global",
        expect=(200, 404),  # 404 until an audit pass has stored a report
        cold_ops={"read": 1},
        warm_ops={"read": 1},
        bytes_range=(2, 100000),
    ),
    _e(
        name="sync-cursors-get",
        method="GET",
        path="/sync/cursors",
        routes=(("GET", "/sync/cursors"),),
        auth=ADMIN,
        target="global",
        expect=(200, 204, 404),
        cold_ops={"read": 1},
        warm_ops={"read": 1},
        bytes_range=(1, 10000),
    ),
    _e(
        name="advisories-feed-get",
        method="GET",
        path="/advisories/feed",
        routes=(("GET", "/advisories/feed"),),
        target="global",
        expect=(200, 404),  # 404 until a snapshot has been stored
        cold_ops={"list": 1},
        warm_ops={"list": 1},
        bytes_range=(0, 10),
    ),
    _e(
        name="health",
        method="GET",
        path="/health",
        routes=(("GET", "/health"),),
        target="global",
        cold_ops={},
        warm_ops={},
        bytes_range=(2, 100),
    ),
    _e(
        name="ready",
        method="GET",
        path="/ready",
        routes=(("GET", "/ready"),),
        target="global",
        cold_ops={"read": 1},
        warm_ops={"read": 1},
        bytes_range=(2, 100),
    ),
    _e(
        name="metrics",
        method="GET",
        path="/metrics",
        routes=(("GET", "/metrics"),),
        target="global",
        cold_ops={},
        warm_ops={},
        bytes_range=(500, 100000),
    ),
    _e(
        name="fallback-404",
        method="GET",
        path="/definitely-not-a-route",
        routes=(("*", "(fallback)"),),
        target="global",
        expect=(404,),
        cold_ops={},
        warm_ops={},
        bytes_range=(2, 1000),
    ),
    # --- write paths: one probe-package lifecycle, in table order ---
    _e(
        name="upload-legacy",
        method="POST",
        path="/legacy/",
        routes=(("POST", "/legacy"), ("POST", "/legacy/")),
        auth=UPLOADER,
        body="wheel_upload",
        expect=(200, 201),
        mutates=True,
        target="probe",
        cold_ops={"read": 11, "write": 11, "list": 2, "delete": 1},
        warm_ops={"read": 11, "write": 11, "list": 2, "delete": 1},
        bytes_range=(0, 100),
    ),
    _e(
        name="tokens-mint",
        method="POST",
        path="/tokens",
        routes=(("POST", "/tokens"),),
        auth=UPLOADER,
        body="empty",
        expect=(200, 201),
        target="global",  # stateless signing: zero storage ops
        cold_ops={},
        warm_ops={},
        bytes_range=(50, 1000),
    ),
    _e(
        name="yank-set",
        method="POST",
        path="/files/{probe_pkg}/{probe_filename}/yank",
        routes=(("POST", "/files/:package/:filename/yank"),),
        auth=ADMIN,
        body="empty",
        expect=(200, 201, 204),
        mutates=True,
        target="probe",
        cold_ops={"read": 6, "write": 5, "list": 2, "delete": 1},
        warm_ops={"read": 6, "write": 5, "list": 2, "delete": 1},
        bytes_range=(0, 100),
    ),
    _e(
        name="yank-clear",
        method="DELETE",
        path="/files/{probe_pkg}/{probe_filename}/yank",
        routes=(("DELETE", "/files/:package/:filename/yank"),),
        auth=ADMIN,
        expect=(200, 204),
        mutates=True,
        target="probe",
        cold_ops={"read": 6, "write": 5, "list": 2, "delete": 1},
        warm_ops={"read": 6, "write": 5, "list": 2, "delete": 1},
        bytes_range=(0, 100),
    ),
    _e(
        name="project-status-set",
        method="POST",
        path="/project/{probe_pkg}/status",
        routes=(("POST", "/project/:package/status"),),
        auth=ADMIN,
        body="status_doc",
        expect=(200, 201, 204),
        mutates=True,
        target="probe",
        cold_ops={"read": 5, "write": 5, "list": 2, "delete": 1},
        warm_ops={"read": 5, "write": 5, "list": 2, "delete": 1},
        bytes_range=(0, 100),
    ),
    _e(
        name="project-status-clear",
        method="DELETE",
        path="/project/{probe_pkg}/status",
        routes=(("DELETE", "/project/:package/status"),),
        auth=ADMIN,
        expect=(200, 204),
        mutates=True,
        target="probe",
        cold_ops={"read": 5, "write": 4, "list": 2, "delete": 2},
        warm_ops={"read": 5, "write": 4, "list": 2, "delete": 2},
        bytes_range=(0, 100),
    ),
    _e(
        name="sync-cursors-put",
        method="PUT",
        path="/sync/cursors",
        routes=(("PUT", "/sync/cursors"),),
        auth=ADMIN,
        body="cursors_json",
        expect=(200, 204),
        mutates=True,
        target="global",
        cold_ops={"write": 1},
        warm_ops={"write": 1},
        bytes_range=(0, 100),
    ),
    _e(
        name="advisories-feed-put",
        method="PUT",
        path="/advisories/feed",
        routes=(("PUT", "/advisories/feed"),),
        auth=ADMIN,
        body="osv_zip",
        expect=(200, 201, 204),
        mutates=True,
        target="global",
        cold_ops={"write": 1, "list": 1},
        warm_ops={"write": 1, "list": 1},
        bytes_range=(0, 100),
    ),
    _e(
        name="files-delete",
        method="DELETE",
        path="/files/{probe_pkg}/{probe_filename}",
        routes=(("DELETE", "/files/:package/:filename"),),
        auth=ADMIN,
        expect=(200, 204),
        mutates=True,
        target="probe",
        cold_ops={"read": 7, "write": 6, "list": 3, "delete": 5},
        warm_ops={"read": 7, "write": 6, "list": 3, "delete": 5},
        bytes_range=(0, 100),
    ),
]


def covered_routes() -> set:
    out = set()
    for ep in ENDPOINTS:
        out.update(ep.routes)
    return out


# --------------------------- router drift parsing ---------------------------

_METHOD_RE = re.compile(r"\b(get|post|put|delete|patch|head)\s*\(")


def parse_app_routes(text: str | None = None) -> set:
    """(METHOD, path) for every .route() in src/app.rs, plus ("*", "(fallback)")."""
    text = APP_RS.read_text() if text is None else text
    found = set()
    for m in re.finditer(r"\.route\(", text):
        depth, i = 1, m.end()
        while depth and i < len(text):
            depth += {"(": 1, ")": -1}.get(text[i], 0)
            i += 1
        span = text[m.end() : i - 1]
        path_m = re.search(r'"([^"]+)"', span)
        if not path_m:
            continue
        for meth in _METHOD_RE.findall(span):
            found.add((meth.upper(), path_m.group(1)))
    if ".fallback(" in text:
        found.add(("*", "(fallback)"))
    return found


# ------------------------------ server lifecycle -----------------------------


def find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# Quiesced: no boot audit, day-long sweep/worker intervals (the worker nudge
# still makes uploads visible), no advisory fetch. Zero background storage ops,
# so /metrics op deltas belong entirely to the requests we send.
QUIESCE_ARGS = (
    "--audit-on-boot",
    "false",
    "--reconcile-interval-secs",
    "86400",
    "--worker-interval-secs",
    "86400",
    # Day-long index-cache TTL: warm hits stay 0-op instead of racing the 1s
    # default's revalidation read (single-node invalidation is exact, so the
    # long TTL never serves stale bytes here).
    "--index-cache-ttl-secs",
    "86400",
)
# Sweep-at-boot: same silence afterward, but the boot audit runs and renders
# every index. Used to build caches and to prepare fresh CI trees.
SWEEP_ARGS = (
    "--audit-on-boot",
    "true",
    "--reconcile-interval-secs",
    "86400",
    "--worker-interval-secs",
    "86400",
    "--index-cache-ttl-secs",
    "86400",
)


def start_server(bin_path, data_dir, log_path, *, sweep: bool, port: int | None = None) -> dict:
    port = port or find_free_port()
    bind = f"127.0.0.1:{port}"
    args = [
        str(bin_path),
        "serve",
        "--bind-addr",
        bind,
        "--data-dir",
        str(data_dir),
        "--admin-user",
        ADMIN[0],
        "--admin-pass",
        ADMIN[1],
        "--uploader-user",
        UPLOADER[0],
        "--uploader-pass",
        UPLOADER[1],
        "--token-signing-key",
        TOKEN_SIGNING_KEY,
        *(SWEEP_ARGS if sweep else QUIESCE_ARGS),
    ]
    env = dict(__import__("os").environ, PYPIRON_ADVISORY_FEED="")
    log_file = open(log_path, "w")
    t0 = time.monotonic()
    proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    base = f"http://{bind}"
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server died at startup; log: {log_path}")
        try:
            urllib.request.urlopen(f"{base}/ready", timeout=2)
            break
        except urllib.error.URLError:
            time.sleep(0.02)
    else:
        raise TimeoutError(f"server never became ready; log: {log_path}")
    return {
        "proc": proc,
        "base": base,
        "log_file": log_file,
        "startup_secs": time.monotonic() - t0,
    }


def stop_server(server: dict) -> None:
    server["proc"].terminate()
    try:
        server["proc"].wait(timeout=10)
    except subprocess.TimeoutExpired:
        server["proc"].kill()
        server["proc"].wait(timeout=10)
    server["log_file"].close()


def wait_swept(base: str, expected_packages: int, timeout: float) -> float:
    """Seconds until the boot audit has processed every fabricated package."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        text = fetch(base, "GET", "/metrics")[2].decode()
        done = sum(
            int(m.group(1))
            for m in re.finditer(
                r"^pypiron_audit_packages_(?:rebuilt|skipped)_total (\d+)$", text, re.M
            )
        )
        finished = re.search(r"^pypiron_audit_last_duration_seconds (\S+)$", text, re.M)
        if done >= expected_packages and finished and float(finished.group(1)) > 0:
            return time.monotonic() - t0
        time.sleep(0.5)
    raise TimeoutError(f"boot audit never covered {expected_packages} packages")


# ------------------------------- request layer -------------------------------


def make_wheel(name: str, version: str) -> bytes:
    dist = name.replace("-", "_")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            f"{dist}-{version}.dist-info/METADATA",
            f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n",
        )
        zf.writestr(f"{dist}-{version}.dist-info/WHEEL", "Wheel-Version: 1.0\n")
    return buf.getvalue()


def wheel_upload_body(name: str, version: str) -> tuple:
    wheel = make_wheel(name, version)
    filename = f"{name.replace('-', '_')}-{version}-py3-none-any.whl"
    boundary = "microbench"
    parts = []
    for fieldname, value in [
        (":action", "file_upload"),
        ("protocol_version", "1"),
        ("name", name),
        ("version", version),
        ("filetype", "bdist_wheel"),
        ("sha256_digest", hashlib.sha256(wheel).hexdigest()),
    ]:
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{fieldname}"\r\n\r\n{value}\r\n'.encode()
        )
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="content"; filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n".encode()
        + wheel
        + b"\r\n"
    )
    parts.append(f"--{boundary}--\r\n".encode())
    return b"".join(parts), f"multipart/form-data; boundary={boundary}", filename


def make_osv_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr(
            "MICRO-0000-0001.json",
            json.dumps(
                {
                    "id": "MICRO-0000-0001",
                    "affected": [
                        {
                            "package": {"ecosystem": "PyPI", "name": "microbench-nonexistent"},
                            "versions": ["0.0.0"],
                        }
                    ],
                }
            ),
        )
    return buf.getvalue()


@dataclass
class Ctx:
    """Per-run request context: dedicated target packages and probe state."""

    targets: dict  # endpoint name -> package name (per-pkg endpoints)
    versions: dict  # package name -> version of its first file
    probe_prefix: str = "mb-probe"
    probe_filenames: dict = field(default_factory=dict)  # i -> uploaded wheel filename

    def probe_pkg(self, i: int) -> str:
        return f"{self.probe_prefix}-{i}"


def assign_targets(data_dir: Path) -> Ctx:
    """Give every per-package endpoint its own virgin target package.

    Deterministic: sorted package names with >=3 files, one per endpoint in
    table order. A dedicated target keeps per-package cold pins honest in a
    shared-process walk — no other endpoint has warmed its caches.
    """
    packages_root = Path(data_dir) / "packages"
    per_pkg = [ep for ep in ENDPOINTS if ep.target == "pkg"]
    targets: dict = {}
    versions: dict = {}
    names = sorted(p.name for p in packages_root.iterdir() if p.is_dir())
    it = iter(names)
    for ep in per_pkg:
        for name in it:
            files = sorted(f.name for f in (packages_root / name).glob("*.tar.gz"))
            if len(files) >= 3:
                targets[ep.name] = name
                versions[name] = "0.0.0"
                break
        else:
            raise RuntimeError("not enough >=3-file packages to dedicate targets")
    return Ctx(targets=targets, versions=versions)


def build_request(ep: Endpoint, ctx: Ctx, i: int) -> tuple:
    """(path, body_bytes|None, content_type|None) for iteration i."""
    pkg = ctx.targets.get(ep.name, "")
    version = ctx.versions.get(pkg, "0.0.0")
    filename = f"{pkg}-{version}.tar.gz"
    probe_pkg = ctx.probe_pkg(i)
    probe_filename = ctx.probe_filenames.get(i, "")
    path = ep.path.format(
        pkg=pkg,
        version=version,
        filename=filename,
        probe_pkg=probe_pkg,
        probe_filename=probe_filename,
        i=i,
    )
    if ep.body is None:
        return path, None, None
    if ep.body == "empty":
        return path, b"", None
    if ep.body == "wheel_upload":
        body, ctype, wheel_name = wheel_upload_body(probe_pkg, "0.0.1")
        ctx.probe_filenames[i] = wheel_name
        return path, body, ctype
    if ep.body == "status_doc":
        return path, json.dumps({"status": "quarantined"}).encode(), "application/json"
    if ep.body == "cursors_json":
        return path, json.dumps({"microbench": str(i)}).encode(), "application/json"
    if ep.body == "osv_zip":
        return path, make_osv_zip(), "application/zip"
    raise ValueError(f"unknown body builder {ep.body}")


def fetch(base: str, method: str, path: str, body=None, ctype=None, auth=None) -> tuple:
    """(status, elapsed_ms, response_bytes) — one serial request."""
    req = urllib.request.Request(base + path, data=body, method=method)
    req.add_header("Accept-Encoding", "identity")
    if ctype:
        req.add_header("Content-Type", ctype)
    if auth:
        cred = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        req.add_header("Authorization", f"Basic {cred}")
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
            return resp.status, (time.monotonic() - t0) * 1000, data
    except urllib.error.HTTPError as e:
        data = e.read()
        return e.code, (time.monotonic() - t0) * 1000, data


def hit(base: str, ep: Endpoint, ctx: Ctx, i: int) -> tuple:
    path, body, ctype = build_request(ep, ctx, i)
    status, ms, data = fetch(base, ep.method, path, body, ctype, ep.auth)
    if status not in ep.expect:
        raise AssertionError(f"{ep.name}: {ep.method} {path} -> {status}: {data[:200]!r}")
    return status, ms, data


def ops_snapshot(base: str) -> dict:
    _, _, data = fetch(base, "GET", "/metrics")
    out = dict.fromkeys(OPS, 0)
    for m in re.finditer(r'^pypiron_storage_ops_total\{op="(\w+)"\} (\d+)$', data.decode(), re.M):
        out[m.group(1)] = int(m.group(2))
    return out


def ops_delta(before: dict, after: dict) -> dict:
    return {op: after[op] - before[op] for op in OPS if after[op] != before[op]}


def ops_settled(base: str, timeout: float = 20.0, quiet: float = 0.15, streak: int = 3) -> dict:
    """The op snapshot once the worker's async tail has stopped moving.

    A mutation returns its HTTP response before the nudged worker finishes the
    index rebuild. One quiet gap is not proof: on a loaded CI runner the worker
    can pause >50ms mid-tail and leak its next op into the following endpoint's
    measurement window (seen as a stray delete on sync-cursors-put). Require
    `streak` consecutive identical snapshots `quiet` apart — sustained silence.
    """
    last = ops_snapshot(base)
    same = 0
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(quiet)
        cur = ops_snapshot(base)
        if cur == last:
            same += 1
            if same >= streak:
                return cur
        else:
            same = 0
            last = cur
    raise TimeoutError("storage ops never settled")


def measure_ops(base: str, ep: Endpoint, ctx: Ctx, i: int) -> dict:
    """Per-request storage-op delta for one serial request (quiesced server).

    The delta of a mutation includes its async index rebuild — the true total
    cost of the write, which is the number worth pinning.
    """
    before = ops_settled(base)
    hit(base, ep, ctx, i)
    after = ops_settled(base) if ep.mutates else ops_snapshot(base)
    return ops_delta(before, after)
