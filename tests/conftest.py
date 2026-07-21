from __future__ import annotations

import base64
import hashlib
import hmac
import http.client
import os
import re
import subprocess
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple
from urllib.parse import quote, unquote, urlencode, urlsplit

import pytest

from .helpers import (
    _http_request,
    cmd_exists,
    ensure_built,
    find_free_port,
    kill_process_tree,
    run_checked,
    run_returncode,
    uv_python_path,
    wait_http_ok,
    wait_http_responding,
)

COMPAT_CLIENTS = ("pip", "uv", "poetry", "pdm", "twine", "flit", "hatch", "pipenv")
COMPAT_FEATURES = (
    "upload",
    "install",
    "resolve",
    "pep658-metadata",
    "yank",
    "hash-check",
    "exclude-newer",
)
COMPAT_OUTCOME_SYMBOLS = {
    "failed": "\u274c",
    "xfailed": "\u274c",
    "passed": "\u2705",
    "skipped": "?",
}
COMPAT_OUTCOME_PRECEDENCE = ("failed", "xfailed", "passed", "skipped")
COMPAT_VERSION_LABELS = {
    "venv-seeded": "venv-seeded",
    "system": "system",
    "dev-dependency": "dev-dependency",
}


def pytest_addoption(parser):
    parser.addoption(
        "--write-compat-doc",
        action="store_true",
        help="Write docs/reference/compatibility.md from tests marked compat(client, feature).",
    )


def pytest_configure(config):
    config._compat_results = []


def pytest_collection_modifyitems(config, items):
    clients = set(COMPAT_CLIENTS)
    features = set(COMPAT_FEATURES)
    errors = []
    for item in items:
        for marker in item.iter_markers("compat"):
            if marker.kwargs or len(marker.args) != 2:
                errors.append(f"{item.nodeid}: compat marker must be compat(client, feature)")
                continue
            client, feature = marker.args
            if client not in clients:
                errors.append(
                    f"{item.nodeid}: unknown compat client {client!r}; "
                    f"expected one of {', '.join(COMPAT_CLIENTS)}"
                )
            if feature not in features:
                errors.append(
                    f"{item.nodeid}: unknown compat feature {feature!r}; "
                    f"expected one of {', '.join(COMPAT_FEATURES)}"
                )
    if errors:
        raise pytest.UsageError("\n".join(errors))


def _tail(path, lines: int = 60) -> str:
    try:
        text = Path(path).read_text(errors="replace")
    except OSError as exc:
        return f"(unreadable: {exc})"
    tail = text.splitlines()[-lines:]
    return "\n".join(tail) if tail else "(empty)"


def _minio_ls_unchecked(minio: Dict, bucket: str, prefix: str) -> str:
    """Best-effort bucket listing for diagnostics: never skips or raises."""
    if not minio.get("endpoint"):
        return "(no emulator endpoint)"
    try:
        keys = _s3_list(minio, bucket, prefix)
    except (AssertionError, OSError) as exc:
        return f"(list failed: {exc})"
    return "\n".join(keys) or "(empty)"


def _storage_diagnostics(item) -> str | None:
    """On a failure in an S3/mixed fixture, collect the evidence a bare assert
    hides: the proxy request log, the pypiron server log, and the bucket
    contents. Best-effort — a broken probe must not mask the real failure."""
    server = None
    candidates = list(getattr(item, "funcargs", {}).values())
    # Pair fixtures (s3_servers_multi) nest their server dicts one level down
    # under "left"/"right"; scan those too.
    candidates.extend(
        nested
        for value in candidates
        if isinstance(value, dict)
        for nested in value.values()
        if isinstance(nested, dict)
    )
    for value in candidates:
        if (
            isinstance(value, dict)
            and "log_path" in value
            and ("faults" in value or "minio" in value or "s3" in value)
        ):
            server = value
            break
    if server is None:
        return None
    lines = [f"--- pypiron server log tail ({server.get('log_path')}) ---"]
    lines.append(_tail(server.get("log_path")))
    faults = server.get("faults")
    proxy_log = getattr(faults, "log_path", None) if faults else None
    if proxy_log:
        lines.append(f"--- s3 fault-proxy request log tail ({proxy_log}) ---")
        lines.append(_tail(proxy_log))
    minio = server.get("minio") or server.get("s3")
    if isinstance(minio, dict):
        for bucket in minio.get("buckets") or ([minio["bucket"]] if minio.get("bucket") else []):
            lines.append(f"--- objects under {bucket}/packages/ ---")
            lines.append(_minio_ls_unchecked(minio, bucket, "packages/"))
    return "\n".join(lines)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when in ("setup", "call") and report.failed:
        # A report section, not print(): xdist swallows worker stdout, but
        # sections ride the report back to the controller and land in the
        # failure output either way.
        diagnostics = _storage_diagnostics(item)
        if diagnostics:
            report.sections.append(("storage failure diagnostics", diagnostics))
    markers = list(item.iter_markers("compat"))
    if not markers:
        return

    compat_outcome = None
    if report.failed:
        compat_outcome = "failed"
    elif report.when == "setup" and report.skipped:
        compat_outcome = "skipped"
    elif report.when == "call":
        if report.skipped and hasattr(report, "wasxfail"):
            compat_outcome = "xfailed"
        elif report.skipped:
            compat_outcome = "skipped"
        elif report.passed:
            compat_outcome = "passed"

    if compat_outcome is None:
        return

    for marker in markers:
        client, feature = marker.args
        item.config._compat_results.append((client, feature, compat_outcome))


def pytest_sessionfinish(session, exitstatus):
    if not session.config.getoption("--write-compat-doc"):
        return
    # Compat results accumulate in each process's config; under xdist they
    # stay in the workers and the controller would overwrite the doc with an
    # empty matrix. Workers must never write (each holds a partial matrix),
    # and the controller fails loudly instead — `make compat` passes -n 0.
    if hasattr(session.config, "workerinput"):
        return
    if session.config.pluginmanager.hasplugin("dsession"):
        raise pytest.UsageError("--write-compat-doc needs a serial run: pass -n 0")
    _write_compat_doc(Path(session.config.rootpath), session.config._compat_results)


def _write_compat_doc(repo_root: Path, results: list[tuple[str, str, str]]) -> None:
    from .helpers import CLIENT_PINS

    doc_path = repo_root / "docs" / "reference" / "compatibility.md"
    doc_path.parent.mkdir(parents=True, exist_ok=True)

    by_cell = {(client, feature): [] for client in COMPAT_CLIENTS for feature in COMPAT_FEATURES}
    for client, feature, outcome in results:
        by_cell[(client, feature)].append(outcome)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    revision = _git_short_head(repo_root)

    lines = [
        "<!-- GENERATED \u2014 do not edit. Regenerate with `make compat`. -->",
        "",
        "# Client compatibility",
        "",
        "Every major Python packaging tool works with pypiron. This matrix shows "
        "which workflows are verified for each client \u2014 every \u2705 is backed by "
        "an integration test that runs the real client binary against a real "
        "pypiron server.",
        "",
        "All listed clients install packages, and the ones that publish can "
        "upload; the advanced columns vary by what each tool implements. Check "
        "yours before you deploy.",
        "",
        "| Client | " + " | ".join(COMPAT_FEATURES) + " |",
        "| --- | " + " | ".join("---" for _ in COMPAT_FEATURES) + " |",
    ]

    for client in COMPAT_CLIENTS:
        cells = [_compat_cell(by_cell[(client, feature)]) for feature in COMPAT_FEATURES]
        lines.append("| " + client + " | " + " | ".join(cells) + " |")

    lines.extend(
        [
            "",
            "Legend: \u2705 verified, \u274c known incompatibility, ? not verified "
            "in this run, \u2014 not tested or not applicable.",
            "",
            "What the columns mean:",
            "",
            "- **upload** \u2014 publish a distribution to the server.",
            "- **install** \u2014 install a package from the server.",
            "- **resolve** \u2014 resolve dependencies against the server's index.",
            "- **pep658-metadata** \u2014 read a file's metadata without downloading "
            "the whole wheel, for faster resolves.",
            "- **yank** \u2014 honor yanked releases, skipping withdrawn versions.",
            "- **hash-check** \u2014 verify downloads against expected hashes.",
            "- **exclude-newer** \u2014 ignore releases newer than a chosen date.",
            "",
            "## Client versions",
            "",
            "| Client | Tested version |",
            "| --- | --- |",
        ]
    )
    for client in COMPAT_CLIENTS:
        lines.append(f"| {client} | {_client_version_label(CLIENT_PINS[client])} |")

    lines.extend(["", f"<sub>Generated {generated_at} from revision `{revision}`.</sub>"])

    doc_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _compat_cell(outcomes: list[str]) -> str:
    if not outcomes:
        return "\u2014"
    seen = set(outcomes)
    for outcome in COMPAT_OUTCOME_PRECEDENCE:
        if outcome in seen:
            return COMPAT_OUTCOME_SYMBOLS[outcome]
    return "\u2014"


def _client_version_label(pin: str) -> str:
    return COMPAT_VERSION_LABELS.get(pin, pin)


def _git_short_head(repo_root: Path) -> str:
    try:
        cp = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return cp.stdout.strip() or "unknown"


# ----------------------------- Basic path fixtures ----------------------------


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def uv_path() -> str:
    uv = os.environ.get("UV", "")
    if uv and Path(uv).exists():
        return uv
    if not cmd_exists("uv"):
        pytest.skip("uv is required for these integration tests; not found on PATH")
    return "uv"


@pytest.fixture(scope="session")
def cargo_path() -> str:
    if not cmd_exists("cargo"):
        pytest.skip("cargo is required to build the pypiron server; not found on PATH")
    return "cargo"


@pytest.fixture(scope="session")
def pypiron_bin(repo_root: Path, cargo_path: str) -> Path:
    return ensure_built(repo_root)


@pytest.fixture(scope="session")
def pypiron_release_bin(repo_root: Path, cargo_path: str) -> Path:
    """Release binary, for perf tests — debug-build numbers are meaningless."""
    return ensure_built(repo_root, release=True)


# ----------------------------- uv venv fixture --------------------------------


@pytest.fixture()
def uv_venv(tmp_path_factory, uv_path: str) -> Path:
    """A fresh uv-managed venv; returns its python path."""
    venv_dir = tmp_path_factory.mktemp("uv-venv")
    run_checked([uv_path, "venv", str(venv_dir)])
    py = uv_python_path(venv_dir)
    assert py.exists(), f"uv venv python not found at {py}"
    return py


@pytest.fixture()
def pip_venv(tmp_path_factory, uv_path: str) -> Path:
    """A fresh venv seeded with pip; returns its python path."""
    venv_dir = tmp_path_factory.mktemp("pip-venv")
    run_checked([uv_path, "venv", "--seed", str(venv_dir)])
    py = uv_python_path(venv_dir)
    assert py.exists(), f"uv venv python not found at {py}"
    return py


# ---------------------------- Disk server fixture -----------------------------


# Credential profiles for _start_disk_server: the serve args to pass and the
# credential keys to advertise in the yielded dict.
#   "full"            — admin (everything, incl. mirror/delete/yank) plus a
#                       publish-only uploader; `user`/`password` mirror the admin
#                       credential (a superset) so any-operation tests keep working.
#   "admin-pass-only" — only `--admin-pass`; the username defaults to `admin`.
#   "none"            — no credentials at all: read-only, every write disabled.
_DISK_SERVER_CREDS = {
    "full": {
        "args": [
            "--admin-user",
            "admin",
            "--admin-pass",
            "secret",
            "--uploader-user",
            "uploader",
            "--uploader-pass",
            "uploadersecret",
        ],
        "extra": {
            "user": "admin",
            "password": "secret",
            "admin_user": "admin",
            "admin_password": "secret",
            "uploader_user": "uploader",
            "uploader_password": "uploadersecret",
        },
    },
    "admin-pass-only": {
        "args": ["--admin-pass", "secret"],
        "extra": {"admin_user": "admin", "admin_password": "secret"},
    },
    "none": {"args": [], "extra": {}},
}


def _start_disk_server(
    tmp_path_factory, bin_path: Path, extra_args=(), extra_env=None, creds="full"
) -> Iterator[Dict]:
    data_dir = tmp_path_factory.mktemp("pypiron-data")
    log_path = data_dir.parent / f"{data_dir.name}-server.log"
    port = find_free_port()
    bind = f"127.0.0.1:{port}"

    profile = _DISK_SERVER_CREDS[creds]
    args = [
        str(bin_path),
        "serve",
        "--bind-addr",
        bind,
        "--data-dir",
        str(data_dir),
        *profile["args"],
        "--worker-interval-secs",
        "1",
        *extra_args,
    ]

    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info,pypiron=debug")
    # Advisory blocking is on by default and would fetch the ~30 MB OSV export at
    # startup — non-hermetic and slow for every test server. Disable it by default;
    # advisory tests re-enable it explicitly (a --advisory-feed CLI flag overrides
    # this, and the implicit-default tests hand-roll their own start).
    env.setdefault("PYPIRON_ADVISORY_FEED", "")
    if extra_env:
        env.update(extra_env)

    # Logs go to a file: an undrained PIPE fills up and deadlocks the server.
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        try:
            # Any HTTP status counts as up: read-auth servers answer 401 here.
            wait_http_responding(f"http://{bind}/simple/index.json", timeout=20.0)
            yield {
                "bind": bind,
                "base_url": f"http://{bind}",
                "legacy": f"http://{bind}/legacy/",
                "simple": f"http://{bind}/simple/",
                "data_dir": data_dir,
                "log_path": log_path,
                "proc": proc,
                **profile["extra"],
            }
        finally:
            kill_process_tree(proc)


@pytest.fixture()
def disk_server(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """pypiron in disk mode with basic auth for uploads."""
    yield from _start_disk_server(tmp_path_factory, pypiron_bin)


@pytest.fixture()
def disk_server_project_labels(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server that attributes /metrics to per-client projects. Opt-in:
    /metrics is unauthenticated, so the labels leak project names by default."""
    yield from _start_disk_server(
        tmp_path_factory, pypiron_bin, extra_args=["--metrics-project-labels"]
    )


@pytest.fixture()
def disk_server_release(tmp_path_factory, pypiron_release_bin: Path) -> Iterator[Dict]:
    """Disk-mode server running the release binary (perf tests)."""
    yield from _start_disk_server(tmp_path_factory, pypiron_release_bin)


@pytest.fixture()
def disk_server_fast_reconcile(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server with an aggressive reconcile sweep (reconciler tests)."""
    yield from _start_disk_server(
        tmp_path_factory, pypiron_bin, extra_args=["--reconcile-interval-secs", "2"]
    )


# ------------------------------ devpi source ----------------------------------

# devpi is the incumbent private index a migration drains from. It is not on the
# PATH — it is fetched on demand via `uv tool run`, pinned so the fixture is
# reproducible. Reads are anonymous; uploads require the index owner's password,
# which is what the migration's --source-user/--source-pass exercise.
DEVPI_SERVER_PIN = "6.15.0"
DEVPI_CLIENT_PIN = "7.2.0"
DEVPI_USER = "migrate"
DEVPI_PASS = "migrate-secret"
DEVPI_INDEX = "dev"


def _start_devpi_server(tmp_path_factory, uv: str) -> Iterator[Dict]:
    server_dir = tmp_path_factory.mktemp("devpi")
    client_dir = server_dir / "client"
    log_path = server_dir.parent / f"{server_dir.name}-devpi.log"
    port = find_free_port()
    base = f"http://127.0.0.1:{port}"

    server_from = [uv, "tool", "run", "--from", f"devpi-server=={DEVPI_SERVER_PIN}"]
    client_env = {**os.environ, "DEVPI_CLIENTDIR": str(client_dir)}

    def devpi(*args, timeout=120):
        run_checked(
            [uv, "tool", "run", "--from", f"devpi-client=={DEVPI_CLIENT_PIN}", "devpi", *args],
            env=client_env,
            timeout=timeout,
        )

    # The first `uv tool run` fetches devpi (slow, network). If it can't be
    # fetched or initialized (offline, cache miss), skip cleanly — the S3/compat
    # pattern for an optional external dependency.
    try:
        run_checked(
            [*server_from, "devpi-init", "--serverdir", str(server_dir)],
            timeout=300,
        )
    except (RuntimeError, subprocess.TimeoutExpired, FileNotFoundError) as e:
        pytest.skip(f"devpi-server unavailable (cannot fetch/init): {e}")

    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [
                *server_from,
                "devpi-server",
                "--serverdir",
                str(server_dir),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_http_responding(f"{base}/", timeout=60.0)
            # Authenticated index: root creates a user with a password, the user
            # owns a standalone index (bases= — no PyPI fallthrough), so the
            # source holds exactly what we upload.
            devpi("use", base)
            devpi("login", "root", "--password", "")
            devpi("user", "-c", DEVPI_USER, f"password={DEVPI_PASS}", "email=migrate@example.com")
            devpi("login", DEVPI_USER, "--password", DEVPI_PASS)
            devpi("index", "-c", DEVPI_INDEX, "bases=", "volatile=True")
            devpi("use", f"{DEVPI_USER}/{DEVPI_INDEX}")
            yield {
                "base_url": base,
                # What `pypiron sync --from` points at: devpi's per-index PEP 503
                # listing lives at `<base>/<user>/<index>/+simple`.
                "source": f"{base}/{DEVPI_USER}/{DEVPI_INDEX}/+simple",
                # The legacy upload endpoint (twine speaks it), auth-gated.
                "upload_url": f"{base}/{DEVPI_USER}/{DEVPI_INDEX}/",
                "user": DEVPI_USER,
                "password": DEVPI_PASS,
                "port": port,
                "log_path": log_path,
                "proc": proc,
            }
        finally:
            kill_process_tree(proc)


@pytest.fixture()
def devpi_server(tmp_path_factory, uv_path: str) -> Iterator[Dict]:
    """A real devpi-server with an authenticated user index, as a migration
    *source*. Fetched via `uv tool run` (pinned); skips cleanly when devpi can't
    be fetched or started."""
    yield from _start_devpi_server(tmp_path_factory, uv_path)


@pytest.fixture()
def disk_server_prefixed(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server reserving the `acme` namespace for private uploads."""
    yield from _start_disk_server(
        tmp_path_factory, pypiron_bin, extra_args=["--private-prefix", "acme"]
    )


@pytest.fixture()
def disk_server_wait_on_upload(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server where uploads wait for index visibility before returning."""
    yield from _start_disk_server(tmp_path_factory, pypiron_bin, extra_args=["--wait-on-upload"])


@pytest.fixture()
def disk_server_fast_counters(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server flushing download counters every second (counter tests)."""
    yield from _start_disk_server(
        tmp_path_factory, pypiron_bin, extra_args=["--counters-flush-interval-secs", "1"]
    )


@pytest.fixture()
def disk_server_read_auth(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server requiring basic auth on index and artifact reads."""
    for server in _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=[
            "--read-user",
            "reader",
            "--read-pass",
            "readersecret",
            # This fixture's tests assert on per-project /metrics attribution.
            "--metrics-project-labels",
        ],
    ):
        server["read_user"] = "reader"
        server["read_password"] = "readersecret"
        yield server


@pytest.fixture()
def disk_server_token_auth(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Read-gated disk server that also signs short-lived install tokens."""
    for server in _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=[
            "--read-user",
            "reader",
            "--read-pass",
            "readersecret",
            "--token-signing-key",
            "test-signing-key-0123456789abcdef",
        ],
    ):
        server["read_user"] = "reader"
        server["read_password"] = "readersecret"
        yield server


@pytest.fixture()
def disk_server_admin_pass_only(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server given only `--admin-pass`: the username defaults to `admin`."""
    yield from _start_disk_server(tmp_path_factory, pypiron_bin, creds="admin-pass-only")


@pytest.fixture()
def disk_server_no_creds(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server with no credentials at all: read-only, every write disabled."""
    yield from _start_disk_server(tmp_path_factory, pypiron_bin, creds="none")


@pytest.fixture()
def disk_server_json_logs(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server logging one JSON object per line."""
    yield from _start_disk_server(
        tmp_path_factory, pypiron_bin, extra_args=["--log-format", "json"]
    )


@pytest.fixture()
def disk_server_access_log(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server with the structured (text) access log on — lets tests observe
    per-request client behavior in the server log."""
    yield from _start_disk_server(tmp_path_factory, pypiron_bin, extra_args=["--access-log"])


@pytest.fixture()
def disk_server_access_log_info(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Access log on at info level (debug OFF) — so /health and /metrics, which
    log only at debug, are excluded."""
    yield from _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=["--access-log"],
        extra_env={"RUST_LOG": "info,pypiron=info"},
    )


@pytest.fixture()
def disk_server_access_log_json(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server with the structured access log on, emitting JSON lines."""
    yield from _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=["--access-log", "--log-format", "json"],
    )


@pytest.fixture()
def disk_server_access_log_clf(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server with the access log on in Combined Log Format."""
    yield from _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=["--access-log", "--access-log-format", "clf"],
    )


# ------------------------------ Proxy fixtures --------------------------------


def _start_proxy_pair(
    tmp_path_factory, pypiron_bin: Path, proxy_extra_args=(), exclude_newer: str | None = ""
) -> Iterator[Dict]:
    """An upstream disk server plus a second server proxying it on demand.

    The proxy disables the default 7-day quarantine (`exclude_newer=""`) so these
    tests can publish a wheel upstream and proxy it immediately; pass
    `exclude_newer=None` to leave the production default in place and exercise the
    cooldown itself."""
    upstream_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    upstream = next(upstream_gen)
    cooldown = [] if exclude_newer is None else ["--exclude-newer", exclude_newer]
    proxy_gen = _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        # The upstream here is a loopback pypiron on plain http, which pypiron
        # refuses without this opt-in: over http a MITM controls both the bytes
        # and the sha256 they're checked against. No MITM on 127.0.0.1.
        extra_args=[
            "--proxy-upstream",
            upstream["base_url"],
            "--allow-insecure-upstream",
            *cooldown,
            *proxy_extra_args,
        ],
    )
    proxy = next(proxy_gen)
    try:
        yield {"upstream": upstream, "proxy": proxy}
    finally:
        proxy_gen.close()
        upstream_gen.close()


@pytest.fixture()
def proxy_pair(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    yield from _start_proxy_pair(tmp_path_factory, pypiron_bin)


@pytest.fixture()
def proxy_pair_fast_counters(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Proxy pair whose proxy flushes download counters every second."""
    yield from _start_proxy_pair(
        tmp_path_factory, pypiron_bin, proxy_extra_args=["--counters-flush-interval-secs", "1"]
    )


@pytest.fixture()
def proxy_pair_wheels_only(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    yield from _start_proxy_pair(
        tmp_path_factory, pypiron_bin, proxy_extra_args=["--include-format", "wheel"]
    )


@pytest.fixture()
def proxy_pair_prefixed(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Proxying server that reserves the `acme` namespace for private uploads."""
    yield from _start_proxy_pair(
        tmp_path_factory, pypiron_bin, proxy_extra_args=["--private-prefix", "acme"]
    )


@pytest.fixture()
def proxy_pair_scoped(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Proxy restricted to an approved-package allowlist: `allowed` (any
    version) and `pinned>=2.0` (version-scoped). Every other name is 404'd."""
    yield from _start_proxy_pair(
        tmp_path_factory,
        pypiron_bin,
        proxy_extra_args=["--include-package", "allowed", "--include-package", "pinned>=2.0"],
    )


@pytest.fixture()
def proxy_pair_denylist(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Open proxy with a whole-name deny and a version-pinned deny."""
    yield from _start_proxy_pair(
        tmp_path_factory,
        pypiron_bin,
        proxy_extra_args=["--exclude-package", "blocked", "--exclude-package", "pinned<2.0"],
    )


@pytest.fixture()
def proxy_pair_deny_wins(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Scoped proxy where the same name is both included and denied."""
    yield from _start_proxy_pair(
        tmp_path_factory,
        pypiron_bin,
        proxy_extra_args=["--include-package", "both", "--exclude-package", "both"],
    )


@pytest.fixture()
def disk_server_uploader_only(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server with only an uploader credential (no admin) — mirror,
    delete, and yank are disabled."""
    data_dir = tmp_path_factory.mktemp("pypiron-uploader-only")
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    log_path = data_dir.parent / f"{data_dir.name}-server.log"
    args = [
        str(pypiron_bin),
        "serve",
        "--bind-addr",
        bind,
        "--data-dir",
        str(data_dir),
        "--uploader-user",
        "uploader",
        "--uploader-pass",
        "uploadersecret",
        "--worker-interval-secs",
        "1",
    ]
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info,pypiron=debug")
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        try:
            wait_http_ok(f"http://{bind}/simple/index.json", timeout=20.0)
            yield {
                "bind": bind,
                "base_url": f"http://{bind}",
                "legacy": f"http://{bind}/legacy/",
                "simple": f"http://{bind}/simple/",
                "user": "uploader",
                "password": "uploadersecret",
                "data_dir": data_dir,
                "log_path": log_path,
                "proc": proc,
            }
        finally:
            kill_process_tree(proc)


# ------------------------------ MinIO (S3) fixtures ---------------------------


def _real_s3_config() -> Dict | None:
    """Real-S3 target from the environment, or None to fall back to MinIO.

    Set ``PYPIRON_TEST_S3_REAL_BUCKET`` to point the whole s3-marked suite at a
    real bucket instead of the Docker emulator. pypiron writes to the bucket
    root (there is no per-run key prefix to scope to), so the bucket must be
    **dedicated and disposable**: the fixture empties it before and after every
    test to give each a clean slate. Credentials come from the ambient AWS
    environment (env vars, shared profile, or instance role), resolved exactly
    the way ``object_store`` resolves them.
    """
    bucket = os.environ.get("PYPIRON_TEST_S3_REAL_BUCKET", "").strip()
    if not bucket:
        return None
    return {
        "endpoint": None,  # real AWS, not an emulator
        "bucket": bucket,
        "access_key": None,  # ambient credentials
        "secret_key": None,
        "region": os.environ.get("AWS_REGION", "").strip() or "us-east-1",
        "real": True,
    }


def _s3_empty_bucket(bucket: str) -> None:
    """Delete every object in a real S3 bucket, using the aws CLI with ambient
    credentials. Skips the run if the CLI is unavailable."""
    if not cmd_exists("aws"):
        pytest.skip("aws CLI is required to manage the real S3 test bucket; not found on PATH")
    run_checked(["aws", "s3", "rm", f"s3://{bucket}/", "--recursive"])


def _docker_published_port(name: str, container_port: str) -> int:
    """The host port the daemon published for a container, polled briefly:
    under parallel session startup a just-started container can take a moment
    to report its mapping (and a one-shot read gave IndexError on a daemon
    hiccup, failing every test on that worker)."""
    deadline = time.time() + 30.0
    while True:
        rc, out, _ = run_returncode(["docker", "port", name, container_port])
        first = out.strip().splitlines()[0] if out.strip() else ""
        if rc == 0 and ":" in first:
            return int(first.rsplit(":", 1)[1])
        if time.time() >= deadline:
            _, state, _ = run_returncode(
                ["docker", "inspect", "-f", "{{.State.Status}} {{.State.Error}}", name]
            )
            raise RuntimeError(
                f"container {name} published no port for {container_port} "
                f"(state: {state.strip() or 'unknown'})"
            )
        time.sleep(0.5)


@pytest.fixture(scope="session")
def _minio_container() -> Iterator[Dict]:
    """One MinIO container per session (per xdist worker, under -n).

    Containers are the expensive part — a docker run + health poll + docker rm
    per test is what tripled the CI s3 leg — while buckets are free, so tests
    isolate by creating their own uuid-named buckets on this shared container
    (see _minio_buckets). The docker daemon assigns the host port: publishing a
    pre-picked "free" port is a bind race under xdist."""
    if not cmd_exists("docker"):
        pytest.skip("docker is required for S3/MinIO integration tests; not found on PATH")

    name = f"pypiron-minio-{uuid.uuid4().hex[:12]}"
    try:
        run_checked(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "-p",
                "127.0.0.1::9000",
                "-e",
                "MINIO_ROOT_USER=minioadmin",
                "-e",
                "MINIO_ROOT_PASSWORD=minioadmin",
                "minio/minio",
                "server",
                "/data",
            ]
        )
        s3_port = _docker_published_port(name, "9000/tcp")
        wait_http_ok(f"http://127.0.0.1:{s3_port}/minio/health/ready", timeout=60.0)
        yield {
            "endpoint": f"http://127.0.0.1:{s3_port}",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
        }
    finally:
        run_returncode(["docker", "rm", "-f", name])


def _minio_buckets(container: Dict, count: int, label: str) -> Iterator[Dict]:
    """Provision fresh uuid-named buckets on the session container, drop them
    afterwards (tests push real payloads — 72 MiB uploads — that must not pile
    up in the shared container). A single-bucket config carries only `bucket`
    so `_s3_env` keeps it in ordinary single-bucket mode; multi-bucket configs
    add the `buckets` list that switches it to the PYPIRON_BUCKETS topology."""
    names = [f"pypiron-{label}-{uuid.uuid4().hex[:8]}" for _ in range(count)]
    cfg = dict(container)
    cfg["bucket"] = names[0]
    if count > 1:
        cfg["buckets"] = names
    for name in names:
        minio_make_bucket(cfg, name)
    try:
        yield cfg
    finally:
        # Best-effort: a test may have deleted its bucket on purpose (outage
        # simulation), and teardown must not mask the test's own outcome.
        for name in names:
            try:
                minio_remove_bucket(cfg, name)
            except (AssertionError, OSError):
                pass


@pytest.fixture()
def minio(request) -> Iterator[Dict]:
    """S3 backend for the s3-marked suite.

    Default: a fresh uuid-named bucket on the session MinIO container (skips
    without Docker). If ``PYPIRON_TEST_S3_REAL_BUCKET`` is set, targets that
    real S3 bucket instead (see ``_real_s3_config``), emptying it around each
    test so every test still starts from an empty bucket. Real-bucket runs
    must be serial — the shared bucket is wiped per test, so `-n`/xdist would
    corrupt concurrent tests; `make test-s3-real` passes `-n 0`."""
    real = _real_s3_config()
    if real is not None:
        if os.environ.get("PYTEST_XDIST_WORKER"):
            pytest.fail(
                "PYPIRON_TEST_S3_REAL_BUCKET wipes a shared bucket around every "
                "test; run serially (make test-s3-real, or pass -n 0)"
            )
        _s3_empty_bucket(real["bucket"])
        try:
            yield real
        finally:
            _s3_empty_bucket(real["bucket"])
        return

    # Lazy so the real-bucket branch above never demands Docker.
    yield from _minio_buckets(request.getfixturevalue("_minio_container"), 1, "s3")


@pytest.fixture()
def minio_two(_minio_container: Dict) -> Iterator[Dict]:
    """Two buckets on the session MinIO container for replication and selection."""
    yield from _minio_buckets(_minio_container, 2, "m2")


@pytest.fixture()
def minio_three(_minio_container: Dict) -> Iterator[Dict]:
    """Three buckets on the session MinIO container for ordered fallback tests."""
    yield from _minio_buckets(_minio_container, 3, "m3")


@pytest.fixture(scope="session")
def _minio_container_alt() -> Iterator[Dict]:
    """A second MinIO container with DIFFERENT root credentials, for the
    per-bucket-credentials tests: a bucket here is reachable only with these
    keys, so the ambient AWS_* creds (which belong to the primary container)
    cannot sign for it. Narrowly scoped — only test_bucket_overrides.py requests
    it — so only that file pays for the extra container."""
    if not cmd_exists("docker"):
        pytest.skip("docker is required for S3/MinIO integration tests; not found on PATH")

    name = f"pypiron-minio-alt-{uuid.uuid4().hex[:12]}"
    try:
        run_checked(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "-p",
                "127.0.0.1::9000",
                "-e",
                "MINIO_ROOT_USER=altadmin",
                "-e",
                "MINIO_ROOT_PASSWORD=altsecret123",
                "minio/minio",
                "server",
                "/data",
            ]
        )
        s3_port = _docker_published_port(name, "9000/tcp")
        wait_http_ok(f"http://127.0.0.1:{s3_port}/minio/health/ready", timeout=60.0)
        yield {
            "endpoint": f"http://127.0.0.1:{s3_port}",
            "access_key": "altadmin",
            "secret_key": "altsecret123",
        }
    finally:
        run_returncode(["docker", "rm", "-f", name])


@pytest.fixture()
def minio_alt(_minio_container_alt: Dict) -> Iterator[Dict]:
    """One fresh uuid-named bucket on the second (different-credentials) MinIO
    container."""
    yield from _minio_buckets(_minio_container_alt, 1, "alt")


class BucketFaults:
    """Thread-safe bucket-specific availability faults for the S3 proxy."""

    def __init__(self) -> None:
        self._failed: set[str] = set()
        self._blackholed: set[str] = set()
        self._dripped: dict[tuple[str, str], float] = {}
        self._dripped_puts: dict[tuple[str, str], float] = {}
        self._requests: list[tuple[str, str, str]] = []
        self._lock = threading.Lock()
        #: Per-fixture request log, set by ``_minio_fault_proxy``. On a test
        #: failure the diagnostics hook tails it to show exactly what the proxy
        #: saw and forwarded for each S3 call.
        self.log_path: Optional[Path] = None

    def fail(self, *buckets: str) -> None:
        with self._lock:
            self._failed.update(buckets)

    def recover(self, *buckets: str) -> None:
        with self._lock:
            self._failed.difference_update(buckets)
            self._blackholed.difference_update(buckets)

    def blackhole(self, *buckets: str) -> None:
        """Accept requests but withhold a response long enough to test timeouts."""
        with self._lock:
            self._blackholed.update(buckets)

    def drip(self, bucket: str, key: str, *, duration: float = 11.0) -> None:
        """Send one object's GET body steadily over ``duration`` seconds."""
        with self._lock:
            self._dripped[(bucket, key)] = duration

    def drip_put(self, bucket: str, key: str, *, duration: float = 11.0) -> None:
        """Read one object's PUT body steadily over ``duration`` seconds."""
        with self._lock:
            self._dripped_puts[(bucket, key)] = duration

    def is_failed(self, bucket: str) -> bool:
        with self._lock:
            return bucket in self._failed

    def is_blackholed(self, bucket: str) -> bool:
        with self._lock:
            return bucket in self._blackholed

    def drip_duration(self, bucket: str, key: str) -> float | None:
        with self._lock:
            return self._dripped.get((bucket, key))

    def put_drip_duration(self, bucket: str, key: str) -> float | None:
        with self._lock:
            return self._dripped_puts.get((bucket, key))

    def record(self, bucket: str, method: str, target: str) -> None:
        with self._lock:
            self._requests.append((bucket, method, unquote(target)))

    def count(
        self, *, bucket: str | None = None, method: str | None = None, needle: str = ""
    ) -> int:
        with self._lock:
            return sum(
                1
                for seen_bucket, seen_method, target in self._requests
                if (bucket is None or bucket == seen_bucket)
                and (method is None or method == seen_method)
                and needle in target
            )

    def requests(self) -> list[tuple[str, str, str]]:
        """Return an ordered snapshot for write-protocol assertions."""
        with self._lock:
            return list(self._requests)


class _S3FaultProxyServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, origin_host: str, origin_port: int, faults: BucketFaults):
        super().__init__(address, _S3FaultProxyHandler)
        self.origin_host = origin_host
        self.origin_port = origin_port
        self.faults = faults
        self.log_path: Optional[Path] = None
        self._log_lock = threading.Lock()

    def log_request_line(self, line: str) -> None:
        """Append one timestamped request line to the per-fixture proxy log."""
        if self.log_path is None:
            return
        stamp = f"{time.strftime('%H:%M:%S')}.{int(time.time() * 1000) % 1000:03d}"
        with self._log_lock:
            with open(self.log_path, "a") as handle:
                handle.write(f"{stamp} {line}\n")


class _S3FaultProxyHandler(BaseHTTPRequestHandler):
    """Forward signed S3 requests byte-for-byte, preserving their Host header."""

    protocol_version = "HTTP/1.1"
    # Cap a single accepted connection: a wedged read (the CI zero-byte trigger)
    # must not pin a proxy thread forever. StreamRequestHandler.setup() applies
    # this to the socket, so a stalled client trips it and the thread is freed.
    timeout = 60

    def do_DELETE(self) -> None:
        self._proxy()

    def do_GET(self) -> None:
        self._proxy()

    def do_HEAD(self) -> None:
        self._proxy()

    def do_POST(self) -> None:
        self._proxy()

    def do_PUT(self) -> None:
        self._proxy()

    def log_message(self, _format: str, *_args) -> None:
        pass

    def _log(self, bucket: str, received: str, forwarded: int, upstream: str) -> None:
        self.server.log_request_line(
            f"{self.command} bucket={bucket or '-'} path={self.path} "
            f"clen={received} forwarded={forwarded}B upstream={upstream}"
        )

    def _proxy(self) -> None:
        path = urlsplit(self.path).path
        bucket, _, key = path.lstrip("/").partition("/")
        key = unquote(key)
        self.server.faults.record(bucket, self.command, self.path)
        received = self.headers.get("Content-Length", "-")
        if bucket and self.server.faults.is_failed(bucket):
            self._log(bucket, received, 0, "503-injected-outage")
            self._send_error(503, b"injected bucket outage")
            return
        if bucket and self.server.faults.is_blackholed(bucket):
            # Health-only calls are cancelled by pypiron after one second. Keep
            # this connection silent much longer so the test distinguishes that
            # bound from an immediate synthetic 503.
            time.sleep(15)
            self._log(bucket, received, 0, "503-injected-blackhole")
            try:
                self._send_error(503, b"injected bucket blackhole")
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        try:
            put_drip_duration = (
                self.server.faults.put_drip_duration(bucket, key) if self.command == "PUT" else None
            )
            body = self._request_body(drip_duration=put_drip_duration)
        except (EOFError, OSError, ValueError):
            self._log(bucket, received, 0, "400-invalid-body")
            self._send_error(400, b"invalid request body")
            return

        connection = http.client.HTTPConnection(
            self.server.origin_host,
            self.server.origin_port,
            timeout=30,
        )
        try:
            connection.putrequest(
                self.command,
                self.path,
                skip_host=True,
                skip_accept_encoding=True,
            )
            for name, value in self.headers.items():
                connection.putheader(name, value)
            connection.endheaders()
            if body:
                connection.send(body)
            response = connection.getresponse()
            response_body = response.read()
            self._log(bucket, received, len(body), str(response.status))
            drip_duration = (
                self.server.faults.drip_duration(bucket, key) if self.command == "GET" else None
            )
            self._send_upstream_response(response, response_body, drip_duration=drip_duration)
        except (ConnectionError, OSError, http.client.HTTPException) as exc:
            self._log(bucket, received, len(body), f"502-{type(exc).__name__}")
            self._send_error(502, b"MinIO proxy upstream unavailable")
        finally:
            connection.close()

    def _request_body(self, *, drip_duration: float | None = None) -> bytes:
        transfer_encoding = self.headers.get("Transfer-Encoding", "").lower()
        if "chunked" in transfer_encoding:
            return self._chunked_body()
        length = int(self.headers.get("Content-Length", "0"))
        if drip_duration is None or length < 2:
            body = self.rfile.read(length)
        else:
            body = self._drip_request_body(length, drip_duration)
        if len(body) != length:
            raise EOFError("short request body")
        return body

    def _drip_request_body(self, length: int, duration: float) -> bytes:
        """Read a fixed-length request in small, regularly paced chunks."""
        body = bytearray()
        started = time.monotonic()
        chunk_size = 64 * 1024
        while len(body) < length:
            if body:
                target = started + duration * len(body) / length
                time.sleep(max(0.0, target - time.monotonic()))
            expected = min(chunk_size, length - len(body))
            chunk = self.rfile.read(expected)
            if len(chunk) != expected:
                raise EOFError("short request body")
            body.extend(chunk)
        return bytes(body)

    def _chunked_body(self) -> bytes:
        """Read chunk framing intact; SigV4 streaming signatures live in it."""
        body = bytearray()
        while True:
            line = self.rfile.readline(65537)
            if not line or len(line) > 65536:
                raise EOFError("missing chunk header")
            body.extend(line)
            size = int(line.split(b";", 1)[0].strip(), 16)
            if size == 0:
                while True:
                    trailer = self.rfile.readline(65537)
                    if not trailer or len(trailer) > 65536:
                        raise EOFError("missing chunk trailer")
                    body.extend(trailer)
                    if trailer in {b"\r\n", b"\n"}:
                        return bytes(body)
            chunk = self.rfile.read(size + 2)
            if len(chunk) != size + 2:
                raise EOFError("short chunk")
            body.extend(chunk)

    def _send_upstream_response(
        self, response, body: bytes, *, drip_duration: float | None = None
    ) -> None:
        hop_by_hop = {
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade",
        }
        self.send_response(response.status, response.reason)
        has_head_length = False
        for name, value in response.getheaders():
            lower = name.lower()
            if lower in hop_by_hop:
                continue
            if lower == "content-length":
                if self.command == "HEAD":
                    has_head_length = True
                else:
                    continue
            self.send_header(name, value)
        if self.command == "HEAD":
            if not has_head_length:
                self.send_header("Content-Length", "0")
        else:
            self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            if drip_duration is None or len(body) < 2:
                self.wfile.write(body)
            else:
                chunk_count = min(12, len(body))
                pause = drip_duration / (chunk_count - 1)
                for index in range(chunk_count):
                    start = index * len(body) // chunk_count
                    end = (index + 1) * len(body) // chunk_count
                    self.wfile.write(body[start:end])
                    self.wfile.flush()
                    if index + 1 < chunk_count:
                        time.sleep(pause)
        self.close_connection = True

    def _send_error(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
        self.close_connection = True


def _minio_fault_proxy(minio: Dict) -> Iterator[Dict]:
    origin = urlsplit(minio["endpoint"])
    if origin.scheme != "http" or origin.hostname is None or origin.port is None:
        raise ValueError("fault proxy requires an explicit HTTP MinIO endpoint")
    faults = BucketFaults()
    server = _S3FaultProxyServer(
        ("127.0.0.1", 0),
        origin.hostname,
        origin.port,
        faults,
    )
    log_path = Path(tempfile.mkdtemp(prefix="s3-fault-proxy-")) / "requests.log"
    server.log_path = log_path
    faults.log_path = log_path
    thread = threading.Thread(target=server.serve_forever, name="s3-fault-proxy", daemon=True)
    thread.start()
    proxied = dict(minio)
    proxied["server_endpoint"] = f"http://127.0.0.1:{server.server_port}"
    proxied["faults"] = faults
    proxied["proxy_log_path"] = log_path
    try:
        yield proxied
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture()
def minio_two_proxy(minio_two: Dict) -> Iterator[Dict]:
    yield from _minio_fault_proxy(minio_two)


@pytest.fixture()
def minio_three_proxy(minio_three: Dict) -> Iterator[Dict]:
    yield from _minio_fault_proxy(minio_three)


@pytest.fixture()
def minio_two_dual_proxy(minio_two: Dict) -> Iterator[Dict]:
    """One MinIO reached through two independently faultable S3 paths."""
    left_gen = _minio_fault_proxy(minio_two)
    left = next(left_gen)
    right_gen = _minio_fault_proxy(minio_two)
    try:
        right = next(right_gen)
        yield {"minio": minio_two, "left": left, "right": right}
    finally:
        right_gen.close()
        left_gen.close()


@pytest.fixture()
def s3_server_multi(tmp_path_factory, pypiron_bin: Path, minio_two: Dict) -> Iterator[Dict]:
    """pypiron configured against two MinIO buckets. Short reconcile and repl-
    sweep intervals keep the tier-3 diff backstop and the `_repl/` note sweep
    running every few seconds so tests never wait out the production defaults."""
    yield from _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two,
        extra_env={
            "PYPIRON_RECONCILE_INTERVAL_SECS": "3",
            "PYPIRON_REPL_SWEEP_INTERVAL_SECS": "2",
        },
    )


@pytest.fixture()
def s3_server_multi_failover(
    tmp_path_factory, pypiron_bin: Path, minio_two_proxy: Dict
) -> Iterator[Dict]:
    """Two buckets behind a bucket-aware 503 proxy with short hysteresis."""
    yield from _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_proxy,
        extra_env={"PYPIRON_AUDIT_ON_BOOT": "false"},
        extra_args=[
            "--bucket-leave-failures",
            "1",
            "--bucket-return-healthy-secs",
            "4",
        ],
    )


@pytest.fixture()
def s3_server_multi_default_failover(
    tmp_path_factory, pypiron_bin: Path, minio_two_proxy: Dict
) -> Iterator[Dict]:
    """Two buckets using the shipped leave/return defaults."""
    yield from _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_proxy,
        extra_env={"PYPIRON_AUDIT_ON_BOOT": "false"},
    )


@pytest.fixture()
def s3_server_multi_cadence(
    tmp_path_factory, pypiron_bin: Path, minio_two_proxy: Dict
) -> Iterator[Dict]:
    """Long periodic cadence used to prove write nudges do not multiply scans."""
    yield from _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_proxy,
        extra_env={
            "PYPIRON_AUDIT_ON_BOOT": "false",
            "PYPIRON_WORKER_INTERVAL_SECS": "60",
        },
    )


@pytest.fixture()
def s3_server_multi_reconcile_cost(
    tmp_path_factory, pypiron_bin: Path, minio_two_proxy: Dict
) -> Iterator[Dict]:
    """Short full-diff cadence behind the request-counting S3 proxy."""
    yield from _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_proxy,
        extra_env={
            "PYPIRON_AUDIT_ON_BOOT": "false",
            "PYPIRON_RECONCILE_INTERVAL_SECS": "2",
        },
    )


@pytest.fixture()
def s3_server_three_failover(
    tmp_path_factory, pypiron_bin: Path, minio_three_proxy: Dict
) -> Iterator[Dict]:
    """Three buckets behind the same path-aware availability proxy."""
    yield from _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio_three_proxy,
        extra_env={
            "PYPIRON_AUDIT_ON_BOOT": "false",
            "PYPIRON_REPL_SWEEP_INTERVAL_SECS": "2",
        },
        extra_args=[
            "--bucket-leave-failures",
            "1",
            "--bucket-return-healthy-secs",
            "4",
        ],
    )


def _start_s3_server_pair(
    tmp_path_factory,
    pypiron_bin: Path,
    minio_two_dual_proxy: Dict,
    *,
    extra_args=(),
) -> Iterator[Dict]:
    """Two nodes sharing a topology through independent availability views."""
    common_args = [
        "--bucket-leave-failures",
        "1",
        "--bucket-return-healthy-secs",
        "2",
        *extra_args,
    ]
    common_env = {
        "PYPIRON_AUDIT_ON_BOOT": "false",
        "PYPIRON_RECONCILE_INTERVAL_SECS": "3",
        "PYPIRON_REPL_SWEEP_INTERVAL_SECS": "2",
    }
    left_gen = _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_dual_proxy["left"],
        extra_env=common_env,
        extra_args=common_args,
    )
    left = next(left_gen)
    right_gen = _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_dual_proxy["right"],
        extra_env=common_env,
        extra_args=common_args,
    )
    try:
        right = next(right_gen)
        yield {
            "left": left,
            "right": right,
            "minio": minio_two_dual_proxy["minio"],
        }
    finally:
        right_gen.close()
        left_gen.close()


@pytest.fixture()
def s3_servers_multi(
    tmp_path_factory, pypiron_bin: Path, minio_two_dual_proxy: Dict
) -> Iterator[Dict]:
    yield from _start_s3_server_pair(
        tmp_path_factory,
        pypiron_bin,
        minio_two_dual_proxy,
    )


@pytest.fixture()
def s3_servers_multi_short_lease(
    tmp_path_factory, pypiron_bin: Path, minio_two_dual_proxy: Dict
) -> Iterator[Dict]:
    """Two nodes with short bucket-local leases for asymmetric-path proofs."""
    yield from _start_s3_server_pair(
        tmp_path_factory,
        pypiron_bin,
        minio_two_dual_proxy,
        extra_args=("--lease-ttl-secs", "3"),
    )


@pytest.fixture()
def s3_servers_multi_proxy(
    tmp_path_factory, pypiron_bin: Path, minio_two_dual_proxy: Dict
) -> Iterator[Dict]:
    """Two independently partitionable S3 nodes proxying one real upstream."""
    upstream_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    upstream = next(upstream_gen)
    pair_gen = _start_s3_server_pair(
        tmp_path_factory,
        pypiron_bin,
        minio_two_dual_proxy,
        extra_args=(
            "--proxy-upstream",
            upstream["base_url"],
            "--allow-insecure-upstream",
            "--exclude-newer",
            "",
        ),
    )
    try:
        pair = next(pair_gen)
        yield {**pair, "upstream": upstream}
    finally:
        pair_gen.close()
        upstream_gen.close()


@pytest.fixture()
def s3_server_multi_proxy_prefixed(
    tmp_path_factory, pypiron_bin: Path, minio_two: Dict
) -> Iterator[Dict]:
    """A multi-bucket proxy with an upstream-collision-proof private prefix."""
    upstream_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    upstream = next(upstream_gen)
    server_gen = _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two,
        extra_env={
            "PYPIRON_AUDIT_ON_BOOT": "false",
            "PYPIRON_RECONCILE_INTERVAL_SECS": "3",
        },
        extra_args=[
            "--proxy-upstream",
            upstream["base_url"],
            "--allow-insecure-upstream",
            "--exclude-newer",
            "",
            "--private-prefix",
            "acme",
        ],
    )
    try:
        server = next(server_gen)
        yield {**server, "upstream": upstream}
    finally:
        server_gen.close()
        upstream_gen.close()


# ----------------------- Read-affinity fixtures (read locality) ----------------
#
# One node over two region-labeled buckets: writes home to the first bucket
# (config order), reads pin to the node's own region bucket (the second). A
# single bucket-aware fault proxy sees every S3 request, so a test asserts WHICH
# bucket served a read from the ordered request log, and fails/recovers each
# bucket independently. See dev/READ_AFFINITY_VISION.md and
# tests/test_read_affinity.py. Additive: these reuse the existing multi-bucket
# MinIO + fault-proxy fixtures without touching them.

#: Region label on the write-home bucket (index 0) — never the node's region.
READ_AFFINITY_WRITE_REGION = "left"
#: Region label on the read-home bucket (index 1) — the node declares this region.
READ_AFFINITY_NODE_REGION = "right"


def read_affinity_buckets_uri(minio: Dict) -> str:
    """`PYPIRON_BUCKETS` for the two-bucket topology with `@region` labels: the
    first bucket is the write home (`@left`), the second the node's region
    (`@right`). The `@region` never changes bucket identity, so the fleet stamp
    is the same as the unlabeled list — only read affinity reads the labels."""
    a, b = minio["buckets"][:2]
    return f"s3://{a}@{READ_AFFINITY_WRITE_REGION},s3://{b}@{READ_AFFINITY_NODE_REGION}"


def _start_read_affinity_server(
    tmp_path_factory,
    pypiron_bin: Path,
    minio: Dict,
    *,
    node_region: str | None,
    leave_failures: int,
    return_healthy_secs: int,
    repl_sweep_secs: int = 2,
    reconcile_secs: int = 3,
    extra_env=None,
    extra_args=None,
) -> Iterator[Dict]:
    """One S3 node over the region-labeled two-bucket topology. `node_region`
    declares the node's region via the operator override (an arbitrary label that
    skips the cloud-metadata probe by design); `None` leaves the node
    region-agnostic so reads follow the write pin. Streaming delivery keeps the
    served bytes flowing through the node so the fault proxy records which bucket
    answered."""
    env = {
        "PYPIRON_AUDIT_ON_BOOT": "false",
        "PYPIRON_BUCKETS": read_affinity_buckets_uri(minio),
        "PYPIRON_ARTIFACT_DELIVERY": "stream",
        "PYPIRON_REPL_SWEEP_INTERVAL_SECS": str(repl_sweep_secs),
        "PYPIRON_RECONCILE_INTERVAL_SECS": str(reconcile_secs),
    }
    if node_region is not None:
        env["PYPIRON_NODE_REGION"] = node_region
    if extra_env:
        env.update(extra_env)
    args = [
        "--bucket-leave-failures",
        str(leave_failures),
        "--bucket-return-healthy-secs",
        str(return_healthy_secs),
        *(extra_args or []),
    ]
    yield from _start_s3_server(
        tmp_path_factory, pypiron_bin, minio, extra_env=env, extra_args=args
    )


@pytest.fixture()
def s3_server_read_affinity(
    tmp_path_factory, pypiron_bin: Path, minio_two_proxy: Dict
) -> Iterator[Dict]:
    """Reads pinned to the region bucket (B); writes home to A. Low leave
    threshold and short return window so a sustained outage moves the read pin in
    seconds and a recovery returns it under the drain gate."""
    yield from _start_read_affinity_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_proxy,
        node_region=READ_AFFINITY_NODE_REGION,
        leave_failures=1,
        return_healthy_secs=2,
    )


@pytest.fixture()
def s3_server_read_affinity_sticky(
    tmp_path_factory, pypiron_bin: Path, minio_two_proxy: Dict
) -> Iterator[Dict]:
    """Same topology with a high leave threshold: a brief bucket blip during a
    publish drops a `_repl/` note without ever moving the read pin off B, so the
    absence-read-through and lagging-tombstone windows are exercised on the
    region bucket itself. The `_repl/` sweep stays fast so B still converges."""
    yield from _start_read_affinity_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_proxy,
        node_region=READ_AFFINITY_NODE_REGION,
        leave_failures=8,
        return_healthy_secs=2,
    )


@pytest.fixture()
def s3_server_read_affinity_no_region(
    tmp_path_factory, pypiron_bin: Path, minio_two_proxy: Dict
) -> Iterator[Dict]:
    """The fail-safe default: same labeled buckets, but the node's region
    matches no configured bucket (it declares none, and the ambient AWS_REGION it
    detects is not a bucket label), so its read pin equals its write pin (A).
    Reads never touch B."""
    yield from _start_read_affinity_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_proxy,
        node_region=None,
        leave_failures=1,
        return_healthy_secs=2,
    )


@pytest.fixture()
def s3_server_read_affinity_proxy(
    tmp_path_factory, pypiron_bin: Path, minio_two_proxy: Dict
) -> Iterator[Dict]:
    """Read affinity plus a real proxy upstream. High leave threshold keeps reads
    on B through a publish-time blip; long repair cadences keep B divergent so a
    private name missing on B is served from the write home under a stable window
    and the upstream is provably never consulted. The upstream is a disk node
    with its access log on so a test can assert it saw no request for the name."""
    upstream_gen = _start_disk_server(
        tmp_path_factory, pypiron_bin, extra_env={"PYPIRON_ACCESS_LOG": "true"}
    )
    upstream = next(upstream_gen)
    server_gen = _start_read_affinity_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_proxy,
        node_region=READ_AFFINITY_NODE_REGION,
        leave_failures=8,
        return_healthy_secs=2,
        repl_sweep_secs=3600,
        reconcile_secs=3600,
        extra_args=[
            "--proxy-upstream",
            upstream["base_url"],
            "--allow-insecure-upstream",
            "--exclude-newer",
            "",
        ],
    )
    try:
        server = next(server_gen)
        yield {**server, "upstream": upstream}
    finally:
        server_gen.close()
        upstream_gen.close()


def s3_buckets_uri(*names: str) -> str:
    """The PYPIRON_BUCKETS value for an ordered list of MinIO bucket names: each
    becomes an `s3://name` URI (region rides on the shared AWS_REGION knob)."""
    return ",".join(f"s3://{name}" for name in names)


def _s3_env(minio: Dict, bind: str) -> Dict[str, str]:
    # Both single- and multi-bucket fixtures go through the one `--buckets` knob:
    # a `buckets` list becomes the multi-bucket topology, a single `bucket` a
    # one-entry `s3://name` list (ordinary single-bucket mode). Region rides on
    # the ambient AWS_REGION knob.
    env = os.environ.copy()
    names = minio.get("buckets") or [minio["bucket"]]
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(*names)
    env.update(
        {
            "AWS_REGION": minio.get("region") or "us-east-1",
            "PYPIRON_BIND_ADDR": bind,
            "PYPIRON_WORKER_INTERVAL_SECS": "1",
            "PYPIRON_ADMIN_USER": "admin",
            "PYPIRON_ADMIN_PASS": "secret",
            "PYPIRON_UPLOADER_USER": "uploader",
            "PYPIRON_UPLOADER_PASS": "uploadersecret",
            "RUST_LOG": "info,pypiron=debug",
        }
    )
    endpoint = minio.get("server_endpoint", minio.get("endpoint"))
    if endpoint:
        # Emulator (MinIO): explicit endpoint, path-style addressing, fixed
        # dev credentials. Real S3 has none of these — it relies on the ambient
        # AWS credentials already carried over by os.environ.copy() above.
        env["PYPIRON_S3_ENDPOINT_URL"] = endpoint
        env["PYPIRON_S3_FORCE_PATH_STYLE"] = "true"
        env["AWS_ACCESS_KEY_ID"] = minio["access_key"]
        env["AWS_SECRET_ACCESS_KEY"] = minio["secret_key"]
    return env


def _start_s3_server(
    tmp_path_factory,
    pypiron_bin: Path,
    minio: Dict,
    extra_env=None,
    extra_args=None,
) -> Iterator[Dict]:
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    log_path = tmp_path_factory.mktemp("pypiron-s3") / "server.log"
    env = _s3_env(minio, bind)
    if extra_env:
        env.update(extra_env)

    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [str(pypiron_bin), "serve", *(extra_args or [])],
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_http_ok(f"http://{bind}/simple/index.json", timeout=30.0)
            yield {
                "bind": bind,
                "base_url": f"http://{bind}",
                "legacy": f"http://{bind}/legacy/",
                "simple": f"http://{bind}/simple/",
                "user": "admin",
                "password": "secret",
                "minio": minio,
                "faults": minio.get("faults"),
                "log_path": log_path,
                "proc": proc,
            }
        finally:
            kill_process_tree(proc)


@pytest.fixture()
def s3_server(tmp_path_factory, pypiron_bin: Path, minio: Dict) -> Iterator[Dict]:
    """pypiron configured against the MinIO S3 backend."""
    yield from _start_s3_server(tmp_path_factory, pypiron_bin, minio)


#: Key prefix the `s3_server_prefixed` fixture roots pypiron under; the bucket
#: also holds foreign keys outside it, which pypiron must leave alone.
STORAGE_PREFIX = "tenant-a"


@pytest.fixture()
def s3_server_prefixed(tmp_path_factory, pypiron_bin: Path, minio: Dict) -> Iterator[Dict]:
    """S3-backed server rooted under a key prefix, sharing the bucket."""
    yield from _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        extra_env={"PYPIRON_STORAGE_PREFIX": STORAGE_PREFIX},
    )


@pytest.fixture()
def s3_server_prefixed_presigned(
    tmp_path_factory, pypiron_bin: Path, minio: Dict
) -> Iterator[Dict]:
    """Prefixed S3 server that redirects downloads to presigned URLs — the URL
    has to be signed for the prefixed key, or the redirect 404s at the store."""
    yield from _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        extra_env={
            "PYPIRON_STORAGE_PREFIX": STORAGE_PREFIX,
            "PYPIRON_ARTIFACT_DELIVERY": "redirect",
        },
    )


# ------------------------- MinIO object inspection ----------------------------
#
# SigV4-signed requests straight against the MinIO emulator, so tests can check
# bucket contents without going through pypiron — ground truth spoken in the
# real S3 wire protocol, stdlib only, mirroring the Azurite helpers below.
# In-process HTTP rather than a `docker run minio/mc` per probe: the oracle is
# polled inside `_eventually` loops, and a container per poll starves both the
# polls and the product's own convergence once tests run in parallel.


def _s3_signed(
    minio: Dict,
    method: str,
    path: str,
    query: Optional[Dict[str, str]] = None,
    data: bytes = b"",
) -> Tuple[int, bytes]:
    """One SigV4-signed request to the MinIO emulator. `path` starts at the
    bucket (`/bucket/key`); returns (status, body) — callers decide which
    statuses are acceptable."""
    if not minio.get("endpoint"):
        pytest.skip("bucket inspection targets the MinIO emulator, not a real S3 bucket")
    host = minio["endpoint"].split("://", 1)[1]
    uri = quote(path, safe="/")
    encoded_query = "&".join(
        f"{quote(k, safe='')}={quote(str(v), safe='')}" for k, v in sorted((query or {}).items())
    )
    amz_date = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    payload_hash = hashlib.sha256(data).hexdigest()
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join(
        [
            method,
            uri,
            encoded_query,
            f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n",
            signed_headers,
            payload_hash,
        ]
    )
    scope = f"{amz_date[:8]}/us-east-1/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    key = f"AWS4{minio['secret_key']}".encode()
    for step in scope.split("/"):
        key = hmac.new(key, step.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    url = f"{minio['endpoint']}{uri}"
    if encoded_query:
        url += f"?{encoded_query}"
    code, body, _ = _http_request(
        url,
        method=method,
        headers={
            "x-amz-date": amz_date,
            "x-amz-content-sha256": payload_hash,
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={minio['access_key']}/{scope}, "
                f"SignedHeaders={signed_headers}, Signature={signature}"
            ),
        },
        data=data or None,
    )
    return code, body


def _s3_list(minio: Dict, bucket: str, prefix: str = "") -> List[str]:
    """Every key under `prefix` in a bucket, following ListObjectsV2 pagination.
    Raises on a missing bucket — `_eventually` treats that as "not yet"."""
    keys: List[str] = []
    token = None
    while True:
        query = {"list-type": "2", "prefix": prefix}
        if token:
            query["continuation-token"] = token
        code, body = _s3_signed(minio, "GET", f"/{bucket}", query=query)
        assert code == 200, f"list {bucket} -> {code}: {body[:200]!r}"
        text = body.decode()
        keys.extend(re.findall(r"<Key>(.*?)</Key>", text))
        match = re.search(r"<NextContinuationToken>(.*?)</NextContinuationToken>", text)
        if not match:
            return keys
        token = match.group(1)


def minio_put_key(minio: Dict, key: str, body: str) -> None:
    """Write a foreign object straight into the bucket, bypassing pypiron."""
    minio_put_key_in(minio, minio["bucket"], key, body)


def minio_get_key(minio: Dict, key: str) -> str:
    """Read an object's body straight from the bucket, bypassing pypiron."""
    return minio_get_key_in(minio, minio["bucket"], key)


def minio_list_keys(minio: Dict) -> List[str]:
    """Every object key in the bucket, as pypiron-independent ground truth."""
    return minio_list_keys_in(minio, minio["bucket"])


def minio_list_keys_in(minio: Dict, bucket: str) -> List[str]:
    """Every object key in a named bucket (for the multi-bucket fixture)."""
    return sorted(_s3_list(minio, bucket))


def minio_get_key_in(minio: Dict, bucket: str, key: str) -> str:
    """Read an object's body from a named bucket, bypassing pypiron."""
    return minio_get_key_bytes_in(minio, bucket, key).decode()


def minio_get_key_bytes_in(minio: Dict, bucket: str, key: str) -> bytes:
    """Read a binary object from a named bucket, bypassing pypiron.

    A failed read must raise, never return b"" — an empty body impersonates a
    zero-byte object and existence/byte-equality oracles silently pass on
    corruption (the CI zero-byte lesson, commit e08371b)."""
    code, body = _s3_signed(minio, "GET", f"/{bucket}/{key}")
    assert code == 200, f"get {bucket}/{key} -> {code}: {body[:200]!r}"
    return body


def minio_object_sha256(minio: Dict, bucket: str, key: str) -> str:
    """sha256 hex of an object's bytes in a named bucket — the byte-equality
    oracle. Existence-only checks pass when a copy lands corrupt or truncated;
    convergence tests compare this against the source upload to prove the exact
    bytes replicated."""
    return hashlib.sha256(minio_get_key_bytes_in(minio, bucket, key)).hexdigest()


def minio_put_key_in(minio: Dict, bucket: str, key: str, body: str) -> None:
    """Write a foreign object straight into a named bucket, bypassing pypiron."""
    code, resp = _s3_signed(minio, "PUT", f"/{bucket}/{key}", data=body.encode())
    assert code == 200, f"put {bucket}/{key} -> {code}: {resp[:200]!r}"


def minio_key_exists_in(minio: Dict, bucket: str, key: str) -> bool:
    """Whether a key exists in a named bucket. A missing bucket raises — the
    caller is asking about a key, and "no bucket" must not read as "no key"."""
    code, _ = _s3_signed(minio, "HEAD", f"/{bucket}/{key}")
    if code == 200:
        return True
    # HeadObject carries no error body, so HeadBucket distinguishes "no key"
    # from "no bucket". (Not ListObjects with max-keys=0: MinIO answers 200 to
    # that even for a bucket that does not exist.)
    bucket_code, _ = _s3_signed(minio, "HEAD", f"/{bucket}")
    assert bucket_code == 200, f"bucket {bucket} -> {bucket_code}"
    assert code == 404, f"head {bucket}/{key} -> {code}"
    return False


def minio_delete_key_in(minio: Dict, bucket: str, key: str) -> None:
    """Delete one object from a named bucket, bypassing pypiron."""
    code, resp = _s3_signed(minio, "DELETE", f"/{bucket}/{key}")
    assert code == 204, f"delete {bucket}/{key} -> {code}: {resp[:200]!r}"


def minio_remove_bucket(minio: Dict, bucket: str) -> None:
    """Delete a bucket and everything in it (simulates a destination outage).

    Sweep-and-delete retries on BucketNotEmpty: a live pypiron keeps writing
    (markers, index rebuilds) between the key sweep and the bucket DELETE, so
    one pass is a race it can lose (`mc rb --force` retried the same way)."""
    deadline = time.time() + 30.0
    while True:
        for key in minio_list_keys_in(minio, bucket):
            code, resp = _s3_signed(minio, "DELETE", f"/{bucket}/{key}")
            assert code in (204, 404), f"delete {bucket}/{key} -> {code}: {resp[:200]!r}"
        code, resp = _s3_signed(minio, "DELETE", f"/{bucket}")
        if code == 204:
            return
        assert code == 409 and time.time() < deadline, (
            f"remove bucket {bucket} -> {code}: {resp[:200]!r}"
        )


def minio_make_bucket(minio: Dict, bucket: str) -> None:
    """(Re)create a bucket (simulates a destination coming back).

    Retries on 503 XMinioServerNotInitialized: a bucket PUT issued right after
    the container starts can race MinIO's own init and lose."""
    deadline = time.time() + 30.0
    while True:
        code, resp = _s3_signed(minio, "PUT", f"/{bucket}")
        if code in (200, 409):
            return
        assert code == 503 and time.time() < deadline, (
            f"make bucket {bucket} -> {code}: {resp[:200]!r}"
        )
        time.sleep(0.5)


@pytest.fixture()
def s3_server_presigned(tmp_path_factory, pypiron_bin: Path, minio: Dict) -> Iterator[Dict]:
    """S3-backed server that redirects ALL artifact downloads to presigned URLs."""
    yield from _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        # access log on so the redirect test can confirm the wheel GET hit this
        # node (and was answered 302, never streamed).
        extra_env={"PYPIRON_ARTIFACT_DELIVERY": "redirect", "PYPIRON_ACCESS_LOG": "true"},
    )


# ------------------- Shared cloud-backed server launcher ----------------------


def _cloud_creds_env(bind: str) -> Dict[str, str]:
    """Common pypiron env (auth, bind, fast worker) for a cloud-backed server."""
    env = os.environ.copy()
    env.update(
        {
            "PYPIRON_BIND_ADDR": bind,
            "PYPIRON_WORKER_INTERVAL_SECS": "1",
            "PYPIRON_ADMIN_USER": "admin",
            "PYPIRON_ADMIN_PASS": "secret",
            "PYPIRON_UPLOADER_USER": "uploader",
            "PYPIRON_UPLOADER_PASS": "uploadersecret",
            "RUST_LOG": "info,pypiron=debug",
        }
    )
    return env


def _start_cloud_server(tmp_path_factory, pypiron_bin: Path, env: Dict, bind: str, label: str):
    log_path = tmp_path_factory.mktemp(f"pypiron-{label}") / "server.log"
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [str(pypiron_bin), "serve"], env=env, stdout=log_file, stderr=subprocess.STDOUT
        )
        try:
            wait_http_ok(f"http://{bind}/simple/index.json", timeout=30.0)
            yield {
                "bind": bind,
                "base_url": f"http://{bind}",
                "legacy": f"http://{bind}/legacy/",
                "simple": f"http://{bind}/simple/",
                "user": "admin",
                "password": "secret",
                "log_path": log_path,
                "proc": proc,
            }
        finally:
            kill_process_tree(proc)


# GCS note: no local emulator faithfully implements object_store's GCS XML
# data-plane (fake-gcs-server rejects the XML PUT; Google's storage-testbench
# omits the required ETag), so GCS has no *emulator* fixture. The GCS backend
# shares the ObjectStorage code path exercised by the S3 and Azure suites; only
# its builder config differs. See dev/TESTING.md. The `gcs_server` fixture below
# closes the gap end to end when a real bucket is configured (e.g. the real-gcs
# CI job, which runs on pushes to master) — GCS's only live coverage.


def _real_gcs_config() -> Dict | None:
    """Real-GCS target from the environment, or None to skip.

    GCS has no faithful local emulator, so this is real-only. Set
    ``PYPIRON_TEST_GCS_REAL_BUCKET`` to a bucket the credentials can write. Each
    test gets its own ``--storage-prefix``, so the bucket needs no dedication and
    is never emptied wholesale: concurrent runs (two CI jobs, two branches) share
    it without seeing each other. Credentials resolve the way ``object_store``
    resolves them: a service-account JSON via
    ``PYPIRON_TEST_GCS_SERVICE_ACCOUNT_PATH`` or ``GOOGLE_APPLICATION_CREDENTIALS``
    (also required for presigned URLs), otherwise ambient Application Default
    Credentials.
    """
    bucket = os.environ.get("PYPIRON_TEST_GCS_REAL_BUCKET", "").strip()
    if not bucket:
        return None
    sa_path = (
        os.environ.get("PYPIRON_TEST_GCS_SERVICE_ACCOUNT_PATH", "").strip()
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    )
    return {"bucket": bucket, "service_account_path": sa_path or None}


def _gcs_rm_prefix(bucket: str, prefix: str) -> None:
    """Best-effort delete of one test's key subtree via the gcloud CLI with
    ambient credentials. Scoped to `prefix` — never the bucket root, which may
    hold another run's objects. ``rm`` exits non-zero when nothing matches, so
    the result is not checked.

    Best-effort really means it: a killed or cancelled run never reaches this,
    stranding its prefix. The bucket's one-day lifecycle rule is what reaps
    those — see dev/TESTING.md. Don't sweep stale prefixes from here; a
    concurrent run's prefix is indistinguishable from a stranded one."""
    run_returncode(["gcloud", "storage", "rm", "--recursive", f"gs://{bucket}/{prefix}/**"])


@pytest.fixture()
def gcs_server(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """pypiron against a REAL GCS bucket — GCS has no local emulator, so this is
    the only GCS end-to-end coverage. Skips unless ``PYPIRON_TEST_GCS_REAL_BUCKET``
    (and credentials) are configured; see ``_real_gcs_config``.

    Isolation comes from a per-test key prefix rather than from owning the
    bucket, so this is safe to run concurrently with itself."""
    gcs = _real_gcs_config()
    if gcs is None:
        pytest.skip("real GCS test bucket not configured (set PYPIRON_TEST_GCS_REAL_BUCKET)")
    if not cmd_exists("gcloud"):
        pytest.skip("gcloud CLI is required to clean up the GCS test prefix; not found on PATH")
    prefix = f"pytest/{uuid.uuid4().hex[:16]}"
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _cloud_creds_env(bind)
    env.update(
        {
            "PYPIRON_BUCKETS": f"gs://{gcs['bucket']}",
            "PYPIRON_STORAGE_PREFIX": prefix,
        }
    )
    if gcs["service_account_path"]:
        env["PYPIRON_GCS_SERVICE_ACCOUNT_PATH"] = gcs["service_account_path"]
    try:
        yield from _start_cloud_server(tmp_path_factory, pypiron_bin, env, bind, "gcs")
    finally:
        _gcs_rm_prefix(gcs["bucket"], prefix)


# ------------------------------ Azurite fixtures ------------------------------

# Azurite's well-known development account and key (public, fixed by Microsoft).
AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


def _azurite_create_container(port: int, container: str) -> int:
    """Create a blob container in Azurite with a SharedKey-signed PUT (stdlib only)."""
    date = formatdate(timeval=time.time(), usegmt=True)
    version = "2021-08-06"
    canon_headers = f"x-ms-date:{date}\nx-ms-version:{version}\n"
    # Azurite uses path-style URLs (/account/container), so its canonicalized
    # resource is "/{account}" + the path — the account name appears twice.
    canon_resource = f"/{AZURITE_ACCOUNT}/{AZURITE_ACCOUNT}/{container}\nrestype:container"
    string_to_sign = "\n".join(
        ["PUT", "", "", "", "", "", "", "", "", "", "", "", canon_headers + canon_resource]
    )
    signature = base64.b64encode(
        hmac.new(
            base64.b64decode(AZURITE_KEY), string_to_sign.encode("utf-8"), hashlib.sha256
        ).digest()
    ).decode()
    url = f"http://127.0.0.1:{port}/{AZURITE_ACCOUNT}/{container}?restype=container"
    code, _, _ = _http_request(
        url,
        method="PUT",
        headers={
            "x-ms-date": date,
            "x-ms-version": version,
            "Content-Length": "0",
            "Authorization": f"SharedKey {AZURITE_ACCOUNT}:{signature}",
        },
    )
    return code


@pytest.fixture(scope="session")
def _azurite_container() -> Iterator[Dict]:
    """One Azurite per session (per xdist worker); tests isolate by uuid-named
    blob containers, mirroring the session MinIO arrangement. The docker daemon
    assigns the host port for the same bind-race reason."""
    if not cmd_exists("docker"):
        pytest.skip("docker is required for Azure integration tests; not found on PATH")

    name = f"pypiron-azurite-{uuid.uuid4().hex[:12]}"
    try:
        run_checked(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "-p",
                "127.0.0.1::10000",
                "mcr.microsoft.com/azure-storage/azurite",
                "azurite-blob",
                "--blobHost",
                "0.0.0.0",
                "--blobPort",
                "10000",
                "--skipApiVersionCheck",
            ]
        )
        port = _docker_published_port(name, "10000/tcp")
        # Azurite answers (a 400 for the bare account URL is "up and responding").
        wait_http_responding(f"http://127.0.0.1:{port}/{AZURITE_ACCOUNT}", timeout=60.0)
        yield {"port": port}
    finally:
        run_returncode(["docker", "rm", "-f", name])


def _azurite_delete_container(port: int, container: str) -> None:
    """Best-effort drop of a test's blob container from the session Azurite."""
    try:
        _azurite_signed(port, "DELETE", f"/{container}", {"restype": "container"})
    except OSError:
        pass


@pytest.fixture()
def azure(_azurite_container: Dict) -> Iterator[Dict]:
    """A fresh uuid-named blob container on the session Azurite."""
    port = _azurite_container["port"]
    container = f"pypiron-az-{uuid.uuid4().hex[:8]}"
    code = _azurite_create_container(port, container)
    if code not in (201, 409):
        pytest.skip(f"unable to create Azurite container (status {code})")
    try:
        yield {
            "endpoint": f"http://127.0.0.1:{port}/{AZURITE_ACCOUNT}",
            "account": AZURITE_ACCOUNT,
            "key": AZURITE_KEY,
            "container": container,
        }
    finally:
        _azurite_delete_container(port, container)


@pytest.fixture()
def azure_server(tmp_path_factory, pypiron_bin: Path, azure: Dict) -> Iterator[Dict]:
    """pypiron configured against the Azurite Azure Blob backend."""
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _cloud_creds_env(bind)
    env.update(
        {
            "PYPIRON_BUCKETS": f"az://{azure['container']}",
            "PYPIRON_AZURE_ACCOUNT": azure["account"],
            "PYPIRON_AZURE_ACCESS_KEY": azure["key"],
            "PYPIRON_AZURE_ENDPOINT_URL": azure["endpoint"],
        }
    )
    yield from _start_cloud_server(tmp_path_factory, pypiron_bin, env, bind, "azure")


# ------------------------- Azurite object inspection --------------------------
#
# SharedKey-signed reads straight against Azurite, so the mixed-backend suite can
# check the Azure leg of a fan-out without going through pypiron — the Azure
# analog of the `mc`-driven MinIO helpers above. Stdlib only, mirroring
# `_azurite_create_container`'s signing.


def _azurite_port(azure: Dict) -> int:
    port = urlsplit(azure["endpoint"]).port
    assert port is not None, f"azure endpoint has no port: {azure['endpoint']!r}"
    return port


def _azurite_signed(port: int, method: str, path: str, query: Optional[Dict[str, str]] = None):
    """One SharedKey-signed request to Azurite. `path` starts at the container
    (`/container/blob`); `query` params join the canonical resource sorted."""
    date = formatdate(timeval=time.time(), usegmt=True)
    version = "2021-08-06"
    canon_headers = f"x-ms-date:{date}\nx-ms-version:{version}\n"
    resource = f"/{AZURITE_ACCOUNT}/{AZURITE_ACCOUNT}{path}"
    for name in sorted(query or {}):
        resource += f"\n{name}:{query[name]}"
    string_to_sign = "\n".join([method] + [""] * 11 + [canon_headers + resource])
    signature = base64.b64encode(
        hmac.new(
            base64.b64decode(AZURITE_KEY), string_to_sign.encode("utf-8"), hashlib.sha256
        ).digest()
    ).decode()
    url = f"http://127.0.0.1:{port}/{AZURITE_ACCOUNT}{path}"
    if query:
        url += "?" + urlencode(query)
    return _http_request(
        url,
        method=method,
        headers={
            "x-ms-date": date,
            "x-ms-version": version,
            "Authorization": f"SharedKey {AZURITE_ACCOUNT}:{signature}",
        },
    )


def azurite_get_blob(azure: Dict, key: str) -> Optional[bytes]:
    """A blob's bytes, or None if it does not exist (the 404 analog)."""
    code, body, _ = _azurite_signed(_azurite_port(azure), "GET", f"/{azure['container']}/{key}")
    if code == 404:
        return None
    assert code == 200, f"azurite GET {key} -> {code}: {body[:200]!r}"
    return body


def azurite_key_exists(azure: Dict, key: str) -> bool:
    return azurite_get_blob(azure, key) is not None


def azurite_object_sha256(azure: Dict, key: str) -> str:
    body = azurite_get_blob(azure, key)
    assert body is not None, f"azurite blob {key} is absent"
    return hashlib.sha256(body).hexdigest()


def azurite_list_keys(azure: Dict, prefix: str = "") -> List[str]:
    """Every blob name in the container (optionally under `prefix`), as
    pypiron-independent ground truth — the Azure analog of `minio_list_keys_in`."""
    query = {"restype": "container", "comp": "list"}
    if prefix:
        query["prefix"] = prefix
    code, body, _ = _azurite_signed(
        _azurite_port(azure), "GET", f"/{azure['container']}", query=query
    )
    assert code == 200, f"azurite list -> {code}: {body[:200]!r}"
    return sorted(re.findall(r"<Name>(.*?)</Name>", body.decode()))


# --------------------------- Mixed-backend topology ---------------------------


@pytest.fixture()
def mixed_cloud(azure: Dict, _minio_container: Dict) -> Iterator[Dict]:
    """A locally-runnable mixed-backend topology: one S3 bucket (MinIO, behind a
    fault proxy so it can be made unavailable) plus one Azure container
    (Azurite). Skips without Docker like the single-cloud suites.

    fake-gcs-server is not faithful to object_store's GCS data plane (see the GCS
    note above), so Azurite is the faithful local second cloud here; the GCS leg
    of the multi-cloud claim is covered by the real-GCS job."""
    minio_gen = _minio_buckets(_minio_container, 1, "mixed")
    minio = next(minio_gen)
    proxy_gen = _minio_fault_proxy(minio)
    try:
        proxied = next(proxy_gen)
        yield {"s3": proxied, "azure": azure}
    finally:
        proxy_gen.close()
        minio_gen.close()


def _mixed_env(mixed: Dict, bind: str) -> Dict[str, str]:
    s3 = mixed["s3"]
    az = mixed["azure"]
    env = os.environ.copy()
    env.update(
        {
            "PYPIRON_BIND_ADDR": bind,
            # S3 preferred (index 0), Azure the failover (index 1).
            "PYPIRON_BUCKETS": f"s3://{s3['bucket']},az://{az['container']}",
            "PYPIRON_WORKER_INTERVAL_SECS": "1",
            "PYPIRON_ADMIN_USER": "admin",
            "PYPIRON_ADMIN_PASS": "secret",
            "PYPIRON_UPLOADER_USER": "uploader",
            "PYPIRON_UPLOADER_PASS": "uploadersecret",
            # Stream artifacts so a failover read is a deterministic 200 with the
            # bytes, never a presigned redirect to a specific store.
            "PYPIRON_ARTIFACT_DELIVERY": "stream",
            # S3 (MinIO through the fault proxy).
            "PYPIRON_S3_ENDPOINT_URL": s3.get("server_endpoint", s3["endpoint"]),
            "PYPIRON_S3_FORCE_PATH_STYLE": "true",
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": s3["access_key"],
            "AWS_SECRET_ACCESS_KEY": s3["secret_key"],
            # Azure (Azurite); the container comes from the az:// bucket URI.
            "PYPIRON_AZURE_ACCOUNT": az["account"],
            "PYPIRON_AZURE_ACCESS_KEY": az["key"],
            "PYPIRON_AZURE_ENDPOINT_URL": az["endpoint"],
            "RUST_LOG": "info,pypiron=debug",
        }
    )
    return env


def _start_mixed_cloud_server(
    tmp_path_factory, pypiron_bin: Path, mixed: Dict, extra_env=None, extra_args=None
) -> Iterator[Dict]:
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    log_path = tmp_path_factory.mktemp("pypiron-mixed") / "server.log"
    env = _mixed_env(mixed, bind)
    if extra_env:
        env.update(extra_env)
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [str(pypiron_bin), "serve", *(extra_args or [])],
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_http_ok(f"http://{bind}/simple/index.json", timeout=30.0)
            yield {
                "bind": bind,
                "base_url": f"http://{bind}",
                "legacy": f"http://{bind}/legacy/",
                "simple": f"http://{bind}/simple/",
                "user": "admin",
                "password": "secret",
                "mixed": mixed,
                "s3": mixed["s3"],
                "azure": mixed["azure"],
                "faults": mixed["s3"].get("faults"),
                "log_path": log_path,
                "proc": proc,
            }
        finally:
            kill_process_tree(proc)


@pytest.fixture()
def mixed_cloud_server(tmp_path_factory, pypiron_bin: Path, mixed_cloud: Dict) -> Iterator[Dict]:
    """pypiron serving a mixed S3+Azure two-bucket topology."""
    yield from _start_mixed_cloud_server(tmp_path_factory, pypiron_bin, mixed_cloud)
