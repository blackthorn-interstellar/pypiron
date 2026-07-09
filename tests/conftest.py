from __future__ import annotations

import base64
import hashlib
import hmac
import os
import shlex
import subprocess
import time
from datetime import datetime, timezone
from email.utils import formatdate
from pathlib import Path
from typing import Dict, Iterator, List

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


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
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


def _start_disk_server(
    tmp_path_factory, bin_path: Path, extra_args=(), extra_env=None
) -> Iterator[Dict]:
    data_dir = tmp_path_factory.mktemp("pypiron-data")
    log_path = data_dir.parent / f"{data_dir.name}-server.log"
    port = find_free_port()
    bind = f"127.0.0.1:{port}"

    # Two roles: admin (everything, incl. mirror/delete/yank) and uploader
    # (publish only). The dict's `user`/`password` are the admin credential —
    # a superset — so tests that do any operation through them keep working.
    args = [
        str(bin_path),
        "serve",
        "--bind-addr",
        bind,
        "--data-dir",
        str(data_dir),
        "--admin-user",
        "admin",
        "--admin-pass",
        "secret",
        "--uploader-user",
        "uploader",
        "--uploader-pass",
        "uploadersecret",
        "--worker-interval-secs",
        "1",
        *extra_args,
    ]

    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info,pypiron=debug")
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
                "user": "admin",
                "password": "secret",
                "admin_user": "admin",
                "admin_password": "secret",
                "uploader_user": "uploader",
                "uploader_password": "uploadersecret",
                "data_dir": data_dir,
                "log_path": log_path,
                "proc": proc,
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
    data_dir = tmp_path_factory.mktemp("pypiron-admin-pass-only")
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
        "--admin-pass",
        "secret",
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
                "admin_user": "admin",
                "admin_password": "secret",
                "data_dir": data_dir,
                "log_path": log_path,
                "proc": proc,
            }
        finally:
            kill_process_tree(proc)


@pytest.fixture()
def disk_server_no_creds(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """Disk server with no credentials at all: read-only, every write disabled."""
    data_dir = tmp_path_factory.mktemp("pypiron-no-creds")
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
                "data_dir": data_dir,
                "log_path": log_path,
                "proc": proc,
            }
        finally:
            kill_process_tree(proc)


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


@pytest.fixture()
def minio(tmp_path_factory) -> Iterator[Dict]:
    """S3 backend for the s3-marked suite.

    Default: a throwaway MinIO container with a fresh bucket (skips without
    Docker). If ``PYPIRON_TEST_S3_REAL_BUCKET`` is set, targets that real S3
    bucket instead (see ``_real_s3_config``), emptying it around each test so
    every test still starts from an empty bucket. Real-bucket runs must be
    serial — the shared bucket is wiped per test, so `-n`/xdist would corrupt
    concurrent tests."""
    real = _real_s3_config()
    if real is not None:
        _s3_empty_bucket(real["bucket"])
        try:
            yield real
        finally:
            _s3_empty_bucket(real["bucket"])
        return

    if not cmd_exists("docker"):
        pytest.skip("docker is required for S3/MinIO integration tests; not found on PATH")

    s3_port = find_free_port()
    name = f"pypiron-minio-{s3_port}-{int(time.time())}"
    bucket = "pypiron-test"
    run_checked(
        [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "-p",
            f"{s3_port}:9000",
            "-e",
            "MINIO_ROOT_USER=minioadmin",
            "-e",
            "MINIO_ROOT_PASSWORD=minioadmin",
            "minio/minio",
            "server",
            "/data",
        ]
    )

    try:
        wait_http_ok(f"http://127.0.0.1:{s3_port}/minio/health/ready", timeout=60.0)

        # Create the bucket with minio/mc; host.docker.internal first, host network fallback.
        rc, _, _ = run_returncode(
            [
                "docker",
                "run",
                "--rm",
                "-e",
                f"MC_HOST_local=http://minioadmin:minioadmin@host.docker.internal:{s3_port}",
                "minio/mc",
                "mb",
                "--ignore-existing",
                f"local/{bucket}",
            ]
        )
        if rc != 0:
            rc, _, _ = run_returncode(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--network",
                    "host",
                    "-e",
                    f"MC_HOST_local=http://minioadmin:minioadmin@127.0.0.1:{s3_port}",
                    "minio/mc",
                    "mb",
                    "--ignore-existing",
                    f"local/{bucket}",
                ]
            )
        if rc != 0:
            pytest.skip("Unable to create MinIO bucket using minio/mc (check Docker networking)")

        yield {
            "endpoint": f"http://127.0.0.1:{s3_port}",
            "bucket": bucket,
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
        }
    finally:
        run_returncode(["docker", "rm", "-f", name])


def _s3_env(minio: Dict, bind: str) -> Dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "PYPIRON_STORAGE": "s3",
            "PYPIRON_S3_BUCKET": minio["bucket"],
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
    if minio.get("endpoint"):
        # Emulator (MinIO): explicit endpoint, path-style addressing, fixed
        # dev credentials. Real S3 has none of these — it relies on the ambient
        # AWS credentials already carried over by os.environ.copy() above.
        env["PYPIRON_S3_ENDPOINT_URL"] = minio["endpoint"]
        env["PYPIRON_S3_FORCE_PATH_STYLE"] = "true"
        env["AWS_ACCESS_KEY_ID"] = minio["access_key"]
        env["AWS_SECRET_ACCESS_KEY"] = minio["secret_key"]
    return env


def _start_s3_server(
    tmp_path_factory, pypiron_bin: Path, minio: Dict, extra_env=None
) -> Iterator[Dict]:
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    log_path = tmp_path_factory.mktemp("pypiron-s3") / "server.log"
    env = _s3_env(minio, bind)
    if extra_env:
        env.update(extra_env)

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
                "minio": minio,
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


def _mc(minio: Dict, script: str) -> str:
    """Run a `mc` shell snippet against the MinIO container, returning stdout.
    Mirrors the host.docker.internal → host-network fallback the `minio` fixture
    uses. Skips only on a networking failure; a broken `mc` command is a bug and
    must fail the test, not silently pass it."""
    if not minio.get("endpoint"):
        pytest.skip("mc bucket inspection targets the MinIO emulator, not a real S3 bucket")
    port = minio["endpoint"].rsplit(":", 1)[1]
    creds = f"{minio['access_key']}:{minio['secret_key']}"
    attempts = []
    for extra in (
        ["-e", f"MC_HOST_local=http://{creds}@host.docker.internal:{port}"],
        ["-e", f"MC_HOST_local=http://{creds}@127.0.0.1:{port}", "--network", "host"],
    ):
        rc, out, err = run_returncode(
            ["docker", "run", "--rm", *extra, "--entrypoint", "sh", "minio/mc", "-c", script]
        )
        if rc == 0:
            return out
        attempts.append(f"rc={rc} {err.strip() or out.strip()}")
    if any("unreachable" in a or "connection refused" in a.lower() for a in attempts):
        pytest.skip("Unable to reach MinIO with minio/mc (check Docker networking)")
    raise AssertionError(f"mc failed: {script!r}\n" + "\n".join(attempts))


def minio_put_key(minio: Dict, key: str, body: str) -> None:
    """Write a foreign object straight into the bucket, bypassing pypiron."""
    dest = shlex.quote(f"local/{minio['bucket']}/{key}")
    _mc(minio, f"printf %s {shlex.quote(body)} | mc pipe {dest}")


def minio_get_key(minio: Dict, key: str) -> str:
    """Read an object's body straight from the bucket, bypassing pypiron."""
    target = shlex.quote(f"local/{minio['bucket']}/{key}")
    return _mc(minio, f"mc cat {target}")


def minio_list_keys(minio: Dict) -> List[str]:
    """Every object key in the bucket, as pypiron-independent ground truth."""
    root = f"local/{minio['bucket']}/"
    out = _mc(minio, f"mc find {shlex.quote(root.rstrip('/'))}")
    return sorted(ln[len(root) :] for ln in out.splitlines() if ln.startswith(root))


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
# closes the gap end to end when a real bucket is configured (e.g. the weekly
# real-gcs CI job) — GCS's only live coverage.


def _real_gcs_config() -> Dict | None:
    """Real-GCS target from the environment, or None to skip.

    GCS has no faithful local emulator, so this is real-only. Set
    ``PYPIRON_TEST_GCS_REAL_BUCKET`` to a **dedicated, disposable** bucket; the
    fixture owns its whole contents and empties it around each test (pypiron
    writes to the bucket root — no per-run key prefix). Credentials resolve the
    way ``object_store`` resolves them: a service-account JSON via
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


def _gcs_empty_bucket(bucket: str) -> None:
    """Best-effort delete of every object in a real GCS bucket via the gcloud
    CLI with ambient credentials. Skips the run if the CLI is unavailable.
    ``rm`` exits non-zero on an empty match, so the result is not checked; the
    per-test setup wipe means a stray leftover self-heals on the next run."""
    if not cmd_exists("gcloud"):
        pytest.skip("gcloud CLI is required to manage the real GCS test bucket; not found on PATH")
    run_returncode(["gcloud", "storage", "rm", "--recursive", f"gs://{bucket}/**"])


@pytest.fixture()
def gcs_server(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict]:
    """pypiron against a REAL GCS bucket — GCS has no local emulator, so this is
    the only GCS end-to-end coverage. Skips unless ``PYPIRON_TEST_GCS_REAL_BUCKET``
    (and credentials) are configured; see ``_real_gcs_config``."""
    gcs = _real_gcs_config()
    if gcs is None:
        pytest.skip("real GCS test bucket not configured (set PYPIRON_TEST_GCS_REAL_BUCKET)")
    _gcs_empty_bucket(gcs["bucket"])
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _cloud_creds_env(bind)
    env.update({"PYPIRON_STORAGE": "gcs", "PYPIRON_GCS_BUCKET": gcs["bucket"]})
    if gcs["service_account_path"]:
        env["PYPIRON_GCS_SERVICE_ACCOUNT_PATH"] = gcs["service_account_path"]
    try:
        yield from _start_cloud_server(tmp_path_factory, pypiron_bin, env, bind, "gcs")
    finally:
        _gcs_empty_bucket(gcs["bucket"])


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


@pytest.fixture()
def azure(tmp_path_factory) -> Iterator[Dict]:
    """Start Azurite via Docker with a fresh container; skip without Docker."""
    if not cmd_exists("docker"):
        pytest.skip("docker is required for Azure integration tests; not found on PATH")

    port = find_free_port()
    name = f"pypiron-azurite-{port}-{int(time.time())}"
    container = "pypiron-test"
    endpoint = f"http://127.0.0.1:{port}/{AZURITE_ACCOUNT}"
    run_checked(
        [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "-p",
            f"{port}:10000",
            "mcr.microsoft.com/azure-storage/azurite",
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            "--blobPort",
            "10000",
            "--skipApiVersionCheck",
        ]
    )
    try:
        # Azurite answers (a 400 for the bare account URL is "up and responding").
        wait_http_responding(f"http://127.0.0.1:{port}/{AZURITE_ACCOUNT}", timeout=60.0)
        code = _azurite_create_container(port, container)
        if code not in (201, 409):
            pytest.skip(f"unable to create Azurite container (status {code})")
        yield {
            "endpoint": endpoint,
            "account": AZURITE_ACCOUNT,
            "key": AZURITE_KEY,
            "container": container,
        }
    finally:
        run_returncode(["docker", "rm", "-f", name])


@pytest.fixture()
def azure_server(tmp_path_factory, pypiron_bin: Path, azure: Dict) -> Iterator[Dict]:
    """pypiron configured against the Azurite Azure Blob backend."""
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _cloud_creds_env(bind)
    env.update(
        {
            "PYPIRON_STORAGE": "azure",
            "PYPIRON_AZURE_ACCOUNT": azure["account"],
            "PYPIRON_AZURE_CONTAINER": azure["container"],
            "PYPIRON_AZURE_ACCESS_KEY": azure["key"],
            "PYPIRON_AZURE_ENDPOINT_URL": azure["endpoint"],
        }
    )
    yield from _start_cloud_server(tmp_path_factory, pypiron_bin, env, bind, "azure")
