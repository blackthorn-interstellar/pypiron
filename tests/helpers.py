from __future__ import annotations

import base64
import hashlib
import json
import os
import platform
import random
import re
import shutil
import socket
import subprocess
import time
import uuid
import zipfile
import zlib
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ACCEPT_PEP691 = "application/vnd.pypi.simple.v1+json"
CLIENT_PINS = {
    "pip": "venv-seeded",
    "uv": "system",
    "poetry": "2.4.1",
    "pdm": "2.27.0",
    "twine": "dev-dependency",
    "flit": "4.0.2",
    "hatch": "1.17.0",
    "pipenv": "2026.5.2",
}


def module_name(package: str) -> str:
    """The importable module name a build backend derives from a package name."""
    return re.sub(r"\W+", "_", package).strip("_").lower()


def unique_package(client: str) -> str:
    """A collision-free ``pypiron-compat-<client>-<rand>`` distribution name."""
    return f"pypiron-compat-{client}-{uuid.uuid4().hex[:8]}"


def origin_owner(raw: str | bytes) -> str:
    """Read current nonce-bearing claims and legacy plain-text fixtures."""
    text = raw.decode() if isinstance(raw, bytes) else raw
    text = text.strip()
    if text.startswith("{"):
        body = json.loads(text)
        owner = body.get("origin")
        nonce = body.get("nonce")
        assert owner in {"private", "mirror", "unclaimed"}, body
        assert isinstance(nonce, str) and re.fullmatch(r"[0-9a-f]{32}", nonce), body
        return owner
    assert text in {"private", "mirror", "unclaimed"}, text
    return text


def uvx_client(client: str) -> list[str]:
    pin = CLIENT_PINS[client]
    if pin in {"venv-seeded", "system", "dev-dependency"}:
        raise ValueError(f"{client} is not run through uv tool")
    return ["uv", "tool", "run", "--from", f"{client}=={pin}", client]


# -------------------------- Command / Process helpers -------------------------


def cmd_exists(cmd: str) -> bool:
    return shutil.which(cmd) is not None


def run_checked(
    args: Iterable[str],
    *,
    cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
    capture_output: bool = True,
    text: bool = True,
    timeout: Optional[float] = None,
) -> subprocess.CompletedProcess:
    """Run a subprocess and raise with rich context on failure."""
    try:
        cp = subprocess.run(
            list(args),
            cwd=str(cwd) if cwd else None,
            env=env,
            capture_output=capture_output,
            text=text,
            timeout=timeout,
            check=True,
        )
        return cp
    except subprocess.CalledProcessError as e:
        stdout = e.stdout if e.stdout else ""
        stderr = e.stderr if e.stderr else ""
        msg = (
            f"Command failed ({e.returncode}): {' '.join(args)}\n"
            f"--- STDOUT ---\n{stdout}\n--- STDERR ---\n{stderr}"
        )
        raise RuntimeError(msg) from e


def run_returncode(
    args: Iterable[str],
    *,
    cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
) -> Tuple[int, str, str]:
    """Run a subprocess and return (rc, stdout, stderr)."""
    cp = subprocess.run(
        list(args),
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return cp.returncode, cp.stdout, cp.stderr


def sync_to(
    pypiron_bin,
    server: Dict,
    *extra: str,
    source: Optional[str] = None,
    cwd: Optional[Path] = None,
    timeout: float = 600,
    env: Optional[Dict[str, str]] = None,
) -> Tuple[int, str, str]:
    """Run `pypiron sync` in mirror-over-HTTP mode against `server` (the only
    mode). Mirroring is an admin operation, so it authenticates with the admin
    credential. Pass the package selection (`--include-package`/
    `--include-packages-from`) and any mirror rules in `*extra`; `source` sets
    `--from`. Returns (rc, out, err)."""
    args = [
        str(pypiron_bin),
        "sync",
        "--to",
        server["base_url"],
        "--admin-user",
        server.get("admin_user", server["user"]),
        "--admin-pass",
        server.get("admin_password", server["password"]),
    ]
    if source is not None:
        args += ["--from", source]
    return run_returncode([*args, *extra], cwd=cwd, timeout=timeout, env=env)


def kill_process_tree(proc: subprocess.Popen) -> None:
    """Terminate a process, then kill if needed (cross-platform)."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
            return
        except subprocess.TimeoutExpired:
            pass
        proc.kill()
    except Exception:
        # Best-effort cleanup
        pass


# ----------------------------- Network helpers -------------------------------


#: Next candidate in this worker's port range; see find_free_port.
_next_worker_port: Optional[int] = None

#: Each xdist worker probes a disjoint 180-port slice of 21000-31799 — below
#: the OS ephemeral range, so the kernel never hands our listener ports out as
#: outgoing source ports between probe and server bind.
_WORKER_PORT_SPAN = 180
_WORKER_PORT_BASE = 21000
_WORKER_SLOTS = 60


def find_free_port() -> int:
    """A free localhost port for a server about to be spawned.

    Serial runs use an OS-assigned ephemeral port. Under pytest-xdist that is
    a cross-process race — bind/close/return means two workers can be handed
    the same port before either server binds it — so each worker instead scans
    its own disjoint range, and a sequential cursor avoids handing the same
    port out twice in a row within a worker."""
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker is None:
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    global _next_worker_port
    index = int("".join(filter(str.isdigit, worker)) or "0")
    if index >= _WORKER_SLOTS:
        raise RuntimeError(
            f"xdist worker {worker} exceeds the {_WORKER_SLOTS}-slot port map; "
            "run with fewer workers or widen _WORKER_SLOTS/_WORKER_PORT_BASE"
        )
    # Salt the slot with the test-run id: concurrent pytest sessions on one
    # machine (the norm in this repo) must not walk the same ranges in
    # lockstep. The shift is constant within a session, so worker slots stay
    # disjoint; the pid offset desynchronizes the rare cross-session slot tie.
    salt = zlib.crc32(os.environ.get("PYTEST_XDIST_TESTRUNUID", "").encode())
    low = _WORKER_PORT_BASE + ((index + salt) % _WORKER_SLOTS) * _WORKER_PORT_SPAN
    if _next_worker_port is None:
        _next_worker_port = low + (os.getpid() % _WORKER_PORT_SPAN)
    for _ in range(_WORKER_PORT_SPAN):
        _next_worker_port = low + (_next_worker_port - low + 1) % _WORKER_PORT_SPAN
        s = socket.socket()
        try:
            s.bind(("127.0.0.1", _next_worker_port))
        except OSError:
            continue
        finally:
            s.close()
        return _next_worker_port
    raise RuntimeError(f"no free port in worker range {low}-{low + _WORKER_PORT_SPAN - 1}")


def _http_request(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    data: Optional[bytes] = None,
    timeout: float = 10.0,
) -> Tuple[int, bytes, Dict[str, str]]:
    req = Request(url, method=method)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urlopen(req, data=data, timeout=timeout) as resp:
            code = resp.getcode()
            body = resp.read()
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            return code, body, hdrs
    except HTTPError as e:
        return (
            e.code,
            e.read() if e.fp else b"",
            {k.lower(): v for k, v in (e.headers or {}).items()},
        )
    except URLError as e:
        raise ConnectionError(f"HTTP request failed to {url}: {e}") from e


def http_get(
    url: str, *, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0
) -> Tuple[int, bytes, Dict[str, str]]:
    """GET returning (status, body, headers); does not raise on non-2xx."""
    return _http_request(url, method="GET", headers=headers, timeout=timeout)


def http_head(
    url: str, *, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0
) -> Tuple[int, bytes, Dict[str, str]]:
    """HEAD returning (status, body, headers); does not raise on non-2xx."""
    return _http_request(url, method="HEAD", headers=headers, timeout=timeout)


def _raw_request(
    method: str,
    url: str,
    *,
    data: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 10.0,
) -> Tuple[int, bytes, Dict[str, str]]:
    """Issue one request without following redirects (for asserting raw 3xx)."""
    import http.client
    from urllib.parse import urlparse

    p = urlparse(url)
    conn = http.client.HTTPConnection(p.hostname, p.port, timeout=timeout)
    try:
        path = p.path + (f"?{p.query}" if p.query else "")
        conn.request(method, path, body=data, headers=headers or {})
        resp = conn.getresponse()
        body = resp.read()
        resp_headers = {k.lower(): v for k, v in resp.getheaders()}
        return resp.status, body, resp_headers
    finally:
        conn.close()


def http_get_no_redirect(
    url: str, *, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0
) -> Tuple[int, bytes, Dict[str, str]]:
    """GET without following redirects (for asserting 302s)."""
    return _raw_request("GET", url, headers=headers, timeout=timeout)


def _post_no_redirect(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[bytes] = None,
    timeout: float = 10.0,
) -> Tuple[int, bytes, Dict[str, str]]:
    """POST without following redirects (for asserting the raw status)."""
    return _raw_request("POST", url, data=data, headers=headers, timeout=timeout)


def http_request_auth(
    method: str,
    url: str,
    *,
    username: str,
    password: str,
    data: Optional[bytes] = None,
    timeout: float = 10.0,
) -> Tuple[int, bytes, Dict[str, str]]:
    """Authenticated request (DELETE/POST management API); does not raise."""
    headers = {"Authorization": _encode_basic_auth(username, password)}
    return _http_request(url, method=method, headers=headers, data=data, timeout=timeout)


def http_get_bytes(
    url: str, *, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0
) -> bytes:
    code, body, _ = _http_request(url, method="GET", headers=headers, timeout=timeout)
    if code < 200 or code >= 300:
        raise RuntimeError(f"GET {url} failed with status {code}")
    return body


def http_get_json(url: str, *, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0):
    hdrs = {"Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    data = http_get_bytes(url, headers=hdrs, timeout=timeout)
    return json.loads(data.decode("utf-8"))


def get_index_json(simple_url: str, package: Optional[str] = None, *, timeout: float = 10.0):
    """Fetch a PEP 691 JSON index — the global index, or `package`'s — as parsed JSON."""
    suffix = f"{package}/index.json" if package else "index.json"
    return http_get_json(
        f"{simple_url}{suffix}", headers={"Accept": ACCEPT_PEP691}, timeout=timeout
    )


# Sentinel a predicate returns while the condition is not yet met; anything
# else (including None) counts as success and is handed back to the caller.
_PENDING = object()


def _poll_until(predicate, *, timeout: float, interval: float):
    """Call `predicate()` until it returns a non-`_PENDING` value or `timeout`
    elapses, sleeping `interval` between tries. Returns the predicate's value on
    success, or `_PENDING` if it timed out — the caller decides how to raise."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result is not _PENDING:
            return result
        time.sleep(interval)
    return _PENDING


def wait_http_ok(url: str, *, timeout: float = 15.0, interval: float = 0.1) -> None:
    """Poll until GET returns 2xx or timeout."""
    last_err = None

    def probe():
        nonlocal last_err
        try:
            code, _, _ = _http_request(url)
            if 200 <= code < 300:
                return True
        except Exception as e:  # noqa: BLE001
            last_err = e
        return _PENDING

    if _poll_until(probe, timeout=timeout, interval=interval) is not _PENDING:
        return
    if last_err:
        raise TimeoutError(f"Timed out waiting for {url}: last error: {last_err}")
    raise TimeoutError(f"Timed out waiting for {url}")


def wait_http_responding(url: str, *, timeout: float = 15.0, interval: float = 0.1) -> None:
    """Poll until GET returns any HTTP status (readiness for auth-gated servers)."""
    last_err = None

    def probe():
        nonlocal last_err
        try:
            _http_request(url)
            return True
        except Exception as e:  # noqa: BLE001
            last_err = e
        return _PENDING

    if _poll_until(probe, timeout=timeout, interval=interval) is not _PENDING:
        return
    raise TimeoutError(f"Timed out waiting for {url}: last error: {last_err}")


# ------------------------------ PyPI helpers ----------------------------------


def pypi_release_file(package: str, version: str, suffix: str = ".whl") -> Tuple[str, str]:
    """Look up (filename, url) for a release file on pypi.org via its JSON API."""
    data = http_get_json(f"https://pypi.org/pypi/{package}/{version}/json", timeout=30.0)
    for f in data.get("urls", []):
        if f["filename"].endswith(suffix):
            return f["filename"], f["url"]
    raise RuntimeError(f"No {suffix} file found for {package}=={version} on PyPI")


def pypi_project_json(package: str) -> dict:
    """Full project JSON from pypi.org (all releases, with upload times)."""
    return http_get_json(f"https://pypi.org/pypi/{package}/json", timeout=30.0)


def download_pypi_wheel(package: str, version: str, dest_dir: Path) -> Path:
    """Download a real wheel from public PyPI into dest_dir; return its path."""
    filename, url = pypi_release_file(package, version)
    path = dest_dir / filename
    path.write_bytes(http_get_bytes(url, timeout=120.0))
    return path


def pypi_provenance(package: str, version: str, filename: str, *, timeout: float = 30.0) -> dict:
    """Fetch a file's PEP 740 provenance object from PyPI's integrity API."""
    url = f"https://pypi.org/integrity/{package}/{version}/{filename}/provenance"
    return json.loads(http_get_bytes(url, timeout=timeout).decode("utf-8"))


def wait_for_file_in_index(
    simple_url: str, package: str, filename: str, *, timeout: float = 30.0
) -> dict:
    """Poll the PEP 691 package index until `filename` appears; return the index doc."""

    def probe():
        try:
            data = http_get_json(
                f"{simple_url}{package}/index.json", headers={"Accept": ACCEPT_PEP691}
            )
            if filename in [f.get("filename") for f in data.get("files", [])]:
                return data
        except (RuntimeError, ConnectionError):
            pass
        return _PENDING

    result = _poll_until(probe, timeout=timeout, interval=0.2)
    if result is not _PENDING:
        return result
    raise TimeoutError(f"{filename} did not appear in index for {package} within {timeout}s")


def wait_for_project_in_global(
    simple_url: str,
    package: str,
    *,
    timeout: float = 30.0,
    headers: Optional[Dict[str, str]] = None,
) -> None:
    """Poll the global index until `package` is listed.

    The package index is written before the global one; tests that read the
    global index right after an upload must wait for this or race the worker.
    """
    accept = {"Accept": ACCEPT_PEP691, **(headers or {})}

    def probe():
        try:
            data = http_get_json(f"{simple_url}index.json", headers=accept)
            if package in [p.get("name") for p in data.get("projects", [])]:
                return True
        except (RuntimeError, ConnectionError):
            pass
        return _PENDING

    if _poll_until(probe, timeout=timeout, interval=0.2) is not _PENDING:
        return
    raise TimeoutError(f"{package} did not appear in the global index within {timeout}s")


def wait_storage_ops_quiet(
    base_url: str, *, timeout: float = 20.0, quiet: float = 0.25, streak: int = 5
) -> None:
    """Wait until the storage WRITE/DELETE counters hold still for `streak`
    consecutive samples `quiet` seconds apart — the async writer is done
    mutating the tree.

    A test that snapshots, digests, or tars the data tree must call this first:
    an upload's HTTP response returns before the worker's view writes (package
    index, global index, inventory) have all landed. Only mutations count —
    the worker's idle ticks read and list on a cadence that never goes silent.
    Sustained silence, not one quiet gap — a loaded box can pause the worker
    mid-tail.
    """

    def ops() -> tuple:
        code, body, _ = http_get(f"{base_url}/metrics")
        assert code == 200, code
        return tuple(
            sorted(
                line
                for line in body.decode().splitlines()
                if line.startswith("pypiron_storage_ops_total")
                and ('op="write"' in line or 'op="delete"' in line)
            )
        )

    deadline = time.monotonic() + timeout
    last, run = None, 0
    while time.monotonic() < deadline:
        cur = ops()
        run = run + 1 if cur == last else 1
        last = cur
        if run >= streak:
            return
        time.sleep(quiet)
    raise TimeoutError(f"storage ops never settled within {timeout}s")


# ------------------------------ File / Hashing --------------------------------


def sha256_file(path: Path) -> str:
    m = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            m.update(chunk)
    return m.hexdigest()


def make_wheel(
    name: str,
    version: str,
    dest_dir: Path,
    *,
    metadata_extra: str = "",
    description: str = "",
    payload_bytes: int = 0,
) -> Path:
    """A minimal valid wheel. `payload_bytes` pads it with that many bytes of
    incompressible data, for tests that need a wheel of a realistic size (the
    proxy's streaming threshold, for one) without downloading a real one."""
    safe_name = re.sub(r"[^A-Za-z0-9.]+", "_", name).strip("_")
    module_name = re.sub(r"\W+", "_", name).strip("_").lower()
    dist_info = f"{safe_name}-{version}.dist-info"
    wheel_path = dest_dir / f"{safe_name}-{version}-py3-none-any.whl"
    metadata = (
        "Metadata-Version: 2.1\n"
        f"Name: {name}\n"
        f"Version: {version}\n"
        "Summary: pypiron test package\n"
        f"{metadata_extra}"
    )
    if description:
        metadata += "\n" + description
    files = {
        f"{module_name}.py": f'__version__ = "{version}"\n'.encode(),
        # Deflate is applied below, so the padding has to be random or the wheel
        # would compress back down to nothing.
        **(
            {f"{module_name}_payload.bin": random.randbytes(payload_bytes)} if payload_bytes else {}
        ),
        f"{dist_info}/METADATA": metadata.encode(),
        f"{dist_info}/WHEEL": (
            "Wheel-Version: 1.0\n"
            "Generator: pypiron-tests\n"
            "Root-Is-Purelib: true\n"
            "Tag: py3-none-any\n"
        ).encode(),
    }

    record_lines = []
    for path, data in files.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        record_lines.append(f"{path},sha256={digest},{len(data)}")
    record_path = f"{dist_info}/RECORD"
    record_lines.append(f"{record_path},,")
    files[record_path] = ("\n".join(record_lines) + "\n").encode()

    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path, data in files.items():
            zf.writestr(path, data)
    return wheel_path


def make_sdist(name: str, version: str, dest_dir: Path) -> Path:
    """A minimal but valid sdist tarball (PKG-INFO only)."""
    import io
    import tarfile

    base = f"{name}-{version}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / f"{base}.tar.gz"
    pkg_info = (f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n").encode()
    with tarfile.open(path, "w:gz") as tf:
        info = tarfile.TarInfo(f"{base}/PKG-INFO")
        info.size = len(pkg_info)
        tf.addfile(info, io.BytesIO(pkg_info))
    return path


# ------------------------------ uv utilities ---------------------------------


def uv_python_path(venv_dir: Path) -> Path:
    if platform.system().lower().startswith("win"):
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


# -------------------------- Legacy upload (multipart) -------------------------


def _encode_basic_auth(user: str, password: str) -> str:
    token = f"{user}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(token).decode("ascii")


def parse_dist_filename(filename: str) -> Tuple[str, str]:
    """Best-effort (name, version) from a wheel/sdist filename."""
    if filename.endswith(".whl"):
        parts = filename[: -len(".whl")].split("-")
        return parts[0], parts[1]
    for suffix in (".tar.gz", ".tar.bz2", ".tar.xz", ".zip"):
        if filename.endswith(suffix):
            stem = filename[: -len(suffix)]
            name, _, version = stem.rpartition("-")
            return name, version
    raise ValueError(f"Unrecognized distribution filename: {filename}")


def upload_legacy(
    legacy_url: str,
    wheel_path: Path,
    *,
    username: Optional[str] = None,
    password: Optional[str] = None,
    fields: Optional[Dict[str, str]] = None,
    timeout: float = 30.0,
    expect_status: int = 200,
    follow_redirects: bool = True,
) -> Tuple[int, bytes]:
    """POST multipart/form-data to /legacy the way twine does.

    Sends the standard metadata fields (:action, name, version, sha256_digest)
    plus the file in field "content". `fields` overrides/extends the defaults.
    Returns (status, body); raises if status != expect_status. Set
    `follow_redirects=False` to observe the server's raw status (urllib would
    otherwise transparently chase a 3xx and mask it).
    """
    filename = wheel_path.name
    file_bytes = wheel_path.read_bytes()
    # Legacy binary formats (.egg/.exe/.msi) carry no parseable name/version; a
    # caller publishing one supplies both in `fields`, which would override the
    # derived pair anyway.
    if fields and "name" in fields and "version" in fields:
        name, version = fields["name"], fields["version"]
    else:
        name, version = parse_dist_filename(filename)

    form: Dict[str, str] = {
        ":action": "file_upload",
        "protocol_version": "1",
        "name": name,
        "version": version,
        "sha256_digest": hashlib.sha256(file_bytes).hexdigest(),
    }
    if fields:
        form.update(fields)

    boundary = f"------------------------{uuid.uuid4().hex}"
    crlf = "\r\n"
    parts: list[bytes] = []

    for key, value in form.items():
        parts.append(
            (
                f"--{boundary}{crlf}"
                f'Content-Disposition: form-data; name="{key}"{crlf}{crlf}'
                f"{value}{crlf}"
            ).encode("utf-8")
        )

    parts.append(
        (
            f"--{boundary}{crlf}"
            f'Content-Disposition: form-data; name="content"; filename="{filename}"{crlf}'
            f"Content-Type: application/octet-stream{crlf}{crlf}"
        ).encode("utf-8")
    )
    parts.append(file_bytes)
    parts.append(crlf.encode("utf-8"))
    parts.append((f"--{boundary}--{crlf}").encode("utf-8"))

    body = b"".join(parts)
    hdrs = {"Content-Type": f"multipart/form-data; boundary={boundary}"}
    if username and password:
        hdrs["Authorization"] = _encode_basic_auth(username, password)

    if follow_redirects:
        code, resp_body, _ = _http_request(
            legacy_url, method="POST", headers=hdrs, data=body, timeout=timeout
        )
    else:
        code, resp_body, _ = _post_no_redirect(legacy_url, headers=hdrs, data=body, timeout=timeout)
    if code != expect_status:
        raise RuntimeError(
            f"Upload returned {code}, expected {expect_status}: {resp_body.decode('utf-8', 'replace')}"
        )
    return code, resp_body


# ----------------------------- Binary path helper ----------------------------


def pypiron_binary_path(repo_root: Path, *, release: bool = False) -> Path:
    exe = "pypiron.exe" if platform.system().lower().startswith("win") else "pypiron"
    profile = "release" if release else "debug"
    return repo_root / "target" / profile / exe


def ensure_built(repo_root: Path, *, release: bool = False) -> Path:
    """Build the binary with cargo. Always invoked: incremental builds make this a
    cheap no-op when fresh, and skipping it would silently test a stale binary."""
    args = ["cargo", "build"]
    if release:
        args.append("--release")
    run_checked(args, cwd=repo_root, timeout=600)
    bin_path = pypiron_binary_path(repo_root, release=release)
    if not bin_path.exists():
        raise FileNotFoundError(f"Did not find built binary at {bin_path}")
    return bin_path


# ------------------------------ Perf harness ---------------------------------


def bench_endpoint(url: str, *, duration: float = 3.0, concurrency: int = 16) -> Dict[str, float]:
    """Hammer a URL with persistent connections from threads.

    Returns request count, RPS, and latency percentiles in ms. The Python client
    is the bottleneck, so treat results as comparative (before/after a change),
    not as absolute server capacity.
    """
    import http.client
    from concurrent.futures import ThreadPoolExecutor
    from urllib.parse import urlparse

    parsed = urlparse(url)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    deadline = time.time() + duration

    def worker(_: int) -> list:
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=10)
        latencies = []
        try:
            while time.time() < deadline:
                t0 = time.perf_counter()
                conn.request("GET", path)
                resp = conn.getresponse()
                resp.read()
                if resp.status != 200:
                    raise RuntimeError(f"GET {url} returned {resp.status}")
                latencies.append((time.perf_counter() - t0) * 1000.0)
        finally:
            conn.close()
        return latencies

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        all_latencies = [lat for chunk in pool.map(worker, range(concurrency)) for lat in chunk]

    all_latencies.sort()
    n = len(all_latencies)

    def pct(p: float) -> float:
        if not n:
            return 0.0
        return all_latencies[min(n - 1, int(n * p))]

    return {
        "requests": n,
        "rps": n / duration,
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
    }
