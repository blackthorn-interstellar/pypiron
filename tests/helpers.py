from __future__ import annotations

import base64
import hashlib
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


ACCEPT_PEP691 = "application/vnd.pypi.simple.v1+json"


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


def find_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


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
        return e.code, e.read() if e.fp else b"", {k.lower(): v for k, v in (e.headers or {}).items()}
    except URLError as e:
        raise ConnectionError(f"HTTP request failed to {url}: {e}") from e


def http_get(
    url: str, *, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0
) -> Tuple[int, bytes, Dict[str, str]]:
    """GET returning (status, body, headers); does not raise on non-2xx."""
    return _http_request(url, method="GET", headers=headers, timeout=timeout)


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


def http_get_bytes(url: str, *, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0) -> bytes:
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


def wait_http_ok(url: str, *, timeout: float = 15.0, interval: float = 0.1) -> None:
    """Poll until GET returns 2xx or timeout."""
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            code, _, _ = _http_request(url)
            if 200 <= code < 300:
                return
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(interval)
    if last_err:
        raise TimeoutError(f"Timed out waiting for {url}: last error: {last_err}")
    raise TimeoutError(f"Timed out waiting for {url}")


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


def wait_for_file_in_index(
    simple_url: str, package: str, filename: str, *, timeout: float = 30.0
) -> dict:
    """Poll the PEP 691 package index until `filename` appears; return the index doc."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            data = http_get_json(
                f"{simple_url}{package}/index.json", headers={"Accept": ACCEPT_PEP691}
            )
            if filename in [f.get("filename") for f in data.get("files", [])]:
                return data
        except (RuntimeError, ConnectionError):
            pass
        time.sleep(0.2)
    raise TimeoutError(f"{filename} did not appear in index for {package} within {timeout}s")


# ------------------------------ File / Hashing --------------------------------


def sha256_file(path: Path) -> str:
    m = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            m.update(chunk)
    return m.hexdigest()


# ------------------------------ uv utilities ---------------------------------


def uv_python_path(venv_dir: Path) -> Path:
    if platform.system().lower().startswith("win"):
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def make_uv_venv(uv: str, venv_dir: Path, *, seed: bool = False) -> Path:
    """Create a venv with uv and return its python. seed=True installs pip into it."""
    args = [uv, "venv"]
    if seed:
        args.append("--seed")
    args.append(str(venv_dir))
    run_checked(args)
    py = uv_python_path(venv_dir)
    if not py.exists():
        raise FileNotFoundError(f"uv venv python not found at {py}")
    return py


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
) -> Tuple[int, bytes]:
    """POST multipart/form-data to /legacy the way twine does.

    Sends the standard metadata fields (:action, name, version, sha256_digest)
    plus the file in field "content". `fields` overrides/extends the defaults.
    Returns (status, body); raises if status != expect_status.
    """
    filename = wheel_path.name
    file_bytes = wheel_path.read_bytes()
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

    code, resp_body, _ = _http_request(legacy_url, method="POST", headers=hdrs, data=body, timeout=timeout)
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