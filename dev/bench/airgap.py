#!/usr/bin/env python3
"""Offline first-time bulk sync benchmark: pypiron (disk) -> pypiron (MinIO S3).

Models `pypiron sync --from https://pypi.org --to <fresh pypiron on S3>` at
~1/60th scale with the same file-size mix (mean ~6 MB, long tail to 200 MB),
entirely on localhost. Every run gets a fresh bucket and a fresh destination
server; the source corpus is seeded once and cached under .local/airgap/src.

    uv run -- python dev/bench/airgap.py --bin .local/airgap/bin/pypiron-baseline \
        --label baseline-a

Records wall, files/s, MB/s, errors, peak RSS + CPU of client and server
(`/usr/bin/time -l`, macOS), a 1 s server RSS timeline, MinIO CPU, and index
lag (sync end -> every synced file listed in /simple/<pkg>/). Writes
dev/bench/results/airgap-<label>.json. Absolute numbers are this Mac's; use
them for before/after comparison only.

Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fabricate import sample_projects  # noqa: E402
from meter import make_wheel_bytes, upload_wheel, wait_healthy, wheel_filename  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
AG = ROOT / ".local" / "airgap"
RESULTS = ROOT / "dev" / "bench" / "results"
SRC_PORT, DST_PORT, MINIO_PORT = 18081, 18082, 19000
MINIO = "pypiron-bench-minio"
MINIO_IMAGE = "quay.io/minio/minio"
# Colima's /var/lib/docker disk is nearly full here; back the named volume with
# the VM's root disk instead (a VM-local path, so the bind works).
MINIO_VM_DIR = "/var/pypiron-bench-minio"
ADMIN = ("admin", "secret")
UPLOADER = ("uploader", "uploadersecret")
PACKAGES = 150
PER_PROJECT_WEIGHT_CAP = 200
TORCH_PKG = "airgap-torchsim"
TORCH_FILES, TORCH_MB = 2, 900
JSON_ACCEPT = {"Accept": "application/vnd.pypi.simple.v1+json"}
INDEX_LAG_CAP = 120.0

# (fraction of files, min bytes, max bytes). ~1500 files -> ~8.5 GB, mean ~5.8 MB.
MIX = [
    (800 / 1500, 50_000, 500_000),
    (450 / 1500, 500_000, 5_000_000),
    (200 / 1500, 5_000_000, 30_000_000),
    (45 / 1500, 30_000_000, 100_000_000),
    (5 / 1500, 100_000_000, 200_000_000),
]


def log(msg: str) -> None:
    print(f"[airgap {time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


# ---------------------------------- corpus -----------------------------------


def corpus_plan(files: int, seed: int) -> list[dict]:
    """Deterministic (project, version, size) list; every project gets >= 1 file."""
    rng = random.Random(seed)
    projects = sample_projects(min(PACKAGES, files), seed=seed, include_largest=True)
    names = [n for n, _ in projects]
    weights = [min(c, PER_PROJECT_WEIGHT_CAP) for _, c in projects]
    owners = names + rng.choices(names, weights=weights, k=files - len(names))
    counts = [round(files * frac) for frac, _, _ in MIX]
    counts[0] += files - sum(counts)
    sizes = [rng.randint(lo, hi) for n, (_, lo, hi) in zip(counts, MIX) for _ in range(n)]
    rng.shuffle(sizes)
    per_pkg: dict[str, int] = {}
    plan = []
    for name, size in zip(owners, sizes):
        i = per_pkg[name] = per_pkg.get(name, 0) + 1
        plan.append({"pkg": name, "version": f"1.{i}.0", "size": size})
    return plan


def torch_plan() -> list[dict]:
    return [
        {"pkg": TORCH_PKG, "version": f"2.{i}.0", "size": TORCH_MB * 1_000_000}
        for i in range(1, TORCH_FILES + 1)
    ]


def seed_entries(base: str, entries: list[dict], workers: int = 6) -> list[dict]:
    """Upload each entry through /legacy/; returns entries with real byte sizes."""

    def one(e: dict) -> dict:
        body = make_wheel_bytes(e["pkg"], e["version"], e["size"])
        fn = wheel_filename(e["pkg"], e["version"])
        status, resp = upload_wheel(f"{base}/legacy/", fn, body, e["pkg"], e["version"], *UPLOADER)
        if status != 200:
            raise RuntimeError(f"seed upload {fn} -> {status}: {resp[:200]!r}")
        return {**e, "filename": fn, "bytes": len(body)}

    out = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i, e in enumerate(pool.map(one, entries), 1):
            out.append(e)
            if i % 100 == 0:
                log(f"seeded {i}/{len(entries)}")
    return out


def expected_counts(entries: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for e in entries:
        counts[e["pkg"]] = counts.get(e["pkg"], 0) + 1
    return counts


# --------------------------------- processes ---------------------------------


def clean_env(extra: dict[str, str]) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("PYPIRON_", "AWS_"))}
    env.update(extra)
    return env


def child_pid(parent: int, timeout: float = 10.0) -> int:
    """The pid `/usr/bin/time` forked (signals and RSS go to it, not to time)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = subprocess.run(["pgrep", "-P", str(parent)], capture_output=True, text=True).stdout
        if out.strip():
            return int(out.split()[0])
        time.sleep(0.05)
    raise RuntimeError(f"no child of pid {parent}")


def parse_time_l(text: str) -> dict:
    """macOS `/usr/bin/time -l` report: wall/user/sys seconds, peak RSS bytes."""
    m = re.search(r"([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys", text)
    rss = re.search(r"(\d+)\s+maximum resident set size", text)
    if not (m and rss):
        return {}
    return {
        "wall_s": float(m[1]),
        "user_s": float(m[2]),
        "sys_s": float(m[3]),
        "peak_rss_mb": round(int(rss[1]) / 1e6, 1),
    }


def rss_kb(pid: int) -> int | None:
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
    return int(out.stdout) if out.stdout.strip() else None


class Sampler(threading.Thread):
    """Once a second: server RSS (ps) and MinIO CPU% (docker stats)."""

    def __init__(self, pid: int) -> None:
        super().__init__(daemon=True)
        self.pid, self.t0 = pid, time.time()
        self.rss: list[tuple[float, float]] = []
        self.minio_cpu: list[float] = []
        self.stop = threading.Event()
        threading.Thread(target=self._minio, daemon=True).start()

    def run(self) -> None:
        while not self.stop.wait(1.0):
            kb = rss_kb(self.pid)
            if kb is not None:
                self.rss.append((round(time.time() - self.t0, 1), round(kb / 1000, 1)))

    def _minio(self) -> None:
        while not self.stop.is_set():
            out = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", MINIO],
                capture_output=True,
                text=True,
            ).stdout.strip()
            try:
                self.minio_cpu.append(float(out.rstrip("%")))
            except ValueError:
                pass
            self.stop.wait(1.0)


def stop_proc(proc: subprocess.Popen, pid: int | None = None, timeout: float = 60.0) -> None:
    """SIGTERM (to the timed child when wrapped) and wait for a graceful exit."""
    if proc.poll() is None:
        try:
            os.kill(pid or proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            log(f"pid {pid or proc.pid} ignored SIGTERM for {timeout}s; killing")
            proc.kill()
            proc.wait()


def start_source(bin_path: Path, data: Path, logf: Path) -> subprocess.Popen:
    (AG / "src-spool").mkdir(parents=True, exist_ok=True)
    env = clean_env(
        {
            "PYPIRON_DATA_DIR": str(data),
            "PYPIRON_BIND_ADDR": f"127.0.0.1:{SRC_PORT}",
            "PYPIRON_ADMIN_USER": ADMIN[0],
            "PYPIRON_ADMIN_PASS": ADMIN[1],
            "PYPIRON_UPLOADER_USER": UPLOADER[0],
            "PYPIRON_UPLOADER_PASS": UPLOADER[1],
            "PYPIRON_SPOOL_DIR": str(AG / "src-spool"),
        }
    )
    with open(logf, "ab") as fh:
        proc = subprocess.Popen([str(bin_path), "serve"], env=env, stdout=fh, stderr=fh)
    wait_healthy(f"http://127.0.0.1:{SRC_PORT}", timeout=120)
    return proc


# ----------------------------------- MinIO -----------------------------------


def docker(*args: str, check: bool = True) -> str:
    cp = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and cp.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)}: {cp.stderr.strip()}")
    return cp.stdout.strip()


def ensure_minio() -> None:
    state = docker("inspect", "-f", "{{.State.Running}}", MINIO, check=False)
    if state != "true":
        if state == "false":
            docker("start", MINIO)
        else:
            if not docker("volume", "ls", "-q", "-f", f"name=^{MINIO}$"):
                subprocess.run(
                    ["colima", "ssh", "--", "sudo", "mkdir", "-p", MINIO_VM_DIR], check=True
                )
                docker(
                    "volume", "create", "--driver", "local", "--opt", "type=none",
                    "--opt", "o=bind", "--opt", f"device={MINIO_VM_DIR}", MINIO,
                )  # fmt: skip
            docker(
                "run", "-d", "--name", MINIO, "-p", f"127.0.0.1:{MINIO_PORT}:9000",
                "-e", "MINIO_ROOT_USER=minioadmin", "-e", "MINIO_ROOT_PASSWORD=minioadmin",
                "-v", f"{MINIO}:/data", MINIO_IMAGE, "server", "/data",
            )  # fmt: skip
    deadline = time.time() + 60
    while True:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{MINIO_PORT}/minio/health/ready", timeout=2
            ):
                break
        except (urllib.error.URLError, OSError):
            if time.time() > deadline:
                raise RuntimeError("MinIO not ready after 60s") from None
            time.sleep(0.5)
    docker("exec", MINIO, "mc", "alias", "set", "l", "http://127.0.0.1:9000", "minioadmin",
           "minioadmin")  # fmt: skip


def mc(*args: str) -> str:
    return docker("exec", MINIO, "mc", *args)


def wait_minio_space(need: int, timeout: float = 600.0) -> None:
    """MinIO frees a removed bucket lazily; starting the next run before that
    lands fills the disk and turns uploads into 503s (XMinioStorageFull)."""
    deadline = time.time() + timeout
    while True:
        avail = int(docker("exec", MINIO, "df", "-k", "/data").splitlines()[-1].split()[3]) * 1024
        if avail >= need:
            return
        if time.time() > deadline:
            raise RuntimeError(f"MinIO has {avail / 1e9:.1f} GB free, need {need / 1e9:.1f} GB")
        time.sleep(2)


# ------------------------------- index checking -------------------------------


def listed(base: str, pkg: str) -> int:
    req = urllib.request.Request(f"{base}/simple/{pkg}/", headers=JSON_ACCEPT)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return len(json.load(resp).get("files", []))
    except urllib.error.HTTPError:
        return 0


def wait_indexed(base: str, want: dict[str, int], cap: float) -> tuple[float | None, int]:
    """Seconds until every package lists its expected file count (None on cap)."""
    t0 = time.perf_counter()
    pending = dict(want)
    have: dict[str, int] = {}
    while True:
        for pkg in list(pending):
            have[pkg] = listed(base, pkg)
            if have[pkg] >= pending[pkg]:
                del pending[pkg]
        if not pending:
            return round(time.perf_counter() - t0, 2), sum(have.values())
        if time.perf_counter() - t0 > cap:
            return None, sum(have.values())
        time.sleep(0.25)


# ------------------------------------ main ------------------------------------


def ensure_corpus(bin_path: Path, files: int, seed: int, torch: bool) -> tuple[Path, list[dict]]:
    key = AG / "src" / f"s{seed}-f{files}"
    data = key / "data"
    data.mkdir(parents=True, exist_ok=True)
    parts = [("manifest.json", lambda: corpus_plan(files, seed))]
    if torch:
        parts.append(("manifest-torch.json", torch_plan))
    missing = [(m, plan) for m, plan in parts if not (key / m).exists()]
    if missing:
        proc = start_source(bin_path, data, key / "seed.log")
        try:
            for m, plan in missing:
                entries = plan()
                total = sum(e["size"] for e in entries) / 1e9
                log(f"seeding {m}: {len(entries)} files, ~{total:.1f} GB")
                done = seed_entries(f"http://127.0.0.1:{SRC_PORT}", entries)
                lag, _ = wait_indexed(f"http://127.0.0.1:{SRC_PORT}", expected_counts(done), 600)
                if lag is None:
                    raise RuntimeError("source index never caught up after seeding")
                (key / m).write_text(json.dumps(done))
        finally:
            stop_proc(proc)
    entries = [e for m, _ in parts for e in json.loads((key / m).read_text())]
    return data, entries


def parse_sync_done(text: str) -> dict:
    m = re.search(
        r"sync done: .*?· ([\d,]+) files · .*?· (\d+) already present · (\d+) errors", text
    )
    if not m:
        return {}
    return {
        "files": int(m[1].replace(",", "")),
        "skipped": int(m[2]),
        "errors": int(m[3]),
        "line": m[0],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--bin", required=True, type=Path, help="pypiron binary under test")
    ap.add_argument("--label", default="run")
    ap.add_argument("--concurrency", type=int)
    ap.add_argument("--package-concurrency", type=int)
    ap.add_argument("--server-env", action="append", default=[], metavar="K=V")
    ap.add_argument("--sync-arg", action="append", default=[], help="extra sync argument")
    ap.add_argument("--files", type=int, default=1500, help="corpus size (default 1500)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--torch", action="store_true", help="add 2 x 900 MB wheels")
    ap.add_argument("--seed-bin", type=Path, help="binary for the source server (default --bin)")
    a = ap.parse_args()

    bin_path = a.bin.resolve()
    src_bin = (a.seed_bin or a.bin).resolve()
    ensure_minio()
    data, entries = ensure_corpus(src_bin, a.files, a.seed, a.torch)
    want = expected_counts(entries)
    want_bytes = sum(e["bytes"] for e in entries)

    run_id = f"{re.sub(r'[^a-z0-9-]', '-', a.label.lower())}-{int(time.time())}"[-50:]
    bucket = f"airgap-{run_id}".strip("-")
    rundir = AG / "runs" / run_id
    rundir.mkdir(parents=True)
    srv_spool, cli_spool = AG / "spool" / run_id, AG / "sync-spool" / run_id
    srv_spool.mkdir(parents=True)
    cli_spool.mkdir(parents=True)
    (rundir / "packages.txt").write_text("\n".join(sorted(want)) + "\n")
    wait_minio_space(int(want_bytes * 1.2) + 2_000_000_000)
    mc("mb", f"l/{bucket}")

    src = dst = sampler = None
    dst_pid = None
    try:
        src = start_source(src_bin, data, rundir / "source.log")
        senv = {
            "PYPIRON_BUCKETS": f"s3://{bucket}",
            "PYPIRON_S3_ENDPOINT_URL": f"http://127.0.0.1:{MINIO_PORT}",
            "PYPIRON_S3_FORCE_PATH_STYLE": "true",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_REGION": "us-east-1",
            "PYPIRON_BIND_ADDR": f"127.0.0.1:{DST_PORT}",
            "PYPIRON_ADMIN_USER": ADMIN[0],
            "PYPIRON_ADMIN_PASS": ADMIN[1],
            "PYPIRON_SPOOL_DIR": str(srv_spool),
        }
        senv.update(kv.split("=", 1) for kv in a.server_env)
        with open(rundir / "server.log", "wb") as fh:
            dst = subprocess.Popen(
                ["/usr/bin/time", "-l", str(bin_path), "serve"],
                env=clean_env(senv),
                stdout=fh,
                stderr=fh,
            )
        dst_pid = child_pid(dst.pid)
        dst_base = f"http://127.0.0.1:{DST_PORT}"
        wait_healthy(dst_base, timeout=120)
        sampler = Sampler(dst_pid)
        sampler.start()

        cmd = [
            "/usr/bin/time", "-l", str(bin_path), "sync",
            "--from", f"http://127.0.0.1:{SRC_PORT}", "--to", dst_base,
            "--admin-user", ADMIN[0], "--admin-pass", ADMIN[1],
            "--include-packages-from", str(rundir / "packages.txt"),
            "--exclude-newer", "", "--no-progress", "--spool-dir", str(cli_spool),
        ]  # fmt: skip
        if a.concurrency is not None:
            cmd += ["--concurrency", str(a.concurrency)]
        if a.package_concurrency is not None:
            cmd += ["--package-concurrency", str(a.package_concurrency)]
        cmd += a.sync_arg
        log(f"sync {len(entries)} files / {want_bytes / 1e9:.2f} GB -> s3://{bucket}")
        t0 = time.perf_counter()
        cp = subprocess.run(cmd, env=clean_env({}), capture_output=True, text=True)
        wall = time.perf_counter() - t0
        (rundir / "sync.log").write_text(cp.stdout + cp.stderr)
        index_lag, indexed = wait_indexed(dst_base, want, INDEX_LAG_CAP)
    finally:
        if sampler:
            sampler.stop.set()
        if dst:
            stop_proc(dst, dst_pid)
        if src:
            stop_proc(src)
        docker("exec", MINIO, "mc", "rb", "--force", f"l/{bucket}", check=False)
        shutil.rmtree(srv_spool, ignore_errors=True)
        shutil.rmtree(cli_spool, ignore_errors=True)

    done = parse_sync_done(cp.stdout + cp.stderr)
    client = parse_time_l(cp.stderr)
    server = parse_time_l((rundir / "server.log").read_text(errors="replace"))
    files = done.get("files", 0)
    problems = []
    if cp.returncode != 0:
        problems.append(f"sync exit {cp.returncode}")
    if files != len(entries):
        problems.append(f"synced {files} files, expected {len(entries)}")
    if done.get("errors", 1) != 0:
        problems.append(f"{done.get('errors', '?')} sync errors")
    if index_lag is None:
        problems.append(f"index incomplete after {INDEX_LAG_CAP:.0f}s ({indexed}/{len(entries)})")
    cpu = sampler.minio_cpu if sampler else []
    result = {
        "label": a.label,
        "bin": str(bin_path),
        "when": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "config": {
            "concurrency": a.concurrency,
            "package_concurrency": a.package_concurrency,
            "server_env": a.server_env,
            "sync_args": a.sync_arg,
            "files": a.files,
            "seed": a.seed,
            "torch": a.torch,
        },
        "expected_files": len(entries),
        "expected_bytes": want_bytes,
        "packages": len(want),
        "ok": not problems,
        "problems": problems,
        "wall_s": round(wall, 2),
        "files_per_s": round(files / wall, 2),
        "mb_per_s": round(want_bytes / 1e6 / wall, 1) if files == len(entries) else None,
        "sync": done,
        "client": client,
        "server": server,
        "server_rss_timeline_max_mb": max((r for _, r in sampler.rss), default=None)
        if sampler
        else None,
        "server_rss_timeline": sampler.rss if sampler else [],
        "minio_cpu_max_pct": max(cpu, default=None),
        "minio_cpu_mean_pct": round(sum(cpu) / len(cpu), 1) if cpu else None,
        "index_lag_s": index_lag,
        "indexed_files": indexed,
        "rundir": str(rundir),
    }
    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / f"airgap-{a.label}.json"
    out.write_text(json.dumps(result, indent=1) + "\n")
    print(
        f"{a.label}: {'OK' if result['ok'] else 'FAIL ' + '; '.join(problems)} | "
        f"{files} files {want_bytes / 1e9:.2f} GB in {wall:.1f}s = "
        f"{result['files_per_s']} files/s {result['mb_per_s']} MB/s | "
        f"client rss {client.get('peak_rss_mb')} MB cpu "
        f"{client.get('user_s', 0) + client.get('sys_s', 0):.0f}s | "
        f"server rss {server.get('peak_rss_mb')} MB cpu "
        f"{server.get('user_s', 0) + server.get('sys_s', 0):.0f}s | "
        f"minio cpu max {result['minio_cpu_max_pct']}% | index lag {index_lag}s -> {out}"
    )
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
