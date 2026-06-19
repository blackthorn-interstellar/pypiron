#!/usr/bin/env python3
"""Orchestrate one server-under-test through a scenario: up -> seed -> warm ->
drive -> emit -> teardown.

Runs on the host; drives docker compose. The loadgen container runs the uv client
(seed.py, drive.py) against the server on the bench network. The same compose
stacks run locally (validation) and on the AWS rig (citable numbers).

  bench.py --server pypiron --tier lite --arch aarch64 --concurrency 1,8 --samples 16
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.request

from benchlib import COMPOSE, HERE, RESULTS, manifest_path, wheelhouse_dir

OHA_VERSION = "v1.4.7"


def ensure_oha(arch: str) -> None:
    """Download a static oha binary (matching the loadgen container arch) into
    bench/install/.bin/oha (bind-mounted into the loadgen at /repo). Idempotent."""
    binary = HERE / ".bin" / "oha"
    if binary.exists():
        return
    binary.parent.mkdir(exist_ok=True)
    suffix = {"x86_64": "amd64", "aarch64": "arm64"}[arch]
    url = f"https://github.com/hatoo/oha/releases/download/{OHA_VERSION}/oha-linux-{suffix}"
    print(f"-- fetching oha {OHA_VERSION} ({suffix})")
    urllib.request.urlretrieve(url, binary)
    binary.chmod(0o755)


# Per-server config (data-driven — a competitor is a compose overlay + an entry):
#   overlay         compose file under compose/
#   index_path      PEP 503 index root the client points at
#   host            host:port on the bench network
#   seed            how the corpus gets in: upload | copy | warm | mirror
#   egress_service  service detached from egress before measuring (warm), so any
#                   cache miss fails loudly; None if it never needs egress
#   service         main server service (for copy/restart ops)
#   copy_target     dir to copy the wheelhouse into (copy seed)
#   mirror_service  one-shot batch service to run (mirror seed)
SERVERS = {
    "pypiron": {
        "overlay": "docker-compose.pypiron.yml",
        "overlay_t2": "docker-compose.pypiron-s3.yml",  # Track 2: S3 + presigned redirect
        "t2_needs_egress": True,  # client follows 302 to S3
        "index_path": "/simple/",
        "host": "pypiron:8080",
        "seed": "upload",
        "egress_service": None,
        "service": "pypiron",
    },
    "devpi": {
        "overlay": "docker-compose.devpi.yml",
        "index_path": "/root/pypi/+simple/",
        "host": "nginx:8080",
        "seed": "warm",
        "egress_service": "devpi",
        "service": "devpi",
    },
    "pypiserver": {
        "overlay": "docker-compose.pypiserver.yml",
        "index_path": "/simple/",
        "host": "pypiserver:8080",
        "seed": "copy",
        "egress_service": None,
        "service": "pypiserver",
        "copy_target": "/data/packages",
    },
    "proxpi": {
        "overlay": "docker-compose.proxpi.yml",
        "index_path": "/index/",
        "host": "proxpi:5000",
        "seed": "warm",
        "egress_service": "proxpi",
        "service": "proxpi",
    },
    "pypicloud": {
        "overlay": "docker-compose.pypicloud.yml",
        "overlay_t2": "docker-compose.pypicloud-dynamo.yml",  # Track 2: S3 + DynamoDB
        "t2_needs_egress": True,  # serves blobs from S3 + talks to DynamoDB live
        "index_path": "/simple/",
        "host": "pypicloud:8080",
        "seed": "warm",
        "egress_service": "pypicloud",
        "service": "pypicloud",
        # pypicloud (archived) cache-mode can't serve a version different from the
        # one a dependency first cached (e.g. redis 8.0.0 after celery[redis] cached
        # an earlier redis). Tolerate that documented gap rather than abort.
        "warm_min_ok": 0.95,
    },
    "bandersnatch": {
        "overlay": "docker-compose.bandersnatch.yml",
        "index_path": "/simple/",
        "host": "web:8080",
        "seed": "mirror",
        "egress_service": None,
        "service": "web",
        "mirror_service": "bandersnatch",
    },
}

PLATFORM = {"aarch64": "linux/arm64", "x86_64": "linux/amd64"}


def gen_bandersnatch_conf(tier: str, arch: str) -> None:
    """Render compose/bandersnatch.gen.conf: a release-level allowlist of exactly
    the pinned (name==version) set, so the mirror downloads only the corpus
    versions (the allowlist does NOT resolve deps — we feed the full closure)."""
    manifest = json.loads(manifest_path(tier, arch).read_text())
    pins = sorted({f"{w['name']}=={w['version']}" for w in manifest["wheels"]})
    body = "\n".join(f"    {p}" for p in pins)
    conf = f"""\
[mirror]
directory = /data/pypi
master = https://pypi.org
workers = 4
timeout = 30
storage-backend = filesystem
simple-format = ALL
json = true
release-files = true

[plugins]
enabled =
    allowlist_project
    allowlist_release

[allowlist]
packages =
{body}
"""
    (COMPOSE / "bandersnatch.gen.conf").write_text(conf)


def gen_pypicloud_dynamo_conf() -> None:
    """Render pypicloud-config-dynamo.gen.ini, substituting the rig bucket/region
    (pypicloud's ini does no env interpolation). Track 2, AWS-only."""
    bucket = os.environ.get("PYPIRON_S3_BUCKET")
    region = os.environ.get("AWS_REGION", "us-east-1")
    if not bucket:
        raise SystemExit("Track 2 pypicloud needs PYPIRON_S3_BUCKET set (the rig bucket)")
    tmpl = (COMPOSE / "pypicloud-config-dynamo.ini").read_text()
    out = tmpl.replace("__BUCKET__", bucket).replace("__REGION__", region)
    (COMPOSE / "pypicloud-config-dynamo.gen.ini").write_text(out)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--server", required=True, choices=list(SERVERS))
    ap.add_argument("--tier", default="lite")
    ap.add_argument("--arch", default="x86_64", choices=list(PLATFORM))
    ap.add_argument("--track", type=int, default=1, choices=[1, 2])
    ap.add_argument("--scenario", default="S1")
    ap.add_argument("--concurrency", default="1,8,32")
    ap.add_argument("--samples", type=int, default=24)
    ap.add_argument("--sampling", default="uniform", choices=["uniform", "zipf"])
    ap.add_argument("--python", default="3.11")
    ap.add_argument(
        "--capacity", action="store_true", help="run the oha index-read breaking-point ramp"
    )
    ap.add_argument(
        "--install-mix", action="store_true", help="run the oha install-mix ramp (installs/sec)"
    )
    ap.add_argument("--no-drive", action="store_true", help="skip the uv install sweep")
    ap.add_argument(
        "--rep-pkg", default="flask", help="representative package for the capacity ramp"
    )
    ap.add_argument("--keep", action="store_true", help="don't tear down after the run")
    args = ap.parse_args()

    spec = SERVERS[args.server]
    project = f"ibench-{args.server}"
    RESULTS.mkdir(parents=True, exist_ok=True)
    out_name = f"{args.scenario}-{args.server}-track{args.track}-{args.tier}-{args.arch}.json"

    # Track 2 = each server in its best-production config. pypiron (S3+redirect)
    # and pypicloud (S3+DynamoDB) get distinct overlays and need LIVE egress (the
    # client follows pypiron's 302 to S3; pypicloud serves blobs from S3 + talks
    # to DynamoDB), so they are not egress-blocked and we don't cut/​sanity them.
    # The other four have no cloud-offload path — their Track 1 config IS their
    # optimal, so Track 2 runs them identically (egress-blocked, cut after warm).
    overlay = spec.get("overlay_t2", spec["overlay"]) if args.track == 2 else spec["overlay"]
    needs_egress = args.track == 2 and spec.get("t2_needs_egress", False)
    env = {
        **os.environ,
        "BENCH_PLATFORM": PLATFORM[args.arch],
        "BENCH_ARCH": args.arch,
        "BENCH_TIER": args.tier,
        "BENCH_INTERNAL": "false" if needs_egress else "true",
    }
    base = ["docker", "compose", "-p", project, "-f", "docker-compose.base.yml", "-f", overlay]

    def dc(*a: str, **kw):
        return subprocess.run([*base, *a], cwd=COMPOSE, env=env, **kw)

    def exec_loadgen(*a: str, check=True):
        return dc("exec", "-T", "loadgen", *a, check=check)

    index_url = f"http://{spec['host']}{spec['index_path']}"

    def drive(*extra: str, mode: str = "measure"):
        exec_loadgen(
            "python3",
            "drive.py",
            "--mode",
            mode,
            "--index-url",
            index_url,
            "--host",
            spec["host"],
            "--tier",
            args.tier,
            "--arch",
            args.arch,
            "--python",
            args.python,
            "--label",
            args.server,
            "--warm-min-ok",
            str(spec.get("warm_min_ok", 1.0)),
            *extra,
        )

    def cut_egress():
        cid = dc(
            "ps", "-q", spec["egress_service"], check=True, capture_output=True, text=True
        ).stdout.strip()
        net = f"{project}_egress"
        print(f"-- cut egress ({net} -x-> {spec['egress_service']})")
        subprocess.run(["docker", "network", "disconnect", net, cid], check=True)

    print(f"== {args.server} / {args.scenario} / track {args.track} / {args.tier} / {args.arch}")
    if spec["seed"] == "mirror":
        gen_bandersnatch_conf(args.tier, args.arch)
    if args.track == 2 and args.server == "pypicloud":
        gen_pypicloud_dynamo_conf()
    t0 = time.time()
    try:
        print("-- up")
        dc("up", "-d", check=True)

        if spec["seed"] == "upload":
            print("-- seed (upload wheelhouse)")
            exec_loadgen(
                "python3",
                "seed.py",
                "--server",
                args.server,
                "--base-url",
                f"http://{spec['host']}",
                "--tier",
                args.tier,
                "--arch",
                args.arch,
            )
        elif spec["seed"] == "copy":
            src = wheelhouse_dir(args.tier, args.arch)
            n = len(list(src.glob("*.whl")))
            if not n:
                raise SystemExit(f"empty wheelhouse {src}; run wheelhouse.py first")
            print(f"-- seed (copy {n} wheels -> {spec['service']}:{spec['copy_target']})")
            dc("cp", f"{src}/.", f"{spec['service']}:{spec['copy_target']}/", check=True)
            dc("restart", spec["service"], check=True)  # rescan the now-populated dir
        elif spec["seed"] == "warm":
            print("-- warm (install corpus, egress on)")
            drive(mode="warm")
            # Lazy proxies persist cached files asynchronously; a second egress-on
            # pass (all cache hits) forces any straggler to be stored before we
            # sever upstream, so the offline sanity is clean.
            print("-- warm confirm (persist stragglers, egress on)")
            drive(mode="warm")
            if needs_egress:
                # S3-backed Track 2 (pypicloud-dynamo) serves blobs from S3 live —
                # egress must stay, so there is no cut/offline-sanity phase.
                print("-- (S3-backed: egress stays; no offline sanity)")
            else:
                cut_egress()
                print("-- offline sanity (must serve fully from cache, egress off)")
                drive(mode="warm")
        elif spec["seed"] == "mirror":
            print("-- mirror (batch download of pinned releases, egress on)")
            dc("run", "--rm", spec["mirror_service"], check=True)  # mirrors then exits

        if not args.no_drive:
            print("-- drive")
            drive(
                "--concurrency",
                args.concurrency,
                "--samples",
                str(args.samples),
                "--sampling",
                args.sampling,
                "--output",
                f"results/{out_name}",
            )
            result_path = RESULTS / out_name
            result = json.loads(result_path.read_text())
            result["meta"] = {
                "server": args.server,
                "scenario": args.scenario,
                "track": args.track,
                "tier": args.tier,
                "arch": args.arch,
                "wall_s": round(time.time() - t0, 1),
            }
            result_path.write_text(json.dumps(result, indent=2) + "\n")

        if args.capacity or args.install_mix:
            ensure_oha(args.arch)
            common = [
                "python3",
                "capacity.py",
                "--index-url",
                index_url,
                "--host",
                spec["host"],
                "--arch",
                args.arch,
                "--oha",
                "/repo/bench/install/.bin/oha",
                "--control-url",
                "http://control/control-index.json",
                "--label",
                args.server,
            ]
            if args.install_mix:
                print("-- capacity (install-mix ramp -> installs/sec)")
                exec_loadgen(*common, "--install-mix", "--output", f"results/capmix-{out_name}")
            if args.capacity:
                print("-- capacity (index-read MST ramp)")
                exec_loadgen(
                    *common, "--rep-pkg", args.rep_pkg, "--output", f"results/cap-{out_name}"
                )

        print(f"\n== {args.server} done in {round(time.time() - t0, 1)}s")
    finally:
        if not args.keep:
            print("-- teardown")
            dc("down", "-v", check=False, capture_output=True)


if __name__ == "__main__":
    main()
