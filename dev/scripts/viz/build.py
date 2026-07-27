"""Build the run-visualizer pages: run the scenario pack, verify it still says
what it claims, and write standalone HTML into .local/viz/.

`make viz` runs this. For every scenario in dev/scripts/viz/scenarios.json it
runs the exact command recorded there, collects the payload the producer wrote
(a JSONL trace from `vopr --trace-jsonl`, a state graph from a model test under
PYPIRON_VIZ_GRAPH), and inlines it into a copy of dev/scripts/viz/player.html —
one self-contained file per scenario, plus an index that links them.

Two gates, and they are the reason this script exists rather than a shell loop:

  * The staleness gate. Every claim a narration makes out loud is pinned in
    scenarios.json — the exit code, the violation text, the storage-op count,
    the objects left at quiescence, the node and edge counts of a state graph —
    and a scenario that stops reproducing FAILS THE BUILD. A page that quietly
    goes green is exactly the stale-evidence failure commit 1e80be3 fixed once.
    A seed alone means nothing, so each one is stored with the commit that
    verified it and the flags it needs.

  * The number-source gate. Every figure in scale.json must name the file and
    line it was measured at. No source, no number; and two sources are refused
    outright because dev/TESTING.md says never to quote them outward.

It also runs the inertness gate: each traced profile runs twice, with and
without `--trace-jsonl`, and the recorder must not perturb the simulation by one
byte. Stdlib only, no shebang. Run from the repo root:

    python dev/scripts/viz/build.py --out .local/viz
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
PLAYER = HERE / "player.html"
SCENARIOS = HERE / "scenarios.json"
SCALE = HERE / "scale.json"

# dev/TESTING.md:149-153 and :1336-1339 both say never to quote these outward.
SOURCE_DENYLIST = ("dr-drill", "make perf")

VOPR = ["cargo", "run", "--release", "--example", "vopr", "--"]
# Two things legitimately differ between two runs of the same seed: the wall
# clock, and cargo's own progress lines (which echo the argv, so the traced run's
# `Running` line names the flag). The inertness gate normalizes both away and
# demands that everything else match to the character.
ELAPSED = re.compile(r"\bin \d+(\.\d+)?(ns|µs|ms|s)\b")
CARGO_NOISE = re.compile(
    r"^\s+(Compiling|Finished|Running|Building|Blocking|Fresh|Locking|Updating|Downloaded"
    r"|Downloading)\b.*$",
    re.M,
)


def run_output(text: str) -> str:
    return ELAPSED.sub("in <t>", CARGO_NOISE.sub("", text))


class Problem(Exception):
    """A scenario stopped reproducing, or a number lost its source."""


def sh(argv: list[str], env: dict[str, str] | None = None) -> tuple[int, str]:
    """Run a command from the repo root; return (exit code, stdout+stderr)."""
    full = dict(os.environ)
    full.update(env or {})
    p = subprocess.run(argv, cwd=ROOT, env=full, capture_output=True, text=True, check=False)
    return p.returncode, p.stdout + p.stderr


def git_commit() -> str:
    code, out = sh(["git", "rev-parse", "--short", "HEAD"])
    if code != 0:
        return "unknown"
    code, dirty = sh(["git", "status", "--porcelain"])
    return out.strip() + ("+dirty" if dirty.strip() else "")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for n, line in enumerate(path.read_text().splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as e:
            raise Problem(f"{path}: line {n + 1} is not JSON: {e}") from e
    return rows


def check_envelope(rows: list[dict[str, Any]]) -> None:
    """The frozen contract: line 0 is meta, last is summary, `i` has no gaps."""
    if not rows:
        raise Problem("empty trace")
    if rows[0].get("t") != "meta":
        raise Problem(f"line 0 is {rows[0].get('t')!r}, not 'meta'")
    if rows[-1].get("t") != "summary":
        raise Problem(f"last line is {rows[-1].get('t')!r}, not 'summary'")
    for n, r in enumerate(rows):
        if r.get("i") != n:
            raise Problem(f"event index gap: line {n} carries i={r.get('i')!r}")
        for field in ("t", "i", "sim", "ts"):
            if field not in r:
                raise Problem(f"line {n} is missing the required field {field!r}")


def fold_world(rows: list[dict[str, Any]]) -> dict[int, dict[str, str]]:
    """Reconstruct final bucket state the one normative way: fold `world`.

    Nothing else contributes. An `op` is emitted at admission, before the call
    executes, so its byte effect is unknown at emit time — guessing it would put
    invented state on screen.
    """
    world: dict[int, dict[str, str]] = {}
    for r in rows:
        if r.get("t") != "world":
            continue
        bucket = world.setdefault(r["b"], {})
        if r.get("full"):
            bucket.clear()
        bucket.update(r.get("put") or {})
        for key in r.get("del") or []:
            bucket.pop(key, None)
    return world


def world_facts(rows: list[dict[str, Any]]) -> dict[str, int]:
    world = fold_world(rows)
    quarantined = [k for b in world.values() for k in b if k.startswith("_quarantine/")]
    return {
        "frozen": sum(1 for b in world.values() for k in b if k.endswith(".frozen")),
        # `quarantine` counts OBJECTS; two buckets holding the same losing body
        # are two objects and one byte-set. A narration that says "preserved
        # bodies" means the second number.
        "quarantine": len(quarantined),
        "distinct_quarantined_bodies": len({k.rsplit("@", 1)[-1] for k in quarantined}),
    }


def ack_facts(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Split the `ack` events, because "acks" is three different numbers.

    An `ack` is emitted for every upload and every delete the workload issued,
    accepted or not: a delete of a filename that is already gone is a refusal
    the client saw as an error, not a client-visible ack. And neither is
    `summary.acked`, which is the ledger's count of distinct FILENAMES that ever
    carried an accepted upload. All three get their own name here so a narration
    has to say which one it means.
    """
    acks = [r for r in rows if r.get("t") == "ack"]
    ok = [a for a in acks if a.get("ok")]
    return {
        "answered": len(acks),
        "accepted": len(ok),
        "refused": len(acks) - len(ok),
        "publish_ok": sum(1 for a in ok if a.get("kind") == "publish"),
        "delete_ok": sum(1 for a in ok if a.get("kind") == "delete"),
        "refused_delete": sum(1 for a in acks if not a.get("ok") and a.get("kind") == "delete"),
        "distinct_files_acked": len({a.get("file") for a in ok if a.get("kind") == "publish"}),
    }


def expect(name: str, got: Any, want: Any, sc: dict[str, Any]) -> None:
    if got == want:
        return
    raise Problem(
        f"{name}: expected {want!r}, got {got!r}\n"
        f"    pinned at commit {sc.get('verified_at')}, built at {git_commit()}\n"
        f"    a seed is only meaningful as (commit, seed, flags); re-measure the\n"
        f"    scenario and update scenarios.json deliberately, or retire it"
    )


# --------------------------------------------------------------------------- #
# producers
# --------------------------------------------------------------------------- #


class Skipped(Exception):
    """A producer needs something this machine does not have (Docker, MinIO)."""


def build_vopr(sc: dict[str, Any], data: Path) -> dict[str, Any]:
    """Run a simulator scenario under `--trace-jsonl`."""
    out = data / f"{sc['id']}.jsonl"
    pretty = " ".join(VOPR + sc["argv"] + ["--trace-jsonl", f"{sc['id']}.jsonl"])
    code, log = sh(VOPR + sc["argv"] + ["--trace-jsonl", str(out)])
    if not out.exists():
        raise Problem(f"the recorder wrote no file\n    {pretty}\n{log[-2000:]}")
    return build_trace(sc, out, code, log, pretty)


def build_region(sc: dict[str, Any], data: Path) -> dict[str, Any]:
    """Run the blackbox region recorder, which writes <id>.jsonl into a directory.

    It needs Docker and MinIO. `may_skip` says what may be missing; when it is,
    the scenario is reported as not built rather than failing the build — the same
    contract the S3 blackbox tests already have.
    """
    out = data / f"{sc['id']}.jsonl"
    out.unlink(missing_ok=True)
    code, log = sh(sc["argv"], env={sc["env_out"]: str(data)})
    if not out.exists():
        skipped = code == 5 or re.search(r"\b\d+ skipped\b", log) or "no tests ran" in log
        if skipped and sc.get("may_skip"):
            raise Skipped(f"{sc['may_skip']} is not available on this machine")
        raise Problem(f"the recorder wrote no file (exit {code})\n{log[-2000:]}")
    if code != sc["expect_exit"]:
        # `build_trace` would catch the code too, but it never sees the log, and
        # a recorder that half-ran is exactly the case whose log you need.
        raise Problem(f"the recorder exited {code}, not {sc['expect_exit']}\n{log[-3000:]}")
    return build_trace(sc, out, code, log, None)


def build_trace(
    sc: dict[str, Any], out: Path, code: int, log: str, pretty: str | None
) -> dict[str, Any]:
    """Verify a recorded JSONL trace against the pack, then make it inlinable."""
    rows = read_jsonl(out)
    check_envelope(rows)
    meta, summary = rows[0], rows[-1]

    expect("exit code", code, sc["expect_exit"], sc)
    # The recorded code is the RUN's own verdict — 0 clean, 2 violated, 3 the
    # determinism recheck — and never the process exit code: `--require-reach`
    # exits 4 on a whole-run gate no single seed can see. The simulator names it
    # `seed_exit_code` for that reason; the region recorder says `exit_code`.
    seed_exit = summary.get("seed_exit_code", summary.get("exit_code"))
    expect("recorded exit code", seed_exit, sc["expect_exit"], sc)
    if summary.get("truncated"):
        raise Problem("the recorder hit its event cap and truncated the trace")

    want_violation = sc.get("expect_violation")
    lines = [r["text"] for r in rows if r.get("t") == "violation"]
    if want_violation is None:
        expect("violations", summary["violations"], 0, sc)
        if lines:
            raise Problem(f"expected a clean run, got: {lines[0]}")
    else:
        if not lines:
            raise Problem(
                f"expected a violation containing {want_violation!r} and got none —\n"
                f"    if this finding was adjudicated away, mark the scenario retired"
            )
        for text in lines:
            if want_violation not in text:
                raise Problem(f"violation does not contain {want_violation!r}: {text}")

    if "expect_trace_events" in sc:
        # NOT the storage-op count: `trace_events` is the TraceHasher's record
        # count, which is the admitted ops PLUS one tuple per CAS outcome. It is
        # pinned because it is what the determinism hash covers; the number a
        # narration means by "storage ops" is `expect_event_counts.op`.
        expect(
            "summary.trace_events (ops + CAS records)",
            summary["trace_events"],
            sc["expect_trace_events"],
            sc,
        )
    for kind, n in (sc.get("expect_event_counts") or {}).items():
        expect(f"`{kind}` events", sum(1 for r in rows if r.get("t") == kind), n, sc)
    acks = ack_facts(rows)
    for key, n in (sc.get("expect_acks") or {}).items():
        expect(f"acks ({key})", acks.get(key), n, sc)
    # The verdict the page will actually render for a named oracle. Pinned
    # separately from the counts because "held" is the claim a narration makes,
    # and an oracle that quietly stopped executing renders "not-executed" while
    # every count around it stays put.
    for name, verdict in (sc.get("expect_oracles") or {}).items():
        seen = [r for r in rows if r.get("t") == "oracle" and r.get("name") == name]
        if not seen:
            raise Problem(f"the recording emitted no `{name}` oracle event")
        expect(f"oracle {name}", seen[0].get("verdict"), verdict, sc)
    kinds = {r.get("t") for r in rows}
    for kind in sc.get("expect_event_kinds") or []:
        if kind not in kinds:
            raise Problem(
                f"the recording emitted no `{kind}` events, so it measures less than it claims"
            )
    facts = world_facts(rows)
    for key, n in (sc.get("expect_world") or {}).items():
        expect(f"objects at quiescence ({key})", facts.get(key), n, sc)

    # The simulator records a run and leaves the naming blank; the pack names it.
    # A producer that named itself (the region recorder) keeps its own words.
    # Caveats accumulate: the producer's own first, then the pack's.
    meta["scenario"] = meta.get("scenario") or sc["id"]
    meta["title"] = meta.get("title") or sc["title"]
    meta["narration"] = meta.get("narration") or sc["narration"]
    meta["cmd"] = pretty or meta.get("cmd") or " ".join(sc["argv"])
    meta["caveats"] = list(meta.get("caveats") or []) + list(sc.get("caveats") or [])
    if not meta["caveats"]:
        raise Problem("a payload with no caveats overclaims; give it at least one")
    out.write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in rows))
    return {"kind": "trace", "lines": rows}


def build_graph(sc: dict[str, Any], data: Path) -> dict[str, Any]:
    """Run a model dump test and return its state-graph payload."""
    out = data / f"{sc['id']}.json"
    # A repo-relative path where possible, because the producer echoes it back
    # into `generated_by` and that string is meant to be paste-runnable anywhere.
    where = out.relative_to(ROOT) if out.is_relative_to(ROOT) else out
    code, log = sh(sc["argv"], env={sc["env_out"]: str(where)})
    expect("exit code", code, sc["expect_exit"], sc)
    if not out.exists():
        raise Problem(f"the dump test wrote no file\n{log[-2000:]}")

    payload = json.loads(out.read_text())
    if payload.get("kind") != "graph":
        raise Problem(f"payload kind is {payload.get('kind')!r}, not 'graph'")
    counts = payload.get("counts") or {}
    for key, n in (sc.get("expect_counts") or {}).items():
        if key not in counts:
            raise Problem(f"the dump no longer reports counts.{key}")
        expect(f"counts.{key}", counts[key], n, sc)
    payload["caveats"] = list(payload.get("caveats") or []) + list(sc.get("caveats") or [])
    if not payload["caveats"]:
        raise Problem("a payload with no caveats overclaims; give it at least one")
    return payload


def measure_live(secs: int, commit: str) -> list[dict[str, Any]]:
    """Two rows nobody has measured for you: this machine, right now."""
    if secs <= 0:
        return []
    argv = VOPR + ["--max-secs", str(secs), "--rotate", "--start-seed", str(int(time.time()))]
    code, log = sh(argv)
    if code != 0:
        raise Problem(f"the live measurement run failed (exit {code})\n{log[-2000:]}")
    m = re.search(
        r"vopr: (\d+) seeds explored, (\d+) storage-op interleavings.*? in ([\d.]+)s",
        log,
    )
    if not m:
        raise Problem("could not parse the live run's summary line")
    seeds, ops, elapsed = int(m.group(1)), int(m.group(2)), float(m.group(3))
    src = f"measured by `make viz` on this machine at commit {commit} · {secs}s --rotate run"
    return [
        {
            "value": round(seeds / elapsed),
            "label": "seeded fault schedules per second",
            "source": src,
            "live": True,
        },
        {
            "value": round(ops / elapsed),
            "label": "storage-op interleavings per second",
            "source": src,
            "live": True,
        },
    ]


def build_scale(sc: dict[str, Any], built: dict[str, Any], live_secs: int) -> dict[str, Any]:
    payload = json.loads(SCALE.read_text())
    payload.pop("_note", None)
    commit = git_commit()
    payload["commit"] = commit
    payload["generated_at"] = time.strftime("%Y-%m-%d")
    payload["title"] = sc["title"]
    payload["narration"] = sc["narration"]

    payload["stats"] = list(payload.get("stats") or []) + measure_live(live_secs, commit)
    for group in ("stats", "bars"):
        for entry in payload.get(group) or []:
            src = (entry.get("source") or "").strip()
            if not src:
                raise Problem(f"{group}: {entry.get('label')!r} has no source")
            for banned in SOURCE_DENYLIST:
                if banned in src:
                    raise Problem(
                        f"{group}: {entry.get('label')!r} cites {banned!r}, which "
                        f"dev/TESTING.md says never to quote outward"
                    )

    loop = built.get(sc.get("loop_trace"))
    if loop:
        payload["loop_trace_lines"] = loop["payload"]["lines"]
    return payload


# --------------------------------------------------------------------------- #
# pages
# --------------------------------------------------------------------------- #


def esc(text: str) -> str:
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def standalone(player: str, payload: dict[str, Any], title: str) -> str:
    """Inline a payload into a copy of the player. `<` is escaped so nothing in
    the data can close the script element."""
    blob = json.dumps(payload, sort_keys=True).replace("<", "\\u003c")
    tag = '<script type="application/json" id="payload"></script>'
    if tag not in player:
        raise Problem(f"player.html no longer contains the inline payload tag: {tag}")
    page = player.replace(tag, f'<script type="application/json" id="payload">{blob}</script>')
    return page.replace(
        "<title>pypiron run visualizer</title>",
        f"<title>{esc(title)} — pypiron</title>",
    )


INDEX_CSS = """
/* Copied from PAGE_CSS in src/html.rs, the same two lines player.html carries. */
:root{color-scheme:light dark;--bg:#faf7f4;--header:#efe7df;--fg:#211b17;--muted:#7c7269;--card:#fff;--border:#e7ddd3;--accent:#bf5a2e;--accent-ink:#a04a24;--code:#f3ece4}
@media(prefers-color-scheme:dark){:root{--bg:#15110e;--header:#1f1813;--fg:#ece5dd;--muted:#a59a8f;--card:#1c1713;--border:#352c24;--accent:#e07b45;--accent-ink:#ef9460;--code:#1f1813}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.55 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
main{max-width:1000px;margin:0 auto;padding:32px 24px 72px}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
h1{margin:0;font-size:30px;letter-spacing:-.02em}
h2{margin:36px 0 10px;font-size:12px;font-weight:600;letter-spacing:.03em;text-transform:uppercase;color:var(--muted)}
p.sub{margin:8px 0 0;color:var(--muted);max-width:80ch}
.band{background:var(--header);border-bottom:1px solid var(--border)}
.band-in{max-width:1000px;margin:0 auto;padding:26px 24px 28px}
.card{border:1px solid var(--border);background:var(--card);border-radius:10px;padding:14px 16px;margin:10px 0}
.card h3{margin:0;font-size:17px}
.card p{margin:6px 0 0;max-width:84ch}
.card p.narr{color:var(--fg)}
.card p.why{color:var(--muted);font-size:13px}
pre{margin:10px 0 0;background:var(--code);border:1px solid var(--border);border-radius:8px;padding:9px 11px;overflow-x:auto;
    font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
.badge{display:inline-block;border:1px solid var(--border);border-radius:5px;padding:1px 8px;margin-left:8px;
       font-size:12px;color:var(--muted);vertical-align:middle;font-variant-numeric:tabular-nums}
.badge.red{border-color:#b91c1c;color:#b91c1c}
.badge.green{border-color:#16a34a;color:#16a34a}
"""


def index_page(rows: list[dict[str, Any]], commit: str, meta_note: str) -> str:
    def card(r: dict[str, Any]) -> str:
        head = esc(r["title"])
        if r.get("badge"):
            head += f'<span class="badge {r.get("badge_class", "")}">{esc(r["badge"])}</span>'
        if r.get("href"):
            head = f'<a href="{esc(r["href"])}">{head}</a>'
        parts = [f'<div class="card"><h3>{head}</h3>', f'<p class="narr">{esc(r["narration"])}</p>']
        if r.get("why"):
            parts.append(f'<p class="why">{esc(r["why"])}</p>')
        if r.get("cmd"):
            parts.append(f"<pre>{esc(r['cmd'])}</pre>")
        parts.append("</div>")
        return "".join(parts)

    def section(title: str, kind: str) -> str:
        group = [r for r in rows if r["group"] == kind]
        if not group:
            return ""
        return f"<h2>{esc(title)}</h2>" + "".join(card(r) for r in group)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pypiron run visualizer</title>
<style>{INDEX_CSS}</style></head>
<body>
<div class="band"><div class="band-in">
<h1>pypiron run visualizer</h1>
<p class="sub">Recorded from the deterministic simulator, the stateright models and the
blackbox suite at commit <code>{esc(commit)}</code>. Every page is self-contained: open the
file, no server needed. Regenerate with <code>make viz</code>.</p>
<p class="sub">{esc(meta_note)}</p>
</div></div>
<main>
{section("Scenarios", "built")}
{section("Retired — the finding was adjudicated away", "retired")}
{section("Not built — and why not", "pending")}
<h2>Reading the payloads directly</h2>
<div class="card"><p class="narr">Every payload is next to these pages under
<code>data/</code>, in the JSONL/JSON contract documented in
<code>dev/scripts/viz/README.md</code>.</p>
<pre>python3 -m json.tool data/model-conflict.json | less
head -1 data/converge.jsonl | python3 -m json.tool</pre></div>
</main></body></html>
"""


# --------------------------------------------------------------------------- #
# gates
# --------------------------------------------------------------------------- #


def inertness_gate(scenarios: list[dict[str, Any]], tmp: Path) -> None:
    """`--trace-jsonl` must not perturb the simulation by one byte.

    Same rule dev/TESTING.md applies to every other addition to this binary,
    applied to the recorder: run each traced profile twice under VOPR_TRACE, once
    with the flag and once without, and require the op-interleaving dump to be
    byte-identical and the printed run summary to match once the wall clock is
    normalized away.
    """
    tmp.mkdir(parents=True, exist_ok=True)
    for sc in scenarios:
        if sc.get("producer") != "vopr" or sc.get("status") != "live":
            continue
        base, flagged = tmp / f"{sc['id']}.a", tmp / f"{sc['id']}.b"
        env_a = {"VOPR_TRACE": "1", "VOPR_TRACE_FILE": str(base)}
        env_b = {"VOPR_TRACE": "1", "VOPR_TRACE_FILE": str(flagged)}
        code_a, out_a = sh(VOPR + sc["argv"], env=env_a)
        code_b, out_b = sh(
            VOPR + sc["argv"] + ["--trace-jsonl", str(tmp / f"{sc['id']}.jsonl")], env=env_b
        )
        if code_a != code_b:
            raise Problem(
                f"inertness: {sc['id']} exits {code_a} without the flag, {code_b} with it"
            )
        if base.read_bytes() != flagged.read_bytes():
            raise Problem(f"inertness: {sc['id']} produced a different op interleaving")
        if run_output(out_a) != run_output(out_b):
            raise Problem(f"inertness: {sc['id']} printed a different run summary")
        print(f"  inert  {sc['id']:<24} {len(base.read_bytes()):>9,} bytes, byte-identical")


def pending_gate(sc: dict[str, Any]) -> None:
    """A pending scenario's excuse must not outlive its cause."""
    needs = sc.get("needs")
    if not needs:
        raise Problem(f"{sc['id']}: a pending scenario must name what it `needs`")
    if (ROOT / needs).exists():
        raise Problem(
            f"{sc['id']}: {needs} now exists, so the scenario is no longer pending — "
            f"give it expectations and flip it to live"
        )


# --------------------------------------------------------------------------- #


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out", default=".local/viz", help="output directory (gitignored)")
    parser.add_argument(
        "--only", action="append", metavar="ID", help="build one scenario (repeatable)"
    )
    parser.add_argument(
        "--live-secs",
        type=int,
        default=20,
        metavar="N",
        help="seconds for the scale page's live measurement; 0 omits those rows",
    )
    parser.add_argument(
        "--skip-inertness",
        action="store_true",
        help="skip the determinism gate (it doubles the vopr runs)",
    )
    parser.add_argument("--list", action="store_true", help="list the pack and exit")
    args = parser.parse_args()

    pack = json.loads(SCENARIOS.read_text())["scenarios"]
    if args.only:
        pack = [s for s in pack if s["id"] in args.only]
        missing = set(args.only) - {s["id"] for s in pack}
        if missing:
            print(f"no such scenario: {', '.join(sorted(missing))}", file=sys.stderr)
            return 2
    if args.list:
        for sc in pack:
            print(f"{sc['status']:<8} {sc['id']:<24} {sc['title']}")
        return 0

    out = (ROOT / args.out).resolve()
    data = out / "data"
    data.mkdir(parents=True, exist_ok=True)
    player = PLAYER.read_text()
    commit = git_commit()
    problems: list[str] = []

    if not args.skip_inertness:
        print("inertness gate — the recorder must not perturb the simulation:")
        try:
            inertness_gate(pack, out / "tmp")
        except Problem as e:
            problems.append(f"inertness gate: {e}")
        finally:
            shutil.rmtree(out / "tmp", ignore_errors=True)

    print(f"building {len(pack)} scenarios at commit {commit}:")
    built: dict[str, Any] = {}
    rows: list[dict[str, Any]] = []
    for sc in pack:
        if sc["status"] == "retired":
            rows.append(
                {
                    "group": "retired",
                    "title": sc["title"],
                    "narration": sc["narration"],
                    "why": f"Retired at {sc['retired_at']}: {sc['retired_because']}",
                    "cmd": " ".join(VOPR + sc["argv"]) if sc["argv"] else "",
                    "badge": "closed",
                    "badge_class": "green",
                }
            )
            print(f"  retired  {sc['id']:<24} closed at {sc['retired_at']}")
            continue
        if sc["status"] == "pending":
            try:
                pending_gate(sc)
            except Problem as e:
                problems.append(str(e))
                continue
            rows.append(
                {
                    "group": "pending",
                    "title": sc["title"],
                    "narration": sc["narration"],
                    "why": sc["pending_because"],
                    "badge": "not built",
                }
            )
            print(f"  pending  {sc['id']:<24} needs {sc['needs']}")
            continue

        try:
            if sc["producer"] == "vopr":
                payload = build_vopr(sc, data)
            elif sc["producer"] == "region":
                payload = build_region(sc, data)
            elif sc["producer"] == "graph":
                payload = build_graph(sc, data)
            elif sc["producer"] == "scale":
                payload = build_scale(sc, built, args.live_secs)
                (data / "scale.json").write_text(json.dumps(payload, indent=1, sort_keys=True))
            else:
                raise Problem(f"unknown producer {sc['producer']!r}")
        except Skipped as e:
            rows.append(
                {
                    "group": "pending",
                    "title": sc["title"],
                    "narration": sc["narration"],
                    "why": f"Not built on this machine: {e}. Install it and re-run `make viz`.",
                    "badge": "not built",
                }
            )
            print(f"  skipped  {sc['id']:<24} {e}")
            continue
        except Problem as e:
            problems.append(f"{sc['id']}: {e}")
            print(f"  FAILED   {sc['id']:<24} {str(e).splitlines()[0]}")
            continue

        page = out / f"{sc['id']}.html"
        page.write_text(standalone(player, payload, sc["title"]))
        built[sc["id"]] = {"payload": payload, "page": page}
        if payload["kind"] == "graph":
            counts = payload["counts"]
            badge, badge_class = f"{counts['nodes']:,} states · {counts['edges']:,} edges", ""
        elif payload["kind"] == "scale":
            badge, badge_class = "", ""
        elif sc.get("expect_violation"):
            n = payload["lines"][-1]["violations"]
            badge, badge_class = f"exit {sc['expect_exit']} · {n} violation(s)", "red"
        else:
            badge, badge_class = "clean", "green"
        rows.append(
            {
                "group": "built",
                "title": sc["title"],
                "narration": sc["narration"],
                "href": page.name,
                "cmd": (
                    payload["lines"][0]["cmd"]
                    if payload["kind"] == "trace"
                    else payload.get("generated_by", "")
                ),
                "badge": badge,
                "badge_class": badge_class,
            }
        )
        print(f"  ok       {sc['id']:<24} {page.stat().st_size:>9,} bytes  {page.name}")

    # The two dead-code panels are the one story that needs both at once. The
    # player stacks two payloads only over ?src=, which needs a real origin.
    pairs = [s for s in pack if s.get("pair") and s["id"] in built]
    if len(pairs) == 2:
        shutil.copyfile(PLAYER, out / "player.html")
        a, b = pairs[0]["id"], pairs[1]["id"]
        rows.append(
            {
                "group": "built",
                "title": "Side by side: the same seed with and without a partitioned fleet",
                "narration": "The two runs above on one clock, in two panels. The player stacks "
                "them from two ?src= payloads, which a browser will only fetch over "
                "http — so this link needs a local server, not a file:// open.",
                "href": f"player.html?src=data/{a}.jsonl,data/{b}.jsonl",
                "cmd": f"cd {args.out} && python3 -m http.server 8009  # then open the link above",
                "badge": "needs a local server",
            }
        )

    note = (
        "The inertness gate ran: every traced profile produced a byte-identical op "
        "interleaving with and without the recorder."
        if not args.skip_inertness
        else "The inertness gate was skipped for this build."
    )
    (out / "index.html").write_text(index_page(rows, commit, note))
    print(f"  ok       {'index':<24} {(out / 'index.html').stat().st_size:>9,} bytes  index.html")

    if problems:
        print(
            f"\n{len(problems)} problem(s) — nothing here may be published as measurement:",
            file=sys.stderr,
        )
        for p in problems:
            print(f"\n  * {p}", file=sys.stderr)
        return 1
    print(f"\nwrote {len(built)} pages to {args.out}/ — open {args.out}/index.html")
    return 0


if __name__ == "__main__":
    sys.exit(main())
