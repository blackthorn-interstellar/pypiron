#!/usr/bin/env python3
"""Render the install-throughput comparison as a uv-style horizontal bar chart.

Reads the mn_ramp result JSONs (results/cmp-*.json), takes each server's peak
sustained installs/sec (the max across its runs — the true ceiling), and emits a
clean SVG bar chart. Stdlib only: the SVG is a few lines of templating, so the
bench keeps its no-extra-dependency rule (no matplotlib).

  plot.py                          # -> results/install-throughput.svg
  plot.py --out chart.svg --title "..."
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path

from benchlib import RESULTS

BAR = "#e07b45"  # iron/rust brand orange — identical in both themes
# Light/dark palettes so the chart can sit on either docs background. The dark
# values track the server/docs scheme (src/web.rs, docs/stylesheets/pypiron.css).
THEMES = {
    "light": {"bg": "white", "text": "#1f2328", "axis": "#8b949e", "grid": "#eaecef"},
    "dark": {"bg": "#1f1813", "text": "#ece5dd", "axis": "#a59a8f", "grid": "#352c24"},
}
XMAX = 3400.0  # axis ceiling (longest bar ~3,026/s; leaves room for its label)
ALIAS = {"bander": "bandersnatch"}  # cmp-bander-ceiling.json -> bandersnatch


def server_of(path: Path) -> str:
    """Server name from the result filename (cmp-<server>[-ceiling][N].json)."""
    name = re.sub(r"^cmp-", "", path.stem)
    name = re.sub(r"-ceiling\d*$|-?\d+$", "", name)  # drop run suffixes
    return ALIAS.get(name, name)


def collect() -> list[tuple[str, float]]:
    """Each server's peak installs/sec (max across its runs), sorted high→low."""
    best: dict[str, float] = {}
    for f in sorted(RESULTS.glob("cmp-*.json")):
        v = json.loads(f.read_text()).get("peak_installs_per_sec")
        if v is not None:
            s = server_of(f)
            best[s] = max(best.get(s, 0.0), float(v))
    return sorted(best.items(), key=lambda kv: -kv[1])


def nice_ticks(top: float, n: int = 5) -> list[float]:
    """Round gridline values from 0 up to (not exceeding) the axis ceiling `top`."""
    step = top / n
    mag = 10 ** math.floor(math.log10(step))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= step)
    return [i * step for i in range(int(top // step) + 1)]


def svg(data: list[tuple[str, float]], title: str, subtitle: str, theme: dict[str, str]) -> str:
    BG, TEXT, AXIS, GRID = theme["bg"], theme["text"], theme["axis"], theme["grid"]
    W, LABEL, PAD_R, ROW = 780, 130, 80, 26
    head = 56 if title else 14
    plot_w = W - LABEL - PAD_R
    base = head + len(data) * ROW
    H = base + 40
    top = XMAX
    ticks = nice_ticks(top)

    def x(v: float) -> float:
        return LABEL + plot_w * v / top

    s = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif">',
        f'<rect width="{W}" height="{H}" fill="{BG}"/>',
    ]
    if title:
        s.append(
            f'<text x="{LABEL}" y="26" font-size="16" font-weight="700" fill="{TEXT}">{title}</text>'
        )
        s.append(f'<text x="{LABEL}" y="44" font-size="12" fill="{AXIS}">{subtitle}</text>')
    for t in ticks:
        gx = x(t)
        s.append(
            f'<line x1="{gx:.1f}" y1="{head}" x2="{gx:.1f}" y2="{base}" stroke="{GRID}" stroke-width="1"/>'
        )
        s.append(
            f'<text x="{gx:.1f}" y="{base + 18}" font-size="11" fill="{AXIS}" '
            f'text-anchor="middle">{t:,.0f}</text>'
        )
    s.append(
        f'<text x="{LABEL + plot_w / 2:.1f}" y="{base + 34}" font-size="11" fill="{AXIS}" '
        f'text-anchor="middle">installs / second (higher is better)</text>'
    )
    bh = 16
    for i, (name, v) in enumerate(data):
        by = head + i * ROW + (ROW - bh) / 2
        ty = by + bh * 0.72
        weight = 700 if i == 0 else 400
        s.append(
            f'<text x="{LABEL - 12}" y="{ty:.1f}" font-size="13" font-weight="{weight}" '
            f'fill="{TEXT}" text-anchor="end">{name}</text>'
        )
        s.append(
            f'<rect x="{LABEL}" y="{by:.1f}" width="{x(v) - LABEL:.1f}" height="{bh}" rx="2" fill="{BAR}"/>'
        )
        s.append(
            f'<text x="{x(v) + 8:.1f}" y="{ty:.1f}" font-size="12" font-weight="{weight}" '
            f'fill="{TEXT}">{int(v + 0.5):,}/s</text>'
        )
    s.append("</svg>")
    return "\n".join(s)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--out", default=str(RESULTS / "install-throughput.svg"))
    ap.add_argument("--theme", choices=tuple(THEMES), default="light")
    # Default titleless: the chart is captioned where it's embedded. Pass --title
    # (and optionally --subtitle) for a standalone version.
    ap.add_argument("--title", default="")
    ap.add_argument("--subtitle", default="")
    args = ap.parse_args()
    data = collect()
    if not data:
        raise SystemExit(f"no cmp-*.json results in {RESULTS}")
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(svg(data, args.title, args.subtitle, THEMES[args.theme]))
    print(f"wrote {out}")
    for name, v in data:
        print(f"  {name:<14} {int(v + 0.5):,}/s")


if __name__ == "__main__":
    main()
