# Standards support

pypiron works with uv, pip, twine, Poetry, and PDM because it speaks the
packaging standards those clients use. "Supported" means the behavior is
verified against the real clients — not that the output looks spec-shaped. This
page lists what's implemented, what's planned, and what's out of scope (with the
reason omitting it costs nothing).

## Support matrix

| Standard | What it is | Status |
|---|---|---|
| [PEP 503](https://peps.python.org/pep-0503/) | Simple HTML index, name normalization, sha256 URL fragments | Supported |
| [PEP 629](https://peps.python.org/pep-0629/) | `pypi:repository-version` meta tag in HTML | Supported |
| [PEP 691](https://peps.python.org/pep-0691/) | JSON simple API with content negotiation (api-version 1.4) | Supported |
| [PEP 700](https://peps.python.org/pep-0700/) | `versions`, `size`, `upload-time` in JSON | Supported |
| Legacy upload API | `POST /legacy/` multipart, as spoken by twine / `uv publish` | Supported |
| [PEP 592](https://peps.python.org/pep-0592/) | Yanked releases (`yanked` key / `data-yanked` attr) | Supported |
| [PEP 658](https://peps.python.org/pep-0658/) / [714](https://peps.python.org/pep-0714/) | Serve wheel `METADATA` as `<filename>.metadata` + `core-metadata` attrs | Supported |
| `requires-python` | `data-requires-python` attr / `requires-python` key | Supported |
| Filename immutability | Reject re-upload of an existing filename (pypi.org rule) | Supported |
| HTTP caching | ETag on indexes, `immutable` on artifacts, Range requests | Supported |
| Package deletion | Delete event → index rebuild | Supported |
| Private/public exclusivity | Each name is private or public — marked on first upload, locked thereafter; collisions rejected (dependency-confusion defense) | Supported |
| Namespace prefix policy | Reserve a configured prefix for private uploads; mirror refuses it (cf. PEP 752) | Supported |
| Mirrored upload times | `sync` carries PyPI's real `upload-time` into per-file metadata so `--exclude-newer` works on mirrored packages too | Supported |
| [PEP 694](https://peps.python.org/pep-0694/) | Upload API 2.0 (draft) | Out of scope until stable |
| [PEP 708](https://peps.python.org/pep-0708/) | Index merging / alternate locations metadata | Out of scope |
| [PEP 740](https://peps.python.org/pep-0740/) | Provenance/attestations — relayed verbatim through `sync` + proxy (`provenance` key / `data-provenance` attr), not re-verified | Supported |
| [PEP 792](https://peps.python.org/pep-0792/) | Project status markers (`active`/`archived`/`quarantined`/`deprecated`) — relayed verbatim through `sync` + proxy (top-level `project-status` JSON / `pypi:project-status` meta); admin endpoint sets/clears the marker | Supported (relay) |
| [PEP 458](https://peps.python.org/pep-0458/) / [480](https://peps.python.org/pep-0480/) | TUF-signed repository metadata | **Not supported** — out of scope; no installer requires it and pypi.org itself has never shipped it |
| XML-RPC API | Legacy `search` / `list_packages` API | **Not supported** — deprecated upstream (PyPI disabled XML-RPC search); the JSON simple API is the path forward |
| `/pypi/<pkg>/json` API | Non-standard pypi.org metadata API, predates the JSON simple API | **Not supported** — the PEP 691/700 JSON simple API is the standard; installers never request this from a custom index (see below) |
| Eggs (`.egg` / bdist_egg) | Legacy binary distribution format | **Not supported** — wheels + sdists only; pip has removed egg installation |

## Why the legacy `/pypi/<pkg>/json` API is safe to omit

Installers — pip, uv, Poetry, PDM — resolve and install against the **simple API**
(PEP 503 HTML / PEP 691 JSON) at the configured `--index-url`; none request
`/pypi/<name>/json` from a custom index. That endpoint is a metadata convenience
for *analytics and recipe tooling pointed at pypi.org* (dashboards, `grayskull`,
badge/version services), not the install path. The data it carried — versions,
file sizes, upload times, yank state — is what PEP 700 puts in the standard JSON
simple API, which pypiron serves. Omitting it costs no client compatibility.

## Why PEP 700 is the minimum bar: `--exclude-newer`

uv's `--exclude-newer <timestamp>` (reproducible, "as of this date" resolution)
filters distributions by upload time. It requires the `upload-time` field from
PEP 700 in the JSON simple API; **files without `upload-time` are treated as
unavailable** when the flag is passed. An index without PEP 700 doesn't degrade
gracefully — it becomes unusable under `--exclude-newer`.

pypiron always has a trustworthy `upload-time`. Direct uploads are correct by
construction: filenames are immutable, so a file is written once and its
last-modified time *is* its upload time. Per-file metadata makes that timestamp
durable — it survives an rsync or a bucket migration — and lets mirroring carry
forward PyPI's original timestamps.
See [DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md#mirroring-carry-forward-true-timestamps)
for the storage detail.

## Behavior notes

Behaviors worth knowing when verifying compatibility:

- **Content negotiation (PEP 691).** A request with
  `Accept: application/vnd.pypi.simple.v1+json` (or plain `application/json`)
  gets the JSON simple API; anything else gets the HTML index.

- **Yanked releases stay reconciled (PEP 592).** On a mirror, upstream is
  authoritative: a yank set, cleared, or re-worded on PyPI is reconciled onto
  your index on the next `sync`. A file that disappears from upstream is flagged
  yanked `removed upstream` — the bytes stay downloadable (pypiron never deletes
  a mirrored artifact), but installers skip it unless it's pinned.

- **Provenance is relayed, not re-minted (PEP 740).** pypiron carries PyPI's
  already-verified provenance through `sync` and the on-demand proxy — served as
  a `<filename>.provenance` companion, advertised by a `provenance` URL and
  `data-provenance` attribute — so an offline consumer verifies the original
  publisher end-to-end (Sigstore bundles verify against a cached trust root, no
  egress). pypiron never runs Sigstore and never mints provenance, so a direct
  upload carrying first-party `attestations` is refused: it has no Trusted
  Publisher identity to bind.

- **Project status is relayed at the project level (PEP 792).** Status set on
  PyPI (`active`/`archived`/`quarantined`/`deprecated`) is propagated by `sync`
  and the proxy; a `quarantined` project renders with no file links. The admin
  endpoint `POST`/`DELETE /project/<pkg>/status` sets and clears the marker so
  `sync` can relay upstream status (an operator can call it too). pypiron relays
  status rather than enforcing it: quarantine does **not** yet fail-close
  downloads of already-stored bytes — that gate is deferred until a real client
  consumes the marker (pip/uv today only *may* warn).
