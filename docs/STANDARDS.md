# Standards support

What PypIron implements, what's planned, and what's deliberately out of scope.
The bar for "supported" is blackbox-verified behavior against real clients
(uv, pip, twine), not just spec-shaped output.

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
| Origin exclusivity | Each package is `private` or `mirror`, claimed at first write; collisions rejected (dependency-confusion defense) | Supported |
| Namespace prefix policy | Reserve a configured prefix for private uploads; mirror refuses it (cf. PEP 752) | Supported |
| Mirrored upload times | `sync` carries PyPI's true `upload-time` into sidecars (over HTTP to a server with an admin credential) so `--exclude-newer` works on mirrored packages | Supported |
| [PEP 694](https://peps.python.org/pep-0694/) | Upload API 2.0 (draft) | Out of scope until stable |
| [PEP 708](https://peps.python.org/pep-0708/) | Index merging / alternate locations metadata | Out of scope |
| [PEP 740](https://peps.python.org/pep-0740/) | Provenance/attestations — relayed verbatim through `sync` + proxy (`provenance` key / `data-provenance` attr), not verified | Supported |
| [PEP 792](https://peps.python.org/pep-0792/) | Project status markers (`active`/`archived`/`quarantined`/`deprecated`) — relayed verbatim through `sync` + proxy (top-level `project-status` JSON / `pypi:project-status` meta); admin endpoint sets/clears the marker, fail-close enforcement deferred | Supported (relay) |
| [PEP 458](https://peps.python.org/pep-0458/) / [480](https://peps.python.org/pep-0480/) | TUF-signed repository metadata | **Not supported** — out of scope; no installer requires it and pypi.org itself has never shipped it |
| XML-RPC API | Legacy `search` / `list_packages` API | **Not supported** — deprecated upstream (PyPI disabled XML-RPC search); the JSON simple API is the path forward |
| `/pypi/<pkg>/json` API | Non-standard pypi.org metadata API, predates the JSON simple API | **Not supported** — the PEP 691/700 JSON simple API is the standard; installers never request this from a custom index (see note) |
| Eggs (`.egg` / bdist_egg) | Legacy binary distribution format | **Not supported** — wheels + sdists only; pip has removed egg installation |

## Why the legacy `/pypi/<pkg>/json` API is safe to omit

Installers — pip, uv, Poetry, PDM — resolve and install against the **simple API**
(PEP 503 HTML / PEP 691 JSON) at the configured `--index-url`; none of them request
`/pypi/<name>/json` from a custom index. That endpoint is a pypi.org-specific
metadata convenience consumed by *analytics and recipe tooling pointed at pypi.org*
(dashboards, `grayskull`, badge/version services), not by the install path. The
structured data it carried — versions, file sizes, upload times, yank state — is
exactly what PEP 700 puts in the standard JSON simple API, which pypiron does serve.
So omitting it costs no client compatibility.

## Why PEP 700 is the minimum bar: `--exclude-newer`

uv's `--exclude-newer <timestamp>` (the mechanism behind reproducible, "as of this
date" resolution) filters distributions by upload time. It requires the
`upload-time` field from PEP 700 in the JSON simple API; **files without
`upload-time` are treated as unavailable** when the flag is passed. An index
without PEP 700 doesn't degrade gracefully — it becomes unusable under
`--exclude-newer`.

PypIron sources `upload-time` from the metadata sidecar when present, falling back
to storage's native last-modified timestamp (disk mtime, S3 `LastModified`). The
fallback is correct by construction for direct uploads — filenames are immutable,
so a file is written exactly once and last-modified *is* upload time. Sidecars make
the timestamp durable (it survives rsync and bucket migrations) and let mirroring
carry forward PyPI's original timestamps; see
[DESIGN.md](DESIGN.md#mirroring-carry-forward-true-timestamps).

## Implementation notes

- `versions` (PEP 700) comes from the upload form's `version` field, captured
  in the sidecar at write time. Filename inference (PEP 427 wheels, PEP 625
  sdists) remains only as the backfill fallback for files that predate
  sidecars.
- Content negotiation: `Accept: application/vnd.pypi.simple.v1+json` (or plain
  `application/json`) gets JSON; everything else gets HTML. Both are pre-rendered
  static files — negotiation just picks which file to serve.
- The `yanked` and PEP 658 features are pure static-file plays: a sidecar flag and
  a sidecar metadata file, each followed by an index rebuild. No new machinery.
  A re-`sync` keeps the flag honest: upstream is authoritative for a mirror, so
  yank state set, cleared, or re-worded upstream is reconciled onto the local
  sidecar, and a file that has disappeared from upstream is flagged yanked
  `removed upstream` — the bytes stay downloadable (pypiron never deletes a
  mirrored artifact), but installers skip it unless pinned.
- PEP 740 is the same play one more time: a `<filename>.provenance` companion
  served next to the artifact, advertised by a `provenance` URL (JSON) and
  `data-provenance` attribute (HTML). pypiron is a **relay, not a verifier** — it
  carries PyPI's already-verified provenance through `sync` (over HTTP)
  and the on-demand proxy so an offline consumer can verify the original publisher
  end-to-end (Sigstore bundles verify against a cached trust root, no egress
  needed). It never runs Sigstore itself and never mints provenance, so a direct
  upload carrying first-party `attestations` is refused — pypiron has no Trusted
  Publisher identity to bind, so it cannot produce a provenance object any verifier
  would trust. Like every URL we emit, `provenance`/`data-provenance` are
  root-relative (we don't know our public base); clients resolve them against the
  index URL.
- PEP 792 (project status, api-version 1.4) is the relay play once more, at the
  project level: status is truth on disk at `packages/<pkg>/.project-status.json`
  (`{status, reason?}`; absent == `active`, which we omit from output entirely),
  carried into the rendered index — top-level `project-status` JSON object and
  `pypi:project-status[-reason]` HTML meta — and propagated from upstream by `sync`
  and the proxy. A `quarantined` project is rendered with no file links. The admin
  endpoint `POST`/`DELETE /project/<pkg>/status` sets and clears the marker; it
  exists so `sync` can relay upstream status over HTTP (an operator can call it
  too). pypiron still does **not** *author* status as policy: quarantine does
  **not** fail-close downloads of already-stored bytes (the `/files/` and upload
  gates are deferred — only worth building once a real client consumes the marker;
  pip/uv today only *MAY* warn). `sync` reconciles status on every run an upstream
  listing actually changes — not only runs that write files — so a quarantine that
  lands with no new release propagates on the next sync; an upstream-quarantined
  project serves no files, so its (empty) listing is left to the status relay
  rather than misread as a mass removal. We do not implement PEP 708
  `alternate-locations` (out of scope), even though pypi.org emits an empty one
  alongside `project-status`.
