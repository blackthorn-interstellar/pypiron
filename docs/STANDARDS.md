# Standards support

What PypIron implements, what's planned, and what's deliberately out of scope.
The bar for "supported" is blackbox-verified behavior against real clients
(uv, pip, twine), not just spec-shaped output.

## Support matrix

| Standard | What it is | Status |
|---|---|---|
| [PEP 503](https://peps.python.org/pep-0503/) | Simple HTML index, name normalization, sha256 URL fragments | Supported |
| [PEP 629](https://peps.python.org/pep-0629/) | `pypi:repository-version` meta tag in HTML | Supported |
| [PEP 691](https://peps.python.org/pep-0691/) | JSON simple API with content negotiation | Supported |
| [PEP 700](https://peps.python.org/pep-0700/) | `versions`, `size`, `upload-time` in JSON (api-version 1.1) | Supported |
| Legacy upload API | `POST /legacy/` multipart, as spoken by twine / `uv publish` | Supported (minimal) |
| [PEP 592](https://peps.python.org/pep-0592/) | Yanked releases (`yanked` key / `data-yanked` attr) | Planned |
| [PEP 658](https://peps.python.org/pep-0658/) / [714](https://peps.python.org/pep-0714/) | Serve wheel `METADATA` as `<filename>.metadata` + `core-metadata` attrs | Planned |
| `requires-python` | `data-requires-python` attr / `requires-python` key | Planned (needs write-time metadata capture) |
| Filename immutability | Reject re-upload of an existing filename (pypi.org rule) | Planned |
| HTTP caching | ETag on indexes, `immutable` on artifacts, Range requests | Planned |
| Package deletion | Delete event → index rebuild | Planned |
| Origin exclusivity | Each package is `private` or `mirror`, claimed at first write; collisions rejected (dependency-confusion defense) | Planned |
| Namespace prefix policy | Reserve a configured prefix for private uploads; mirror refuses it (cf. PEP 752) | Planned |
| Mirrored upload times | `sync` writes PyPI's true `upload-time` into sidecars so `--exclude-newer` works on mirrored packages | Planned |
| [PEP 694](https://peps.python.org/pep-0694/) | Upload API 2.0 (draft) | Out of scope until stable |
| [PEP 708](https://peps.python.org/pep-0708/) | Index merging / alternate locations metadata | Out of scope |
| [PEP 740](https://peps.python.org/pep-0740/) | Attestations | Out of scope |
| XML-RPC / search API | Deprecated upstream | Out of scope |
| `/pypi/<pkg>/json` API | Non-standard pypi.org JSON API | Out of scope (the simple API is the standard) |

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
