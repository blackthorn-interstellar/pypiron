---
description: Curate which public packages your PyPI server serves. One approved list with version specifiers drives proxy and sync; file filters trim the rest.
---

# Approval lists

Your builds install only what you've approved — every client, every tool, every
transitive dependency. An approval list names the public packages the server
serves.

## One list, one file

`packages.txt`, one package per line, with optional version specifiers:

```text
requests>=2.32,<3
urllib3
six==1.16.0
```

Point the `[mirror]` table at it:

```toml
[mirror]
include-packages-from = "packages.txt"
```

`include-packages` holds the same specs inline. `exclude-packages` and
`exclude-packages-from` carve exceptions out. Excludes always win. Every key is
also a flag and a `PYPIRON_*` environment variable —
[Mirror selection](../reference/configuration.md#mirror-selection) lists them
all.

## Trim the files, not just the packages

An approved package still ships files you may not want — Windows wheels on a
Linux CI fleet, prereleases, builds for Pythons you retired. The same table
filters those:

```toml
[mirror]
include-packages-from = "packages.txt"
include-format = ["wheel"]                   # wheel, sdist, other
exclude-platform-tag = ["win*", "macosx_*"]  # matches wheel tags, `*` allowed
exclude-python-below = "3.9"
exclude-prereleases = true
```

`exclude-python-below` drops wheels built only for older Pythons but keeps
sdists and universal wheels. `exclude-newer` — the cooldown that makes new
releases wait seven days — lives in the same table and is on by default:
[Security](../security.md).

## Proxy runs open; sync requires a list

The same `[mirror]` table drives proxy and sync:

- **The proxy can run open.** With no include list it serves any non-private
  package on demand — the file filters and the cooldown still apply to
  everything it fetches.
- **`sync` refuses to run open.** Pre-loading needs `include-packages` or
  `include-packages-from` — nobody mirrors all of PyPI by accident.

Your own private packages are outside the list entirely — each name is private
or public, never both: [Package sources](package-sources.md).

## When an approval list is the right control

- **Compliance.** The list is the policy. Approving a dependency is an edit to
  `packages.txt` — reviewed, versioned, enforced for every client at once.
- **CI.** A new dependency arrives by pull request, not by resolution finding
  it first.
- **Air-gapped serving.** `sync` pre-loads the approved list from a host with
  egress; the serving node needs none:
  [Air-gapped deployment](../guides/air-gapped.md).
