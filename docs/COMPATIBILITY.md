<!-- GENERATED — do not edit. Regenerate with `make compat`. -->

# Client Compatibility

Every populated cell is backed by an integration test that runs the real client binary against a real pypiron server.

Generated: 2026-06-12 08:28:24 UTC
Revision: `b79dd16`

| Client | upload | install | resolve | pep658-metadata | yank | hash-check | exclude-newer |
| --- | --- | --- | --- | --- | --- | --- | --- |
| pip | — | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| uv | ✅ | ✅ | ✅ | ✅ | — | — | ✅ |
| poetry | ✅ | ✅ | ✅ | — | — | — | — |
| pdm | ✅ | ✅ | ✅ | — | — | — | — |
| twine | ✅ | — | — | — | — | — | — |
| flit | ✅ | — | — | — | — | — | — |
| hatch | ✅ | — | — | — | — | — | — |

Legend: ❌ known incompatibility / failing, ✅ verified, ? not verified in this run, — not tested / not applicable.

## Client Versions

| Client | Version source |
| --- | --- |
| pip | venv-seeded |
| uv | system |
| poetry | 2.4.1 |
| pdm | 2.27.0 |
| twine | dev-dependency |
| flit | 3.12.0 |
| hatch | 1.17.0 |
