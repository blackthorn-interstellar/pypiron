<!-- GENERATED — do not edit. Regenerate with `make compat`. -->

# Client Compatibility

Every populated cell is backed by an integration test that runs the real client binary against a real pypiron server.

Generated: 2026-06-18 23:39:10 UTC
Revision: `59c3251`

| Client | upload | install | resolve | pep658-metadata | yank | hash-check | exclude-newer |
| --- | --- | --- | --- | --- | --- | --- | --- |
| pip | — | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| uv | ✅ | ✅ | ✅ | ✅ | — | — | ✅ |
| poetry | ✅ | ✅ | ✅ | — | — | — | — |
| pdm | ✅ | ✅ | ✅ | — | — | — | — |
| twine | ✅ | — | — | — | — | — | — |
| flit | ✅ | — | — | — | — | — | — |
| hatch | ✅ | — | — | — | — | — | — |
| pipenv | — | ✅ | ✅ | — | — | — | — |

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
| pipenv | 2026.5.2 |
