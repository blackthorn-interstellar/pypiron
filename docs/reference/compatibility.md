<!-- GENERATED — do not edit. Regenerate with `make compat`. -->

# Client compatibility

Every major Python packaging tool works with pypiron. This matrix shows which workflows are verified for each client — every ✅ is backed by an integration test that runs the real client binary against a real pypiron server.

All listed clients install packages, and the ones that publish can upload; the advanced columns vary by what each tool implements. Check yours before you deploy.

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

Legend: ✅ verified, ❌ known incompatibility, ? not verified in this run, — not tested or not applicable.

What the columns mean:

- **upload** — publish a distribution to the server.
- **install** — install a package from the server.
- **resolve** — resolve dependencies against the server's index.
- **pep658-metadata** — read a file's metadata without downloading the whole wheel, for faster resolves.
- **yank** — honor yanked releases, skipping withdrawn versions.
- **hash-check** — verify downloads against expected hashes.
- **exclude-newer** — ignore releases newer than a chosen date.

## Client versions

| Client | Tested version |
| --- | --- |
| pip | venv-seeded |
| uv | system |
| poetry | 2.4.1 |
| pdm | 2.27.0 |
| twine | dev-dependency |
| flit | 3.12.0 |
| hatch | 1.17.0 |
| pipenv | 2026.5.2 |

<sub>Generated 2026-06-18 23:39:10 UTC from revision `59c3251`.</sub>
