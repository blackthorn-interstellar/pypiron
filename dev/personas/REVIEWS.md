# Documentation review

Reviewed September 6, 2026. This record covers the documentation in the same
commit. Earlier reviews remain in git history.

Three Sol agents split editing and verification. Separate agents then reviewed
each other's pages; the primary agent checked the resulting changes against the
owner's instructions and earlier documentation conversations.

| Pages | Reader check |
| --- | --- |
| README.md / docs/index.md | Clear benefits, bold claims, visible start/deploy actions, one-command start, prerequisite and password defined. |
| docs/compare/index.md and the three competitor pages | Clear fit, supported comparisons, concise evidence, working next actions. |
| docs/concepts.md | Private packages, public packages, storage, access, security, and operations in the reader's order. |
| docs/security.md | Accurate defaults, public/private scope, credential transport, and usable release verification. |
| docs/testing.md | Claims match the workflows and distinguish the client/backend coverage. |
| docs/privacy.md | Existing advertising disclosure retained; actual server and cloud-metadata requests explained. |
| docs/reference/configuration.md | Exact options and precedence, source-build availability, retained lookup anchors. |
| docs/for-agents.md | Decision criteria, setup, verification, operating facts, and limitations. |
| docs/guides/publish-install.md | Terminal and directory transitions, package build, publishing, installation, and alternative clients. |
| docs/guides/standard-cloud.md | Config and service files, credentials, startup, readiness, and client setup. |
| docs/guides/air-gapped.md | Complete dependency selection, staging, integrity checks, safe replacement, and advisory delivery. |
| docs/guides/multi-region.md | Shared config, failover limits, maintenance context, and recovery steps. |
| docs/guides/migrate.md | Source requirements, private-name selection, destination maintenance, and release availability. |

## Checks

- `make check`: passed; 755 Rust tests passed, five ignored.
- `uv run -- pytest tests/dev/scripts -n 0`: README website/PyPI link regressions passed.
- `make docs`: strict build passed; README generates the homepage.
- `uv run -- python dev/scripts/check_docs.py --bin target/debug/pypiron`:
  CLI, configuration reference, and config template agree.
- Generated HTML: internal links, fragment targets, image paths, descriptions,
  social metadata, search coverage, and llms.txt coverage checked.
- Public install: PyPI 0.0.17 installs with uvx. Its pypicloud migration flags
  are not released; both the guide and reference state the source-build requirement.
- Real-client workflows: building and publishing a wheel, fresh private and public
  installs, release-to-release migration, and offline archive verification and
  replacement passed. Cloud instructions were checked against configuration and
  implementation; no external cloud deployments were changed.
- Install conversion tracking: the exact inline script records one server-command
  copy event, ignores client-command copies, and prevents duplicate events. Both
  Material's current copy-button attributes and its legacy selector passed in an
  offline event harness; no advertising requests were sent.

Browser visual review could not run: the Browser runtime failed during setup
with `Importing module "node:process" is not allowed in node_repl`. Generated
HTML checks do not establish desktop or mobile rendering quality. The existing
theme, chart, product screenshot, and advertising pixel were retained.
