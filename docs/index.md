# pypiron

An ultra-fast Python package server, written in Rust.

One binary, no database. pypiron serves your private uploads, mirrors public PyPI
on demand, and bulk-syncs allowlists — all behind one URL and one namespace.
Truth is files on disk or object storage; indexes are regenerable views.

![Max sustained install throughput](assets/install-throughput.svg#only-light)
![Max sustained install throughput](assets/install-throughput-dark.svg#only-dark)

## Highlights

- **Handles 5–90× more load** than other PyPI servers.
- **Supply-chain quarantine, on by default.** Releases younger than a sliding
  7-day window are held back (`--exclude-newer`, tunable or `""` to disable);
  `uv --exclude-newer` resolves against it.
- **Works with the whole ecosystem.** uv, pip, poetry, twine, pipenv, hatch.
- **Horizontal scaling that just works.** Point any number of nodes at one
  bucket; reads need zero coordination.
- **Per-package download stats.** Per-package, per-version counts at
  `GET /stats/downloads/<pkg>` and `GET /stats/downloads`.
- **Private and public together.** One URL serves private packages and cached
  public dependencies.
- **Dependency-confusion defense.** Every name is exclusively private or
  mirrored, claimed at first write.

## Quickstart

```bash
uvx pypiron serve --admin-pass "$ADMIN"
```

Publish, then install — both against the one URL:

=== "uv"

    ```bash
    uv publish --publish-url http://localhost:8080/legacy/ \
      --username admin --password "$ADMIN" dist/*

    uv add --index http://localhost:8080/simple/ acme-widgets
    ```

=== "pip"

    ```bash
    twine upload --repository-url http://localhost:8080/legacy/ \
      --username admin --password "$ADMIN" dist/*

    pip install --extra-index-url http://localhost:8080/simple/ acme-widgets
    ```

## Next steps

<div class="grid cards" markdown>

- :material-download: __Installation__ — get the binary ([Installation](getting-started/installation.md))
- :material-rocket-launch: __First steps__ — publish & install ([First steps](getting-started/first-steps.md))
- :material-book-open: __Guides__ — four real setups ([Host private packages](guides/private-packages.md))
- :material-cog: __Configuration__ — every flag ([Configuration](reference/configuration.md))

</div>
