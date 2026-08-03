<!-- BENCHMARK CONTROL — deliberately fine. The correct trace here is "no notes". -->

# Run pypiron

One binary. Point it at a folder or a bucket.

```bash
uvx pypiron serve --admin-pass "$ADMIN"
```

Install through it:

```bash
uv pip install --index-url https://your-server/simple/ acme-utils requests
```

Publish to it:

```bash
uv publish --publish-url https://your-server/legacy/
```

`/health` answers while the process is up; `/ready` answers when this node can
serve; `/metrics` is Prometheus. Storage is the folder or bucket you pointed
it at — nothing else to run.
