# Artifact delivery

On download, pypiron either streams the bytes through the server or hands the
download straight to object storage. The default (`auto`) is right for almost
everyone — **do nothing.** Change it only if:

- Your server's outbound bandwidth is the bottleneck and clients can reach your
  bucket directly → `redirect`.
- Your clients *can't* reach the bucket — private subnet, firewalled storage →
  `stream`.

`auto` gives uv the fast path (straight from object storage) and keeps every
other client correct. pypiron picks the mode per client; you don't choose.

## The download URL never changes

Index pages hand out a stable artifact URL:

```text
/files/<pkg>/<filename>#sha256=...
```

That URL lands in a lockfile and a client's download cache. Keyed by content
hash, never expires. The delivery mode governs only what the server *does* when
a client fetches one — it never changes the URL, so lockfiles stay stable across
modes.

## The modes

| Mode | Behavior |
| --- | --- |
| `auto` *(default)* | Hand uv the download straight from object storage; stream the bytes for everyone else. |
| `redirect` | Always hand the download to object storage. The server never touches wheel bytes. |
| `stream` | Always proxy the bytes through the server with immutable cache headers. |

Unrecognized clients get the streaming path, so their download cache stays
correct.

## Choosing

| Pick | When |
| --- | --- |
| `auto` | Default. Cheap server bandwidth for uv, correct caching for everything else. |
| `redirect` | Server egress is the binding constraint and clients can reach the bucket directly. |
| `stream` | Clients can't reach the storage endpoint — private subnet, firewalled bucket — or you want every client's HTTP cache to stay effective. |

Set it with the flag or env var:

```bash
pypiron serve --storage s3 --s3-bucket acme-pkgs --artifact-delivery redirect
# or: PYPIRON_ARTIFACT_DELIVERY=redirect
```

## When the server always streams

Some setups always stream, whatever mode you set:

- The **disk** backend has no object store to hand off to.
- Some backends (GCS, Azure) need extra config before they can sign download
  URLs — see [Storage backends](storage.md).
- Tiny, resolution-critical metadata files always stream; a hand-off would only
  add a round trip.

See [Storage backends](storage.md) for which backends sign URLs, and
[Configuration](../reference/configuration.md#artifact-delivery) for the flag and
env var. Full read-path reasoning:
[DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md#read-path-zero-coordination).
