# Artifact delivery

Index pages always hand clients a stable artifact URL:

```text
/files/<pkg>/<filename>#sha256=...
```

That URL is what lands in a lockfile and a client's download cache. It is keyed
by the content hash and never expires. `--artifact-delivery` only governs what
the server does when a client GETs one of those URLs — it never changes the URL
itself.

## The modes

| Mode | Behavior |
| --- | --- |
| `auto` *(default)* | Redirect clients verified immune to presigned-URL churn (uv); stream everyone else. |
| `redirect` | Always 302 to a presigned object-store URL. The node never touches wheel bytes. |
| `stream` | Always proxy the bytes through the node with immutable cache headers. |

Set it with the flag or the env var:

```bash
pypiron serve --storage s3 --s3-bucket acme-pkgs --artifact-delivery redirect
# or: PYPIRON_ARTIFACT_DELIVERY=redirect
```

## The tradeoff

A presigned redirect moves the megabytes to object storage — the node hands back
a 302 and the client pulls the wheel straight from the bucket. But each response
carries a freshly signed URL, so any client whose download cache is keyed by the
serving URL can never get a hit.

That is the whole tension:

- pip's HTTP cache is keyed by the URL it fetched. A fresh signed URL per request
  means every clean-environment `pip install` re-downloads the full wheel.
- uv keys its wheel cache by index plus filename, so it is immune — the signed URL
  changes, the cache hit stands.

`auto` resolves this per request: it inspects the client and redirects only the
ones verified safe (today, uv), streaming everyone else under the stable
`/files/` URL.

!!! note
    The redirect-safe list grows by verified cache behavior, not by client
    popularity. Unknown clients are assumed URL-keyed and get streamed bytes.

## When a mode is forced

Some configurations can't redirect at all and always stream, regardless of the
flag:

- The **disk** backend has no object store to sign against.
- **GCS under Application Default Credentials** — signing needs the private key,
  which ADC tokens don't carry. Provide a service-account key to enable redirects.
- **Azure without an account key** — SAS URLs are derived from the account key.
- **PEP 658 `.metadata` companions** always stream. They are tiny and
  resolution-critical, so a redirect would only add a round trip.

## Choosing

| Pick | When |
| --- | --- |
| `auto` | Default. Cheap node bandwidth for uv, correct caching for everything else. |
| `redirect` | Node egress is the binding constraint and clients can reach the bucket directly. |
| `stream` | Clients can't reach the storage endpoint — private subnet, firewalled bucket — or you want every client's HTTP cache to stay effective. |

See [Storage backends](storage.md) for which backends can sign URLs, and
[Configuration](../reference/configuration.md#artifact-delivery) for the flag and
its env var. The full read-path reasoning is in
[DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md#read-path-zero-coordination).
