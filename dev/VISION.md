# Vision

An ultra-fast, reliable, standards-compliant PyPI server for private registries that only serves static files. No database.

Truth lives in the packages tree: immutable artifacts plus write-time metadata sidecars (hashes, name/version, yank flags, extracted PEP 658 METADATA). The simple index (PEP 503 HTML, PEP 691 JSON) is a materialized view, idempotently regenerable from a storage listing. Views may lag truth but never lead it: artifact before index on upload, index before artifact on delete.

Upload and delete events drop dirty markers; a single worker rebuilds marked packages from listing and deletes markers last. A periodic full reconcile is the backbone — events merely accelerate it, so lost events self-heal. The global index regenerates only when the set of package names changes.

Reads need zero coordination and the server is fully cache-correct — filenames can never be re-uploaded, so artifacts are served `immutable`; indexes are ETag-revalidated; on cloud backends, redirect-safe clients (uv) are 302'd to presigned URLs so the node never touches wheel bytes (default `auto`; `redirect` does this for every client, `stream` for none). Client caches, proxies, or an optional CDN compound a single node's already-sufficient capacity. Disk-backed or cloud-backed (S3, Google Cloud Storage, Azure Blob).

For multi-node, only the index writer is singular: a conditional-write lease on the bucket, sloppy by design — rebuilds are idempotent, so split-brain merely duplicates work.

An optional synchronous upload mode waits for index visibility before returning 200, for publish-then-install CI pipelines.

Mirrored packages carry PyPI's original upload times in their sidecars, and every package is exclusively private or mirrored, claimed at first write.