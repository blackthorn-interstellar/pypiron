# Storage backends

pypiron keeps no database. The files on disk or in your object store *are* the
truth; the indexes are regenerable views. Pick where those files live with
`--storage`.

| Backend | Select with         | You provide                  | Best for                          |
| ------- | ------------------- | ---------------------------- | --------------------------------- |
| `disk`  | *(default)*         | a directory                  | single box, or a shared filesystem |
| `s3`    | `--storage s3`      | a bucket                     | AWS, MinIO, any S3-compatible store |
| `gcs`   | `--storage gcs`     | a bucket                     | Google Cloud Storage              |
| `azure` | `--storage azure`   | an account + container       | Azure Blob Storage                |

The three cloud backends share one implementation over the
[`object_store`](https://docs.rs/object_store) crate. They differ only in how you
name the destination and how credentials are supplied.

!!! warning "Buckets and containers must already exist"
    pypiron writes objects; it does not create the bucket or container. Provision
    it first, then point pypiron at it.

## disk

The zero-dependency default. No cloud account, no credentials.

```bash
pypiron serve --data-dir /var/lib/pypiron
```

`--data-dir` defaults to `~/.pypiron/packages`. Put it on a real filesystem the
process can write. A shared filesystem (NFS, EBS) works for a single node; for
multiple nodes behind one URL, use an object store, which coordinates leader
election on its own.

## s3 (and S3-compatible)

```bash
pypiron serve --storage s3 --s3-bucket acme-packages
export AWS_REGION=us-east-1
```

Credentials follow the standard AWS chain: `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` (plus `AWS_SESSION_TOKEN`), web identity, or instance
metadata. Point `--s3-endpoint-url` at MinIO or another S3-compatible store; an
`http://` endpoint is allowed automatically for local use.

## gcs

```bash
pypiron serve --storage gcs --gcs-bucket acme-packages
```

Credentials come from a service-account key (`--gcs-service-account-path`, or the
standard `GOOGLE_*` / `GOOGLE_APPLICATION_CREDENTIALS` envs), otherwise
Application Default Credentials.

!!! note "Presigned redirects need a service-account key"
    Signing artifact URLs needs the private key, which ADC tokens do not carry.
    Under ADC, pypiron streams artifact bytes through the node instead. See
    [Artifact delivery](artifact-delivery.md).

## azure

```bash
pypiron serve --storage azure \
  --azure-account acmestorage \
  --azure-container packages
```

Credentials come from an account access key (`--azure-access-key`, or the
standard `AZURE_*` envs), or a managed identity / bearer token.

!!! note "Presigned redirects need the account key"
    Signed SAS URLs are derived from the account key. Without it, pypiron streams
    artifact bytes through the node. See [Artifact delivery](artifact-delivery.md).

## Reference

Exact flags, env vars, and per-backend options live in
[Configuration → Storage](../reference/configuration.md#storage-serve). The
storage layout contract is in
[DESIGN.md](https://github.com/brycedrennan/pypiron/blob/master/dev/DESIGN.md).
