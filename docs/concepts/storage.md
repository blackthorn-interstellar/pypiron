# Storage backends

Choose where your packages live — local disk, S3, GCS, or Azure — and scale from
one box to many. Select the backend with `--storage`.

| Backend | Select with         | You provide                  | Best for                          |
| ------- | ------------------- | ---------------------------- | --------------------------------- |
| `disk`  | *(default)*         | a directory                  | single box, or a shared filesystem |
| `s3`    | `--storage s3`      | a bucket                     | AWS, MinIO, any S3-compatible store |
| `gcs`   | `--storage gcs`     | a bucket                     | Google Cloud Storage              |
| `azure` | `--storage azure`   | an account + container       | Azure Blob Storage                |

The three cloud backends behave identically — they differ only in naming and
credentials.

!!! warning "Buckets and containers must already exist"
    pypiron writes objects; it does not create the bucket or container. Provision
    it first, then point pypiron at it.

## disk

**Pick this if** you want a zero-setup default — no cloud account, no
credentials.

```bash
pypiron serve --data-dir /var/lib/pypiron
```

`--data-dir` defaults to `~/.pypiron/packages`. Put it on a real filesystem the
process can write. A shared filesystem (NFS, EBS) works for a single node; to run
several nodes behind one URL, point them all at an object store. No coordination.

## s3 (and S3-compatible)

**Pick this if** you're on AWS or any S3-compatible store (MinIO, and friends).

```bash
pypiron serve --storage s3 --s3-bucket acme-packages
export AWS_REGION=us-east-1
```

Credentials follow the standard AWS chain: `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` (plus `AWS_SESSION_TOKEN`), web identity, or instance
metadata. Point `--s3-endpoint-url` at MinIO or another S3-compatible store; an
`http://` endpoint is allowed for local use.

## gcs

**Pick this if** you're on Google Cloud.

```bash
pypiron serve --storage gcs --gcs-bucket acme-packages
```

Credentials come from a service-account key (`--gcs-service-account-path`, or the
standard `GOOGLE_*` / `GOOGLE_APPLICATION_CREDENTIALS` envs), otherwise
Application Default Credentials.

!!! note
    A service-account key also hands downloads straight to storage; without one,
    downloads stream through the node. See
    [Artifact delivery](artifact-delivery.md).

## azure

**Pick this if** you're on Azure (including AKS).

```bash
pypiron serve --storage azure \
  --azure-account acmestorage \
  --azure-container packages
```

Credentials come from an account access key (`--azure-access-key`, or the
standard `AZURE_*` envs), or a managed identity / bearer token.

!!! note
    An account key also hands downloads straight to storage; without it,
    downloads stream through the node. See
    [Artifact delivery](artifact-delivery.md).

## Reference

Exact flags, env vars, and per-backend options live in
[Configuration → Storage](../reference/configuration.md#storage-serve). The
storage layout contract is in
[DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md).
