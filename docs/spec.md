# MVP Technical Spec: PypIron (Disk or S3) PyPI Server — **Current Implementation**

> This document reflects the behavior implemented in the code under `src/` as of version `0.1.0`.

---

## 0) What changed since earlier drafts

* **No redirect-based uploads.** Only the **legacy upload** flow is supported.
* **Uploads live at `/legacy` or `/legacy/`.** Compatible with `uv publish` and `twine`.
* The legacy endpoint **does not** use `X-Filename` or `?filename=`. The file part’s `filename` is used, or a `filename` text field if missing.
* **Worker default interval is 5 seconds** (not minutes).
* All index links are **relative** (`/files/...`, `/simple/...`). There is **no** `--public-base-url`.
* Removed unused `--upload-confirm-timeout-secs`.

---

## 1) Overview

* Single HTTP service plus an in‑process background worker.
* **Storage**: local filesystem (`disk`, default) or S3/S3‑compatible (`s3`).
* **Clients**: read PEP 503 (HTML) and PEP 691 (JSON) indexes.
* **Uploads**: legacy multipart to `/legacy`.

---

## 2) Logical Storage Layout

```
/packages/<project>/<filename.whl|tar.gz>

/simple/index.html          # PEP 503
/simple/index.json          # PEP 691

/simple/<project>/index.html
/simple/<project>/index.json

/_internal/queue/pending/   # jobs awaiting processing
/_internal/queue/processing/# jobs being processed
```

* **Disk**: rooted at `--data-dir` (default `./pypiron-data`)
* **S3**: keys stored in the configured bucket

---

## 3) API

### 3.1 Upload (Legacy) — **Supported**

**Routes**

* `POST /legacy`
* `POST /legacy/`

**Auth**

* HTTP Basic is required **only if configured** (both user & pass provided). If unset, uploads are unauthenticated.

**Request**

* `multipart/form-data`

  * File field: `content` or `file` (uses the part’s `filename`)
  * Optional text field: `filename` (used only if the part lacks a filename)

**Behavior**

1. Write file bytes to:

```
packages/<pep503-normalized-name>/<filename>
```

2. Enqueue a job under:

```
/_internal/queue/pending/<epoch>-<package>-<filename>.json
```

with JSON like:

```json
{
  "package": "<name>",
  "filename": "<filename>",
  "s3_key": "packages/<name>/<filename>",
  "uploaded_at": "RFC3339 timestamp"
}
```

**Response**

* `200 OK` with body `OK`.

**Examples**

```bash
uv publish --publish-url http://localhost:8080/legacy/ -u <user> -p <pass> dist/*.whl
twine upload --repository-url http://localhost:8080/legacy/ -u <user> -p <pass> dist/*
```

---

### 3.2 Simple Index (Read)

**Global**

* `GET /simple` and `GET /simple/` — HTML by default, JSON if `Accept` includes `application/vnd.pypi.simple.v1+json` or `application/json`.
* `GET /simple/index.json` — always JSON.

**Per‑project**

* `GET /simple/<project>` and `GET /simple/<project>/` — HTML by default, JSON via content negotiation.
* `GET /simple/<project>/index.json` — always JSON.

On startup, the server creates empty `/simple/index.html` and `/simple/index.json` if missing.

---

### 3.3 Artifact Download

* `GET /files/<project>/<filename>` — streams the artifact.
* Sets `Content-Type` best‑effort and `Content-Length` when known.

---

### 3.4 Logging & Fallback

* Logs each request (method + URI) and response status.
* Unmatched routes return `404 Not Found`.

---

## 4) Background Worker

* **Interval:** `--worker-interval-secs` (default **5**)
* **Batch size:** `--job-batch-size` (default **20**)

**Tick**

1. List up to *batch size* jobs in `/_internal/queue/pending/` (JSON files only).
2. Move each to `/_internal/queue/processing/` (claim).
3. Determine affected packages from job JSON (or infer from filename as a fallback).
4. For each touched package:

   * List artifacts under `/packages/<package>/`.
   * Compute **SHA256** for each artifact.
   * Regenerate per‑package PEP 503 HTML and PEP 691 JSON.
5. Once per batch, rebuild **global** PEP 503/691 indexes.
6. Delete processed jobs from `processing/`.

---

## 5) Index Shapes

**Per‑package PEP 503 HTML**

* Minimal page with `<a href="/files/<package>/<filename>">` per file.

**Per‑package PEP 691 JSON**

```json
{
  "meta": {"api-version": "1.0"},
  "name": "<package>",
  "files": [
    {
      "filename": "<filename>",
      "url": "/files/<package>/<filename>",
      "hashes": {"sha256": "<hex>"},
      "yanked": null
    }
  ]
}
```

**Global PEP 503 HTML**

* Minimal page with `<a href="/simple/<package>/">` per project.

**Global PEP 691 JSON**

```json
{
  "meta": {"api-version": "1.0"},
  "projects": [
    {"name": "<package>", "url": "/simple/<package>/"}
  ]
}
```

---

## 6) Configuration (CLI & Env)

**Storage**

* `--storage {disk|s3}` / `PYPIRON_STORAGE` (default: `disk`)
* `--data-dir PATH` / `PYPIRON_DATA_DIR` (`disk`; default: `./pypiron-data`)
* `--s3-bucket NAME` / `PYPIRON_S3_BUCKET` (**required for `s3`**)
* `--aws-region STR` / `AWS_REGION` (optional)
* `--s3-endpoint-url URL` / `PYPIRON_S3_ENDPOINT_URL` (S3‑compatible)
* `--s3-force-path-style` / `PYPIRON_S3_FORCE_PATH_STYLE` (default: `false`)

**Auth**

* `--basic-auth-user STR` / `PYPIRON_BASIC_AUTH_USER` (optional)
* `--basic-auth-pass STR` / `PYPIRON_BASIC_AUTH_PASS` (optional)

  If **both** are set, uploads require HTTP Basic; otherwise uploads are unauthenticated.

**Worker / Server**

* `--worker-interval-secs N` / `PYPIRON_WORKER_INTERVAL_SECS` (default: **5**)
* `--job-batch-size N` / `PYPIRON_JOB_BATCH_SIZE` (default: **20**)
* `--bind-addr HOST:PORT` / `PYPIRON_BIND_ADDR` (default: `0.0.0.0:8080`)

**AWS credentials**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`.

---

## 7) Name Normalization & Filename Inference

* **PEP 503 normalization**: lowercase; collapse runs of `[-_.]` to `-`.
* **Package inference from filename**:

  * Wheel: `<dist>-<version>-…` → `dist`
  * sdist: `dist-<version>.tar.*` → `dist`
  * Fallback: split on last `-` if possible

---

## 8) Compatibility

**Install with pip**

```bash
pip install --index-url http://localhost:8080/simple/ <pkg>
```

**Upload**

```bash
uv publish --publish-url http://localhost:8080/legacy/ -u <user> -p <pass> dist/*
twine upload --repository-url http://localhost:8080/legacy/ -u <user> -p <pass> dist/*
```

**Not supported**

* Raw POST uploads to `/` with `?filename=...`
* Presigned S3 PUT/redirect flows

---

## 9) References

* PEP 503 — Simple Repository API (HTML)
* PEP 691 — Simple Repository API (JSON)
* Warehouse legacy API
