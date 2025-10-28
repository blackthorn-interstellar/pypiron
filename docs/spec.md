# MVP Technical Spec: S3‑Backed PyPI Server

## 1) Overview
- One container with a single API service and an in‑process background worker.
- S3 bucket is the source of truth for packages and generated indexes.
- Clients (`twine` for uploads, `pip` for downloads) talk only to the API.

---

## 2) S3 Bucket Layout
- `/packages/<package-name>/<filename.whl|tar.gz>` — package artifacts.
- `/simple/index.html` — global PEP 503 index.
- `/simple/index.json` — global PEP 691 index.
- `/simple/<package-name>/index.html` — per‑package PEP 503 index.
- `/simple/<package-name>/index.json` — per‑package PEP 691 index.
- `/_internal/queue/pending/` — job files created after uploads.
- `/_internal/queue/processing/` — jobs claimed by the worker.

---

## 3) API Service

### 3.1) Upload Endpoint
- **Route:** `POST /` (compatible with `twine upload`)
- **Auth:** HTTP Basic (single username/password via env).
- **Flow:**
  1. Validate `Authorization`; else `401 Unauthorized`.
  2. **Do not read the request body.**
  3. Generate a pre‑signed S3 **PUT** URL for the final path (e.g., `/packages/<name>/<filename>`).
  4. Respond `307 Temporary Redirect` with `Location` set to the pre‑signed URL.
  5. After responding, confirm upload exists via S3 `HEAD`.
  6. Write a job file to `/_internal/queue/pending/`, e.g.  
     `/_internal/queue/pending/<epoch>-<package>-<filename>.json`.

### 3.2) Download & Serving Endpoints
- **`GET /simple/`**
  - Serve `/simple/index.html` from S3.
- **`GET /simple/<package-name>/`** (and without trailing slash)
  - Serve `/simple/<package-name>/index.html` from S3.
- **`GET /files/<package-name>/<filename>`**
  - Stream `packages/<package-name>/<filename>` from S3 to client.
- Notes:
  - Endpoints proxy content (clients never access S3 directly).
  - Ensure `/simple/<name>` and `/simple/<name>/` both work.

---

## 4) Background Worker
- Runs in the same process as the API.
- Loop (configurable interval; default: 5 minutes):
  1. List up to **20** pending jobs under `/_internal/queue/pending/`.
  2. Move those job files to `/_internal/queue/processing/` (claim).
  3. Execute **Index Regeneration** (Section 5).
  4. On success, delete processed job files from `processing/`.
  5. Repeat.

---

## 5) Index Regeneration
1. **Aggregate jobs** → deduplicate to a set of affected package names.
2. **Per‑package indexes** (for each affected package):
   - List all artifacts under `/packages/<package-name>/`.
   - Generate:
     - `PEP 503` HTML at `/simple/<package-name>/index.html`.
     - `PEP 691` JSON at `/simple/<package-name>/index.json`.
   - Artifact links **must** point to API: `/files/<package-name>/<filename>`.
   - Overwrite both files in S3.
3. **Global indexes** (once per batch):
   - Enumerate available package directories to build package list.
   - Generate `/simple/index.html` (PEP 503) and `/simple/index.json` (PEP 691) with links to `/simple/<package-name>/`.
   - Overwrite both in S3.

---

## 6) Operational Notes
- Single container, no external DB.
- Do not expose S3/MinIO directly; all client traffic goes through the API.
- Paths must tolerate missing trailing slashes and `index.html` omission.
