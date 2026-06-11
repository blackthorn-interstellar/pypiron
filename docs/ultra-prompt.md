ultracode

  Goal: implement PypIron per the design docs, working through every roadmap
  milestone until done.

  Read first, in order: docs/VISION.md, docs/DESIGN.md, docs/STANDARDS.md,
  docs/TESTING.md, docs/ROADMAP.md. These documents are the authoritative
  contract. The code is not.

  Greenfield rules:
  - This is greenfield. There are no users and no backwards-compatibility
    obligations. Give zero deference to existing code — it is a sketch, not an
    investment. Rewrite or delete anything that conflicts with the design,
    including endpoints, storage conventions, the queue, and old test scripts.
    Where the implementation disagrees with the docs, the implementation is
    wrong.
  - The one fixed contract is the "Storage layout (the contract)" section of
    DESIGN.md: implement exactly that tree and sidecar schema. Do not invent
    variants. If you discover the layout itself is flawed, update DESIGN.md
    first, then implement the corrected version.

  Execution:
  - Work through ROADMAP.md milestones strictly in order, starting at
    milestone 0. Each milestone's acceptance criterion is a blackbox test per
    TESTING.md: the real compiled binary run as a subprocess, driven over HTTP
    by real clients (uv, pip, twine) with real wheels fetched from public PyPI,
    against both disk and MinIO-S3 backends. A milestone without its passing
    blackbox test is not done.
  - After every change run `make check` and the relevant tests; everything
    green before moving on. Commit each completed milestone separately with a
    conventional-commit message, and update STANDARDS.md statuses and the
    ROADMAP.md "Current state" section in the same commit so the docs always
    reflect reality.
  - Hold the design tenets in every decision: truth lives in the packages
    tree; views are idempotently regenerable and may lag truth but never lead
    it; capture metadata at write time, never compute it at read or rebuild
    time; no database; no new moving parts. When two designs work, pick the
    boring one.

  Definition of done: ROADMAP milestones 0 through 12 implemented and each
  verified by its blackbox test; `make check` and the full suite green; docs
  updated to match the shipped reality.