---
description: Tamper-evident checkpoints for your PyPI server. An append-only, hash-chained log catches an attacker who rewrites an artifact and its checksum.
---

# Tamper-evident checkpoints

Everything pypiron serves is verified against a sha256 recorded next to each
file. But that record lives in your storage bucket. An attacker who steals your
storage credentials can rewrite an artifact **and** its recorded sha256 in one
motion — now every check agrees, because the evidence was changed too.

Tamper-evident checkpoints close that gap. As it runs, pypiron writes an
append-only, hash-chained log of every file's sha256. Each entry seals the one
before it, so the history can't be quietly rewritten after the fact. Later,
`pypiron verify-chain` replays that log against what your bucket holds now and
tells you if anything was swapped out from under it.

This is on by default. There is nothing to turn on.

## Catch a tampered file

Run it against the same storage your server uses:

```bash
pypiron verify-chain --config pypiron.toml
```

- **Exit 0** — every recorded file still matches its checkpoint. Nothing was
  touched.
- **Exit 1** — the record no longer holds. Each finding prints on its own line:

  ```
  hash-changed   acme-tools   acme_tools-2.1.0-py3-none-any.whl: committed abc… but the sidecar now holds def…
  vanished       acme-tools   acme_tools-1.0.0-py3-none-any.whl: committed sidecar is gone with no tombstone
  ```

  Most findings name a file that was altered or deleted out-of-band. One
  doesn't: `chain-diverged` names two buckets whose histories part company
  ([below](#when-two-buckets-disagree)).
- **Exit 2** — the check couldn't run (storage unreachable, bad config).

Files you uploaded since the last checkpoint aren't flagged — the log trails
live truth by at most one audit cycle, and a not-yet-recorded file is simply not
yet under watch. A file you deleted through pypiron is clean too: the deletion is
recorded, so verify-chain expects it gone.

Replacing a mirrored file with your own build is the one ordinary operation that
does report a change. The withdrawn file's disappearance is clean — pypiron
recorded that you pulled it — but the build now standing under that name holds
different bytes than the last checkpoint, and verify-chain says so until the next
audit records the swap. Read the row rather than dismissing it: once the swap is
recorded, the same row means some bucket is still serving the version you
withdrew.

## Every bucket, not just one

If you run across multiple buckets or regions, the checkpoint log rides along to
all of them, and `verify-chain` checks each one. It replays the fullest log it
finds and compares it against what **every** bucket holds — so a file rewritten
in one region is caught, not just the one your server happens to prefer. A bucket
that hasn't caught up yet is reported as lagging, not flagged as tampered. And
because the log travels with your data, losing a bucket and failing over to
another keeps the same unbroken history instead of quietly starting fresh.

## When two buckets disagree

`chain-diverged` means two buckets hold different entries at the same point, and
from there on they record two different histories. No file is named, because
none is implicated: each history is internally sound.

It takes a real split to get here — buckets that couldn't reach each other for
long enough that both kept recording. Both histories are evidence, so pypiron
does not pick one. Read the row: it names each bucket and where the two part.

Decide which bucket's history is yours — normally the one your writers were on.
Without a lock on `_transparency/`, copy that bucket's log over the other's and
the split is gone. Under Object Lock the losing entries can't be removed at all:
run verify-chain against the bucket you chose (`--buckets s3://…`) for a clean
check, and leave the split standing. Keeping it is what the lock is for.

Serving is unaffected either way — nothing reads the log to answer a request.

## Close the rollback gap

The chain proves nobody rewrote a file's history — as long as the history itself
survives. An attacker with storage credentials could instead delete old
checkpoints and start a fresh, clean-looking chain.

Deny that move by making the log write-once. On S3, enable **Object Lock**
(compliance or governance retention) on the `_transparency/` prefix. Checkpoints
can then be added but never deleted or overwritten for the retention window, so a
rollback is physically impossible and any tamper stays on the record.

Without a lock, verify-chain still catches every in-place rewrite; it just can't
promise the log wasn't rolled back. State that trade-off plainly to yourself:
**no lock, no rollback guarantee.**

## Nothing depends on the chain

The checkpoint log is pure evidence. The server never reads it to answer a
request, and indexes never derive from it. Delete the entire `_transparency/`
tree and the server keeps serving exactly as before — the history simply
restarts from empty, and the next audit begins a new chain. Turning checkpoints
off with `--transparency false` likewise changes nothing a client can see; it
only stops new links from being written.
