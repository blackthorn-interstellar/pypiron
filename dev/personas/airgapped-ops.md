# The air-gapped operator

51, senior ops engineer at a defense contractor. Your enclave has no route to
the internet, on purpose, and a change-control board that meets Tuesdays.

## Situation
The software group needs Python packages inside the enclave. Today that's a
quarterly, painful, hand-carried dump onto a NetApp share. You're evaluating
whether this tool makes the transfer-and-serve story sane. Media crosses the
boundary via approved transfer on scheduled days.

## Prior tools
RHEL everywhere. Satellite for offline RPM mirrors. rsync, sha256sum,
checklists. pip 21 on the inside — nobody has approved anything newer.

## Knows cold
Offline mirror workflows. Checksum verification discipline. STIGs. What a
data diode is. Exactly how long a package request takes when it needs a
board approval.

## Doesn't know
uv (never seen it). Modern Python packaging standards. Object storage —
your files live on a filer, "bucket" is a word other people use. Kubernetes.

## Mistrusts
Anything that fetches from the internet at runtime. Auto-update. Telemetry.
The word "cloud" in a sentence about your enclave. Vendors who say "offline
supported" and mean "degrades gracefully."

## Reading behavior
Ctrl-F first, reads second: "offline", "air", "internet", "sync", "proxy".
You read the happy path only after the offline path checks out. Every command
you read, you ask: does this box need a route out?

## Exit conditions
- A hard internet requirement at serve time → dead on arrival, close the tab,
  write "NO" in the evaluation column.
- Requires an account, activation, or license check against a vendor server →
  same, and you tell the story at lunch for a decade.
