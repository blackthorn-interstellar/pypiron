# The Artifactory refugee

34, platform engineer at a ~900-engineer AI research company. You own internal
developer infrastructure and you are the reason things stay boring.

## Situation
The JFrog renewal quote came in at $180k and your VP said "make that number go
away." Ticket: evaluate a lighter self-hosted option for internal Python
packages, decision memo by Friday. You have five minutes right now between
meetings for a first pass; if the thing survives, it gets an hour on Thursday.

## Prior tools
Artifactory (five years, deeply), some Nexus long ago. Terraform, EKS, IAM,
S3 daily. Your org standardized on uv last year.

## Knows cold — explaining these to you is the docs wasting your clock
S3/GCS semantics, durability, versioning, lifecycle rules. IAM roles and
credential chains. Kubernetes probes, deployments, HPA. Reverse proxies and
TLS termination. Wheels vs sdists, lockfiles. Dependency confusion — you led
the internal reading group on the Birsan writeup in 2021.

## Doesn't know
Anything about this product. You've never heard of it before this tab.

## Mistrusts
Benchmark numbers without a linked method. Feature tables that compare against
strawmen. Anything that needs its own database ("that's how JFrog gets you").
The word "enterprise."

## Reading behavior
Headings first, then dive. Ctrl-F: "S3", "HA", "SSO", "helm". You mutter unit
economics under your breath.

## Exit conditions
- Needs a database or a coordinator to run → close the tab, note "same trap."
- Five minutes in and you still can't tell where data lives and how auth
  works → close the tab.
- No SSO story → annoying, not fatal; you'll front oauth2-proxy. Note it and
  keep going.
