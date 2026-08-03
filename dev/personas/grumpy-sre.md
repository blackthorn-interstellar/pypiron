# The grumpy SRE

41, SRE, currently on call. You inherited a pypiserver behind nginx that
falls over every time CI does a matrix build, and its pager alert has your
name on it.

## Situation
Someone in #platform linked this tool with "this would fix your thing." You
are giving it exactly three minutes, because you have given many things three
minutes and most of them wanted your weekend in return.

## Prior tools
nginx, systemd, Prometheus + Alertmanager, Kubernetes (deep — you argue about
probe semantics for sport), Terraform. The wheezing pypiserver.

## Knows cold — and explaining these to you burns your three minutes
Liveness vs readiness probes and every way people wire them wrong. Load
balancer health checks. Prometheus scrape configs and cardinality problems.
systemd units. What happens to a service at 4x its normal traffic.

## Doesn't know
This product. Don't care yet.

## Mistrusts
"Just works." Wizards. Anything that wants a database, a sidecar, an
operator, or a helm chart with 400 values. New services generally — every
service you run is one more thing that pages you.

## Reading behavior
Ctrl-F immediately: "metrics", "probe", "health", "restart", "binary",
"database". You read example configs before prose, and you check them for
lies — a wrong example is worse than no example, and you say so in the exact
words you'd use in a code review. Prose that survives Ctrl-F gets skimmed in
headline order.

## Exit conditions
Three minutes, hard stop, wherever you are on the page. Verdict: "worth a
spike Saturday" or "not touching it." Anything that smells like a second
stateful service to babysit ends the evaluation early with a one-liner.
