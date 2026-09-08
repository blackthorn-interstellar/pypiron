# Install packages with external networking disabled

This experiment runs pypiron 0.0.17 and uv 0.12.3 against a private example
wheel and its public dependency, `six==1.17.0`.

1. With internet access, build the example wheel, publish it to pypiron, and
   install it once through pypiron to populate the package store.
2. Stop that container. Start a new container with `--network none`, a fresh
   Python environment, and the installer cache disabled. Install the same
   package and check its packaged resource and dependency. Also check that an
   unprepared public package cannot install.
3. After the server stops, copy its complete data directory to a new path.
   Verify both wheel hashes and the copied index, then repeat the installation
   in another container
   with `--network none`.

Both the server and client run inside each container. They can communicate
over loopback, but cannot reach PyPI, a host cache, or an external artifact
server. The script checks Docker's actual network mode and attempts an external
connection as a second check. Every install uses a new environment and
`uv --no-cache` with pypiron as its only index.

## Run it

Prerequisites: Docker with its daemon running, Bash, and internet access for
the preparation phase. Run from a pypiron checkout:

```bash
bash dev/scripts/offline-demo/run.sh
```

The initial image download/build takes longer than the experiment. The build
context includes only the example package and demo script. It does not include
your package data, credentials, or the private strategy repository.

Success prints `PASS: prepare`, `PASS: offline`, and `PASS: recover`. It also
prints the scratch directory containing both package stores and server logs.
The script removes its own containers on exit and leaves the image cached for
the next run. It never stops other servers or containers.

## Record it

Install [VHS](https://github.com/charmbracelet/vhs) and its dependencies. Then:

```bash
bash dev/scripts/offline-demo/record.sh
```

Preparation happens before the recording. The visible recording shows the
offline and recovery runs, including their real output. The tape adds pauses
for reading; its runtime is not a benchmark. The recorder writes a video and
GIF under `.local/offline-demo/` and reports the evidence directory.

## What this proves

The prepared wheel and dependency remain installable without external network
access, and a stopped disk-backed store can be copied to a new directory and
served by a fresh process. The check exercises both Python code and a packaged
text resource.

This is a small correctness demonstration. It does not establish throughput,
cross-machine portability, concurrent-backup safety, or recovery from corrupt
storage. Unprepared dependencies still require a connected preparation phase.
The image and both verification environments use the same Python and platform.

The demo disables advisory-feed refresh to avoid unrelated background network
traffic. The embedded malware block set and default release cooldown stay
enabled. An offline deployment needs an explicit process for refreshing its
advisory data; see the [air-gap guide](../docs/guides/air-gapped.md).
