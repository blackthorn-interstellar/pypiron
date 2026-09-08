FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea

RUN pip install --no-cache-dir uv==0.12.3 pypiron==0.0.17
COPY examples/package-ci /example
RUN uv build --wheel --out-dir /wheels /example
COPY dev/scripts/offline-demo/inside.sh /inside.sh
WORKDIR /demo

# Nothing in the demo needs privilege. run.sh overrides this with the host's own
# uid:gid so the bind-mounted evidence directory stays writable and does not come
# back root-owned; 65534 (nobody) is the default when someone runs the image by
# hand. HOME is set because neither uid has a usable home directory, and uv wants
# one.
ENV HOME=/tmp
USER 65534:65534

ENTRYPOINT ["bash", "/inside.sh"]
