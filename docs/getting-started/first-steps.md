# First steps

The whole loop: start a server, publish a package, install it back. Three
commands, one terminal.

## 1. Start a server

```bash
uvx pypiron serve --admin-pass secret
```

This serves on `http://localhost:8080`. Your admin credential is `admin` /
`secret` — (username defaults to `admin`).

- The root URL (`http://localhost:8080`) is a web dashboard with copy-paste
  client config.
- `GET /health` is an unauthenticated probe for load balancers.


## 2. Publish a package

Build your distributions to `dist/`, then upload to the legacy API at `/legacy/`.

=== "uv"
    ```bash
    uv publish --publish-url http://localhost:8080/legacy/ \
      --username admin --password secret dist/*
    ```

=== "twine"
    ```bash
    twine upload --repository-url http://localhost:8080/legacy/ \
      -u admin -p secret dist/*
    ```

## 3. Install it back

Install from the simple index at `/simple/`.

=== "uv"
    ```bash
    uv add --index http://localhost:8080/simple/ acme-widgets
    ```

=== "pip"
    ```bash
    pip install --index-url http://localhost:8080/simple/ acme-widgets
    ```

## Next

- Real setups — auth, a single public + private index, S3: see the
  [guides](../guides/private-packages.md).
- Every flag and its `PYPIRON_*` env var: see
  [Configuration](../reference/configuration.md).
