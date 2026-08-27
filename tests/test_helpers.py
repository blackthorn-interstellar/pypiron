"""Unit coverage for the suite's own helpers (no server, no network)."""

from __future__ import annotations

import ntpath

import pytest

from .helpers import CLIENT_EXCLUDE_NEWER, _client_env

# What `os.path.basename` yields on Windows for the UV env var the CI job sets;
# spelled via ntpath so the case is exercised on any host.
WINDOWS_UV = ntpath.basename(r"C:\Users\ci\.local\bin\uv.exe")


@pytest.mark.parametrize("argv0", ["uv", "/usr/local/bin/uv", WINDOWS_UV, "UV.EXE"])
def test_client_env_overrides_cooldown_for_every_uv_spelling(argv0):
    env = _client_env([argv0, "pip", "install", "six"], {"PATH": "/usr/bin"})
    assert env is not None
    assert env["UV_EXCLUDE_NEWER"] == CLIENT_EXCLUDE_NEWER
    assert env["PATH"] == "/usr/bin"


@pytest.mark.parametrize("args", [[], ["pip"], ["/usr/bin/python"], ["uvx"], ["nonuv"]])
def test_client_env_leaves_other_clients_alone(args):
    env = {"PATH": "/usr/bin"}
    assert _client_env(args, env) is env


def test_client_env_defaults_to_the_ambient_environment(monkeypatch):
    monkeypatch.setenv("PYPIRON_HELPERS_MARKER", "ambient")
    env = _client_env(["uv", "--version"], None)
    assert env is not None
    assert env["PYPIRON_HELPERS_MARKER"] == "ambient"
    assert env["UV_EXCLUDE_NEWER"] == CLIENT_EXCLUDE_NEWER
