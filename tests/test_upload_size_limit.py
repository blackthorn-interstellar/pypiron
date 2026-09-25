"""The upload size limit depends on the credential: 1 GiB for an uploader, 5 GiB
for the admin credential `sync` mirrors with, so CUDA-class wheels (1–3 GB)
can be mirrored while ordinary publishing keeps the tighter cap."""

from __future__ import annotations

import base64
import hashlib
import http.client
import uuid
import zipfile

from .helpers import wait_for_file_in_index

GIB = 1024 * 1024 * 1024


def _auth(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def _post_headers_only(bind: str, auth: str, length: int) -> int:
    """Declare a body of `length` bytes, send none, and return the status."""
    conn = http.client.HTTPConnection(bind, timeout=10)
    try:
        conn.putrequest("POST", "/legacy/")
        conn.putheader("Authorization", auth)
        conn.putheader("Content-Type", "multipart/form-data; boundary=x")
        conn.putheader("Content-Length", str(length))
        conn.endheaders()
        return conn.getresponse().status
    finally:
        conn.close()


def test_upload_over_the_credential_limit_is_refused_before_the_body(disk_server):
    bind = disk_server["bind"]
    uploader = _auth(disk_server["uploader_user"], disk_server["uploader_password"])
    admin = _auth(disk_server["admin_user"], disk_server["admin_password"])
    assert _post_headers_only(bind, uploader, GIB + 1) == 413
    assert _post_headers_only(bind, admin, 5 * GIB + 1) == 413


def _write_big_wheel(path, name: str, version: str, payload: int) -> None:
    di = f"{name}-{version}.dist-info"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as zf:
        with zf.open(zipfile.ZipInfo(f"{name}/payload.bin"), "w", force_zip64=True) as f:
            chunk = b"\0" * (1 << 20)
            for _ in range(payload // len(chunk)):
                f.write(chunk)
        zf.writestr(f"{di}/METADATA", f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        zf.writestr(f"{di}/WHEEL", "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        zf.writestr(f"{di}/RECORD", "")


def test_admin_uploads_a_wheel_over_one_gib(disk_server, tmp_path):
    name, version = "hugewheel", "1.0"
    wheel = tmp_path / f"{name}-{version}-py3-none-any.whl"
    _write_big_wheel(wheel, name, version, GIB + (16 << 20))
    size = wheel.stat().st_size
    assert size > GIB

    digest = hashlib.sha256()
    with open(wheel, "rb") as f:
        while block := f.read(1 << 20):
            digest.update(block)
    boundary = uuid.uuid4().hex
    fields = {
        ":action": "file_upload",
        "protocol_version": "1",
        "name": name,
        "version": version,
        "sha256_digest": digest.hexdigest(),
    }
    head = (
        b"".join(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode()
            for k, v in fields.items()
        )
        + (
            f'--{boundary}\r\nContent-Disposition: form-data; name="content"; '
            f'filename="{wheel.name}"\r\nContent-Type: application/octet-stream\r\n\r\n'
        ).encode()
    )
    tail = f"\r\n--{boundary}--\r\n".encode()

    conn = http.client.HTTPConnection(disk_server["bind"], timeout=300)
    try:
        conn.putrequest("POST", "/legacy/")
        conn.putheader(
            "Authorization", _auth(disk_server["admin_user"], disk_server["admin_password"])
        )
        conn.putheader("Content-Type", f"multipart/form-data; boundary={boundary}")
        conn.putheader("Content-Length", str(len(head) + size + len(tail)))
        conn.endheaders()
        conn.send(head)
        with open(wheel, "rb") as f:
            while block := f.read(1 << 20):
                conn.send(block)
        conn.send(tail)
        resp = conn.getresponse()
        assert resp.status == 200, resp.read()[:500]
    finally:
        conn.close()

    doc = wait_for_file_in_index(disk_server["simple"], name, wheel.name)
    (listed,) = [f for f in doc["files"] if f["filename"] == wheel.name]
    assert listed["size"] == size
