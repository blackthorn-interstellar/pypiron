//! Extra trust roots for outbound **upstream** TLS.
//!
//! In a corporate network the egress path is often a forwarding TLS proxy that
//! re-signs every connection with a private root CA (a "MITM" appliance). Direct
//! validation against the built-in webpki roots then fails, because the cert the
//! proxy presents is signed by that private CA, not a public one.
//!
//! `--upstream-ca-cert <pem-bundle>` (env `PYPIRON_UPSTREAM_CA_CERT`, on `serve`
//! and `sync`) points at the operator's CA bundle. It is loaded **once at
//! startup** — never at import time — into a process-global set of roots, read
//! by every upstream client builder ([`apply`]): the proxy upstream fetch, the
//! sync source client, and the advisory feed/probe clients. The certs *augment*
//! the built-in roots (reqwest `add_root_certificate` adds, it does not replace),
//! so a direct fetch of public PyPI keeps working while the corporate CA is also
//! trusted — the right behaviour in a MITM shop, where that CA already sees the
//! traffic.
//!
//! Loading is fail-closed: a missing, unreadable, certificate-free, or otherwise
//! unusable bundle is a hard startup error. The operator asked us to trust a
//! specific CA; we refuse to run pretending it is trusted when we could not load
//! it. The IMDS client (`node_region`) is deliberately *not* routed through here
//! — it talks plain HTTP to a link-local address and needs no extra roots.

use std::path::Path;
use std::sync::OnceLock;

use anyhow::{bail, Context as _, Result};
use reqwest::{Certificate, ClientBuilder};

/// The operator's extra upstream roots, set once by [`init`] at startup. Empty
/// (unset) means "no extra roots" — [`apply`] is then a no-op.
static ROOTS: OnceLock<Vec<Certificate>> = OnceLock::new();

/// Load the operator's PEM CA bundle once at startup. `None` (the flag unset)
/// leaves the roots empty and is a no-op. A bundle that cannot be read, does not
/// parse as PEM, contains no certificate, or is not a usable trust anchor is a
/// hard error — fail-closed, surfaced before the listener binds or a sync run
/// makes its first request, not lazily on the first upstream fetch.
pub fn init(path: Option<&Path>) -> Result<()> {
    let Some(path) = path else { return Ok(()) };
    let pem = std::fs::read(path)
        .with_context(|| format!("reading --upstream-ca-cert bundle {}", path.display()))?;
    let certs = Certificate::from_pem_bundle(&pem).with_context(|| {
        format!(
            "parsing --upstream-ca-cert bundle {} as PEM",
            path.display()
        )
    })?;
    if certs.is_empty() {
        bail!(
            "--upstream-ca-cert bundle {} contained no PEM certificates",
            path.display()
        );
    }
    // Force validation now by building a throwaway client with the roots applied:
    // a structurally broken certificate must fail here, at startup, rather than
    // surface as a mysterious TLS error on the first upstream fetch.
    add_roots(reqwest::Client::builder(), &certs)
        .build()
        .with_context(|| {
            format!(
                "--upstream-ca-cert bundle {} is not a usable trust anchor",
                path.display()
            )
        })?;
    // Set-once; the first bundle wins. A second startup call in one process only
    // happens in the test harness, never in the shipped single-command binary.
    let _ = ROOTS.set(certs);
    Ok(())
}

/// Augment `builder` with the operator's extra upstream roots, if any were
/// loaded. Called by every upstream client builder so a corporate CA reaches the
/// proxy fetch, the sync source client, and the advisory feed/probe alike.
pub fn apply(builder: ClientBuilder) -> ClientBuilder {
    match ROOTS.get() {
        Some(certs) => add_roots(builder, certs),
        None => builder,
    }
}

fn add_roots(mut builder: ClientBuilder, certs: &[Certificate]) -> ClientBuilder {
    for cert in certs {
        builder = builder.add_root_certificate(cert.clone());
    }
    builder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_none_is_a_noop() {
        // The common case (flag unset) loads nothing and never errors.
        init(None).unwrap();
    }

    #[test]
    fn init_rejects_a_missing_bundle() {
        let err = init(Some(Path::new("/nonexistent/pypiron-upstream-ca.pem"))).unwrap_err();
        assert!(
            err.to_string().contains("upstream-ca-cert"),
            "missing bundle must fail closed, got: {err}"
        );
    }

    #[test]
    fn init_rejects_a_certificate_free_bundle() {
        let path =
            std::env::temp_dir().join(format!("pypiron-ca-garbage-{}.pem", std::process::id()));
        std::fs::write(&path, b"this file holds no certificate\n").unwrap();
        let err = init(Some(&path)).unwrap_err();
        let _ = std::fs::remove_file(&path);
        assert!(
            err.to_string().contains("no PEM certificates"),
            "a bundle with no certs must fail closed, got: {err}"
        );
    }
}
