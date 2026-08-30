//! Offline, independent verification of relayed PEP 740 provenance.
//!
//! A mirror-origin `.provenance` companion carries PyPI's Sigstore attestation
//! bundle(s). `src/provenance.rs` relays and digest-binds them without crypto;
//! this module goes the last mile — it verifies a bundle *itself*, offline,
//! against a Sigstore trust root **baked into the binary** (`src/assets/`, the
//! same play as the embedded malware floor). No TUF updater, no runtime fetch,
//! no user key: an air-gapped pypiron confirms the original publisher on its own.
//!
//! A full pass proves, for the exact bytes this server stores:
//!   1. DSSE — the in-toto statement is signed by the bundle's Fulcio leaf cert
//!      (ECDSA-P256 over the DSSE PAE encoding).
//!   2. Chain — that leaf chains to an embedded Fulcio CA (ECDSA-P384/SHA-384),
//!      with code-signing EKU, digital-signature key usage, non-CA basic
//!      constraints, and validity evaluated **at the Rekor integrated time**
//!      (keyless leaf certs live ~10 minutes; wall-clock would reject them all).
//!   3. SCT — the certificate was logged: its embedded Signed Certificate
//!      Timestamp verifies against an embedded CT-log key (RFC 6962 precert).
//!   4. Transparency — the logged entry's body is over *this* DSSE signature and
//!      payload, and its Signed Entry Timestamp (SET) verifies against the
//!      embedded Rekor log key named by the entry's log id (ECDSA-P256 for the v1
//!      log, Ed25519 for "Log2025"), within that key's validity window.
//!   5. Binding — the statement's subject sha256 equals the served artifact's.
//!
//! On a full pass we return the signer identity read from the **certificate**
//! (its SAN plus the Fulcio OIDC-issuer / source-repo OID extensions) — signed
//! material. We never treat the unsigned `attestation_bundles[].publisher` JSON
//! as verified: it is an attacker-settable sibling of the signature, so a valid
//! bundle over these bytes proves *who Fulcio issued the cert to*, and nothing
//! about the `publisher` field. This is the cosign `--certificate-identity` /
//! sigstore-python identity discipline: a bundle with no identity check proves
//! authenticity of nobody in particular. We surface the cert identity and leave
//! the "is this the rightful owner?" judgement to the human viewer — pypiron has
//! no per-package expected-publisher policy.
//!
//! Everything here is **fail-closed and best-effort**: any missing field,
//! malformed DER, unknown algorithm, expired-at-integrated-time cert, absent log
//! key, a trust root newer than the embedded one, or an oversized/adversarial
//! bundle resolves to `None` (not verified) — never a panic, never fail-open.
//! The caller then falls back to the relayed + digest-bound labeling.

use std::sync::OnceLock;

use base64::Engine;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use signature::Verifier;
use x509_cert::der::{Decode, Encode};

/// OID 1.3.6.1.5.5.7.3.3 — id-kp-codeSigning (the EKU Fulcio leaf certs carry).
const OID_EKU_CODE_SIGNING: &str = "1.3.6.1.5.5.7.3.3";
/// OID 1.3.6.1.4.1.11129.2.4.2 — embedded SignedCertificateTimestampList.
const OID_SCT_LIST: &str = "1.3.6.1.4.1.11129.2.4.2";
/// OID 2.5.29.15 — keyUsage. 2.5.29.19 — basicConstraints. 2.5.29.37 — EKU.
const OID_KEY_USAGE: &str = "2.5.29.15";
const OID_BASIC_CONSTRAINTS: &str = "2.5.29.19";
const OID_EXT_KEY_USAGE: &str = "2.5.29.37";
/// OID 2.5.29.17 — subjectAltName (the signer identity Fulcio bound the cert to).
const OID_SUBJECT_ALT_NAME: &str = "2.5.29.17";
/// Fulcio identity extensions: .8 OIDC issuer (V2, DER UTF8String), .1 the
/// deprecated bare-string issuer, .12 source repository URI (DER UTF8String).
const OID_FULCIO_ISSUER_V2: &str = "1.3.6.1.4.1.57264.1.8";
const OID_FULCIO_ISSUER_V1: &str = "1.3.6.1.4.1.57264.1.1";
const OID_FULCIO_SOURCE_REPO: &str = "1.3.6.1.4.1.57264.1.12";

/// DSSE payload type for an in-toto v1 statement — part of the PAE the leaf
/// signature covers.
const DSSE_PAYLOAD_TYPE: &[u8] = b"application/vnd.in-toto+json";

/// Defensive upper bound on the provenance bytes we will parse at render. A real
/// PEP 740 object is a few KB; ingest already caps it, this bounds render work
/// regardless of what is stored.
const MAX_PROVENANCE_BYTES: usize = 4 * 1024 * 1024;

/// How many attestations one provenance object may have verified. PyPI relays
/// one or two per file; the cap only ever bites on an object built to make a
/// project page grind through thousands of cert chains.
const MAX_ATTESTATIONS: usize = 32;

/// The signer identity a full verification proves — extracted from the *leaf
/// certificate*, which is signed material, never from the unsigned `publisher`
/// JSON. This is *who Fulcio issued the signing cert to*, not a claim that the
/// signer is the package's rightful owner (pypiron has no per-package expected-
/// publisher policy to check against — the human viewer makes that judgement).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSigner {
    /// The certificate's Subject Alternative Name — the identity Fulcio bound the
    /// cert to (e.g. a GitHub Actions workflow ref URI). Always present on a pass.
    pub identity: String,
    /// The OIDC issuer that authenticated the signer (Fulcio OID
    /// 1.3.6.1.4.1.57264.1.8, else the deprecated .1), e.g.
    /// `https://token.actions.githubusercontent.com`.
    pub issuer: Option<String>,
    /// Source repository URI (Fulcio OID 1.3.6.1.4.1.57264.1.12) when present —
    /// friendlier for display than the raw workflow SAN.
    pub source_repo: Option<String>,
}

/// Verify every attestation in a relayed provenance object, returning the
/// cert-derived signer identity of the first that fully verifies against the
/// embedded trust root and binds to `artifact_sha256`. `None` means "not
/// verified" — fail-closed on any missing field, malformed input, or failed
/// check. Best-effort and panic-free on any input.
pub fn verify(provenance_bytes: &[u8], artifact_sha256: &str) -> Option<VerifiedSigner> {
    // Defensive cap: this runs at page render on the stored `.provenance`
    // companion. Ingest bounds it (4 MiB on the sync/legacy upload path, 16 MiB
    // on the proxy-fill path), and a real bundle is a few KB — so refuse to crunch
    // anything larger, whatever produced it, rather than burn CPU/RAM per render.
    if provenance_bytes.len() > MAX_PROVENANCE_BYTES {
        return None;
    }
    // No usable embedded root (asset unreadable): fail closed.
    let root = embedded_trust_root()?;
    let want = artifact_sha256.trim();
    if want.is_empty() {
        return None;
    }
    let v: Value = serde_json::from_slice(provenance_bytes).ok()?;
    v.get("attestation_bundles")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|b| b.get("attestations").and_then(Value::as_array))
        .flatten()
        // Second defensive cap, on *count* rather than bytes: within the 4 MiB
        // ceiling above a hostile object can still pack thousands of small
        // attestations, and each one costs a cert parse, a chain walk, an SCT
        // check and two ECDSA verifies. A real bundle carries one or two.
        .take(MAX_ATTESTATIONS)
        .find_map(|att| verify_attestation(att, want, root))
}

/// Verify one attestation end to end, returning its cert-derived signer identity
/// on a full pass. Any step that cannot be completed returns `None`.
fn verify_attestation(
    att: &Value,
    artifact_sha256: &str,
    root: &TrustedRoot,
) -> Option<VerifiedSigner> {
    let vm = att.get("verification_material")?;
    let env = att.get("envelope")?;

    // Leaf certificate (base64 DER), the DSSE payload (base64 in-toto statement)
    // and signature (base64 DER ECDSA).
    let leaf_der = vm
        .get("certificate")
        .and_then(Value::as_str)
        .and_then(b64)?;
    let payload = env.get("statement").and_then(Value::as_str).and_then(b64)?;
    let dsse_sig = env.get("signature").and_then(Value::as_str).and_then(b64)?;
    let leaf = x509_cert::Certificate::from_der(&leaf_der).ok()?;

    // The transparency entry fixes the moment to evaluate certificate validity —
    // a keyless leaf is valid for ~10 minutes and is long expired by wall clock.
    let entry = vm
        .get("transparency_entries")
        .and_then(Value::as_array)
        .and_then(|a| a.first())?;
    let integrated_time = entry.get("integratedTime").and_then(as_i64)?;

    // 1. Subject digest binds to the served bytes (the relayed integrity match,
    // now a required step of the full proof).
    if !statement_binds(&payload, artifact_sha256) {
        return None;
    }

    // 2. Chain the leaf to an embedded Fulcio CA and gather the issuer for SCT.
    let issuer_spki = verify_chain(&leaf, root, integrated_time)?;

    // 3. Leaf constraints: code-signing EKU, digital-signature usage, not a CA.
    if !leaf_constraints_ok(&leaf) {
        return None;
    }

    // 4. SCT: the certificate was logged (embedded precert SCT vs a CT-log key).
    if !verify_sct(&leaf, &issuer_spki, root) {
        return None;
    }

    // 5. DSSE: the leaf signed the statement (ECDSA-P256 over the PAE).
    let leaf_spki = leaf.tbs_certificate.subject_public_key_info.to_der().ok()?;
    if !ecdsa_verify(&leaf_spki, &dsse_pae(&payload), &dsse_sig) {
        return None;
    }

    // 6. The logged entry is over THIS DSSE signature and payload, recorded
    // against THIS leaf cert — so the SET we verify next provably covers this
    // attestation and this signer, not some unrelated entry.
    if !entry_binds_attestation(entry, &leaf_der, &payload, &dsse_sig) {
        return None;
    }

    // 7. Rekor SET: the entry is in a transparency log we trust, at that time.
    if !verify_set(entry, root, integrated_time) {
        return None;
    }

    // Everything held: return the identity drawn from the VERIFIED certificate,
    // never the attacker-settable `publisher` JSON.
    signer_identity(&leaf)
}

/// True if the Rekor entry's `canonicalizedBody` (a `dsse` v0.0.1 entry) is over
/// exactly this attestation: its `payloadHash` is the sha256 of our DSSE payload,
/// and it carries a signature that is our envelope signature **recorded against
/// our leaf certificate** (`verifier`). Binding the entry's recorded signer to
/// the cert we display — as cosign/sigstore-python do — closes the gap where the
/// entry we cite could name a different signer than the certificate shown.
fn entry_binds_attestation(
    entry: &Value,
    leaf_der: &[u8],
    payload: &[u8],
    dsse_sig: &[u8],
) -> bool {
    let Some(body) = entry
        .get("canonicalizedBody")
        .and_then(Value::as_str)
        .and_then(b64)
    else {
        return false;
    };
    let Ok(body) = serde_json::from_slice::<Value>(&body) else {
        return false;
    };
    let Some(spec) = body.get("spec") else {
        return false;
    };
    // payloadHash (sha256 hex) must equal sha256(our DSSE payload).
    let want_payload_hash = hex_lower(&Sha256::digest(payload));
    let payload_hash_ok = spec
        .pointer("/payloadHash/value")
        .and_then(Value::as_str)
        .is_some_and(|v| v.eq_ignore_ascii_case(&want_payload_hash));
    if !payload_hash_ok {
        return false;
    }
    // Some signature entry must be OUR envelope signature AND record OUR leaf cert
    // as its verifier — both in the same entry, so signature and cert are bound.
    let want_sig = base64::engine::general_purpose::STANDARD.encode(dsse_sig);
    spec.get("signatures")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|s| {
            let sig_ok = s
                .get("signature")
                .and_then(Value::as_str)
                .is_some_and(|v| v.trim() == want_sig);
            sig_ok && verifier_is_leaf(s.get("verifier"), leaf_der)
        })
}

/// True if an entry signature's `verifier` field is a PEM certificate whose DER
/// equals our verified leaf certificate. (Rekor `dsse` entries record the signer
/// as the base64 of the leaf cert's PEM.)
fn verifier_is_leaf(verifier: Option<&Value>, leaf_der: &[u8]) -> bool {
    use x509_cert::der::DecodePem;
    verifier
        .and_then(Value::as_str)
        .and_then(b64)
        .and_then(|pem| x509_cert::Certificate::from_pem(&pem).ok())
        .and_then(|c| c.to_der().ok())
        .is_some_and(|der| der == leaf_der)
}

/// Draw the signer identity from a VERIFIED leaf certificate: its SAN (required)
/// plus the Fulcio OIDC-issuer and source-repository OID extensions. Returns
/// `None` if no SAN identity can be read — with nothing to attribute the
/// signature to, there is no verified signer to show.
fn signer_identity(leaf: &x509_cert::Certificate) -> Option<VerifiedSigner> {
    let exts = leaf.tbs_certificate.extensions.as_ref()?;
    let mut identity: Option<String> = None;
    let mut issuer_v2: Option<String> = None;
    let mut issuer_v1: Option<String> = None;
    let mut source_repo: Option<String> = None;
    for ext in exts {
        let oid = ext.extn_id.to_string();
        let val = ext.extn_value.as_bytes();
        match oid.as_str() {
            OID_SUBJECT_ALT_NAME => identity = san_identity(val),
            // Fulcio V2 issuer / source-repo: DER-encoded UTF8String.
            OID_FULCIO_ISSUER_V2 => issuer_v2 = der_utf8(val),
            OID_FULCIO_SOURCE_REPO => source_repo = der_utf8(val),
            // Deprecated issuer: a bare (non-DER) UTF-8 string.
            OID_FULCIO_ISSUER_V1 => issuer_v1 = std::str::from_utf8(val).ok().map(str::to_string),
            _ => {}
        }
    }
    let identity = identity?;
    if identity.is_empty() {
        return None;
    }
    // The OIDC issuer is part of the verified identity we present, and a genuine
    // Fulcio cert always carries it — a leaf missing it is not something we can
    // stand behind as verified.
    let issuer = issuer_v2.or(issuer_v1).filter(|s| !s.trim().is_empty())?;
    Some(VerifiedSigner {
        identity,
        issuer: Some(issuer),
        source_repo,
    })
}

/// The first URI (else RFC822 name) in a SubjectAltName extension.
fn san_identity(extn_value: &[u8]) -> Option<String> {
    use x509_cert::ext::pkix::name::GeneralName;
    let san = x509_cert::ext::pkix::SubjectAltName::from_der(extn_value).ok()?;
    for gn in &san.0 {
        match gn {
            GeneralName::UniformResourceIdentifier(uri) => return Some(uri.as_str().to_string()),
            GeneralName::Rfc822Name(email) => return Some(email.as_str().to_string()),
            _ => {}
        }
    }
    None
}

/// Decode a DER-encoded `UTF8String` (the wrapping Fulcio V2 OID extensions use).
fn der_utf8(extn_value: &[u8]) -> Option<String> {
    x509_cert::der::asn1::Utf8StringRef::from_der(extn_value)
        .ok()
        .map(|s| s.as_str().to_string())
}

/// True if the in-toto statement names a subject whose sha256 equals
/// `artifact_sha256` (case-insensitive hex).
fn statement_binds(payload: &[u8], artifact_sha256: &str) -> bool {
    let Ok(stmt) = serde_json::from_slice::<Value>(payload) else {
        return false;
    };
    stmt.get("subject")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|s| s.pointer("/digest/sha256").and_then(Value::as_str))
        .any(|d| d.trim().eq_ignore_ascii_case(artifact_sha256))
}

/// The DSSE Pre-Authentication Encoding the leaf signature covers:
/// `DSSEv1 SP len(type) SP type SP len(body) SP body`.
fn dsse_pae(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 64);
    out.extend_from_slice(b"DSSEv1 ");
    out.extend_from_slice(DSSE_PAYLOAD_TYPE.len().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(DSSE_PAYLOAD_TYPE);
    out.push(b' ');
    out.extend_from_slice(payload.len().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload);
    out
}

/// Verify the leaf chains to one embedded Fulcio CA whose window and cert
/// validities cover `integrated_time`. Returns the issuing cert's SPKI DER (used
/// as the SCT issuer key hash) on success.
fn verify_chain(
    leaf: &x509_cert::Certificate,
    root: &TrustedRoot,
    integrated_time: i64,
) -> Option<Vec<u8>> {
    // Leaf must itself be valid at the integrated time.
    if !cert_valid_at(leaf, integrated_time) {
        return None;
    }
    let leaf_tbs = leaf.tbs_certificate.to_der().ok()?;
    let leaf_sig = leaf.signature.as_bytes()?;

    for ca in &root.certificate_authorities {
        if !window_covers(&ca.valid_for, integrated_time) {
            continue;
        }
        // Decode this CA's chain (issuer-most-leaf first: [intermediate, root]).
        let certs: Vec<x509_cert::Certificate> = ca
            .cert_chain
            .certificates
            .iter()
            .filter_map(|c| b64(&c.raw_bytes))
            .filter_map(|der| x509_cert::Certificate::from_der(&der).ok())
            .collect();
        if certs.len() != ca.cert_chain.certificates.len() || certs.is_empty() {
            continue;
        }
        // Every CA cert must be valid at the integrated time too.
        if !certs.iter().all(|c| cert_valid_at(c, integrated_time)) {
            continue;
        }
        let issuer_spki = match certs[0].tbs_certificate.subject_public_key_info.to_der() {
            Ok(d) => d,
            Err(_) => continue,
        };
        // Leaf signed by chain[0], chain[i] signed by chain[i+1]. The final cert
        // is the embedded, trusted anchor (its self-signature is not re-checked).
        if !ecdsa_verify(&issuer_spki, &leaf_tbs, leaf_sig) {
            continue;
        }
        let mut chain_ok = true;
        for pair in certs.windows(2) {
            let child_tbs = match pair[0].tbs_certificate.to_der() {
                Ok(d) => d,
                Err(_) => {
                    chain_ok = false;
                    break;
                }
            };
            let child_sig = match pair[0].signature.as_bytes() {
                Some(s) => s,
                None => {
                    chain_ok = false;
                    break;
                }
            };
            let parent_spki = match pair[1].tbs_certificate.subject_public_key_info.to_der() {
                Ok(d) => d,
                Err(_) => {
                    chain_ok = false;
                    break;
                }
            };
            if !ecdsa_verify(&parent_spki, &child_tbs, child_sig) {
                chain_ok = false;
                break;
            }
        }
        if chain_ok {
            return Some(issuer_spki);
        }
    }
    None
}

/// True if `t` (unix seconds) lies within the certificate's validity window.
fn cert_valid_at(cert: &x509_cert::Certificate, t: i64) -> bool {
    let nb = cert.tbs_certificate.validity.not_before.to_unix_duration();
    let na = cert.tbs_certificate.validity.not_after.to_unix_duration();
    let (nb, na) = (nb.as_secs() as i64, na.as_secs() as i64);
    t >= nb && t <= na
}

/// The critical-extension OIDs we actually process on the leaf. RFC 5280 path
/// validation says a cert bearing a critical extension the verifier does not
/// process MUST be rejected — so any critical extension outside this set fails
/// closed below.
const RECOGNIZED_CRITICAL: &[&str] = &[
    OID_KEY_USAGE,
    OID_BASIC_CONSTRAINTS,
    OID_EXT_KEY_USAGE,
    OID_SUBJECT_ALT_NAME,
    OID_SCT_LIST,
];

/// Leaf must carry the code-signing EKU, assert digital-signature key usage, and
/// not be a CA. Fails closed (RFC 5280): a `basicConstraints` that will not
/// decode is rejected rather than treated as absent, and any *critical* extension
/// we do not process rejects the cert.
fn leaf_constraints_ok(leaf: &x509_cert::Certificate) -> bool {
    let Some(exts) = leaf.tbs_certificate.extensions.as_ref() else {
        return false;
    };
    let mut has_code_signing = false;
    let mut key_usage_ok = false;
    for ext in exts {
        let oid = ext.extn_id.to_string();
        let val = ext.extn_value.as_bytes();
        // Any critical extension we don't recognize/process → reject (fail closed).
        if ext.critical && !RECOGNIZED_CRITICAL.contains(&oid.as_str()) {
            return false;
        }
        if oid == OID_EXT_KEY_USAGE {
            // SEQUENCE OF OBJECT IDENTIFIER; require id-kp-codeSigning present.
            match x509_cert::ext::pkix::ExtendedKeyUsage::from_der(val) {
                Ok(ekus) => {
                    has_code_signing = ekus.0.iter().any(|o| o.to_string() == OID_EKU_CODE_SIGNING)
                }
                Err(_) => return false,
            }
        } else if oid == OID_BASIC_CONSTRAINTS {
            // A basicConstraints that won't decode must not slip past as "non-CA".
            match x509_cert::ext::pkix::BasicConstraints::from_der(val) {
                Ok(bc) if bc.ca => return false,
                Ok(_) => {}
                Err(_) => return false,
            }
        } else if oid == OID_KEY_USAGE {
            match x509_cert::ext::pkix::KeyUsage::from_der(val) {
                Ok(ku) => {
                    key_usage_ok =
                        ku.0.contains(x509_cert::ext::pkix::KeyUsages::DigitalSignature)
                }
                Err(_) => return false,
            }
        }
    }
    has_code_signing && key_usage_ok
}

/// Verify the leaf's embedded SCT against a CT-log key whose window covers the
/// SCT timestamp. Reconstructs the RFC 6962 precertificate signed entry (the
/// leaf TBS with its SCT extension removed) and checks the signature.
fn verify_sct(leaf: &x509_cert::Certificate, issuer_spki: &[u8], root: &TrustedRoot) -> bool {
    let Some(exts) = leaf.tbs_certificate.extensions.as_ref() else {
        return false;
    };
    let Some(sct_ext) = exts.iter().find(|e| e.extn_id.to_string() == OID_SCT_LIST) else {
        return false;
    };
    // extnValue is a DER OCTET STRING wrapping the TLS SCT list.
    let Ok(inner) = x509_cert::der::asn1::OctetString::from_der(sct_ext.extn_value.as_bytes())
    else {
        return false;
    };
    let Some(scts) = parse_sct_list(inner.as_bytes()) else {
        return false;
    };

    // Precert TBS = the leaf TBS with the SCT-list extension removed, re-encoded.
    let mut tbs = leaf.tbs_certificate.clone();
    if let Some(list) = tbs.extensions.as_mut() {
        list.retain(|e| e.extn_id.to_string() != OID_SCT_LIST);
    }
    let Ok(precert_tbs) = tbs.to_der() else {
        return false;
    };
    let issuer_key_hash = Sha256::digest(issuer_spki);

    for sct in &scts {
        // A valid SCT under any embedded, in-window CT-log key is enough.
        for ct in &root.ctlogs {
            let Some(key_id) = b64(&ct.log_id.key_id) else {
                continue;
            };
            if key_id != sct.log_id {
                continue;
            }
            if !window_covers(&ct.public_key.valid_for, (sct.timestamp / 1000) as i64) {
                continue;
            }
            let signed = sct_signed_entry(sct, &issuer_key_hash, &precert_tbs);
            let Some(spki) = b64(&ct.public_key.raw_bytes) else {
                continue;
            };
            if ecdsa_verify(&spki, &signed, &sct.signature) {
                return true;
            }
        }
    }
    false
}

/// One parsed RFC 6962 Signed Certificate Timestamp.
struct Sct {
    log_id: Vec<u8>,
    timestamp: u64,
    extensions: Vec<u8>,
    signature: Vec<u8>,
}

/// Parse a TLS `SignedCertificateTimestampList` (u16 list length, then per SCT a
/// u16 length prefix). Returns `None` on any framing error.
fn parse_sct_list(bytes: &[u8]) -> Option<Vec<Sct>> {
    let total = u16::from_be_bytes([*bytes.first()?, *bytes.get(1)?]) as usize;
    let body = bytes.get(2..2 + total)?;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < body.len() {
        let len = u16::from_be_bytes([*body.get(i)?, *body.get(i + 1)?]) as usize;
        i += 2;
        let sct = body.get(i..i + len)?;
        out.push(parse_sct(sct)?);
        i += len;
    }
    (!out.is_empty()).then_some(out)
}

/// Parse one SCT: version(1)=v1, log_id(32), timestamp(8), extensions(u16),
/// then a TLS digitally-signed blob (hash alg, sig alg, u16 signature). Requires
/// the declared algorithms to be SHA-256 (4) + ECDSA (3) — matching how we verify
/// — and rejects trailing bytes past the signature.
fn parse_sct(b: &[u8]) -> Option<Sct> {
    if *b.first()? != 0 {
        return None; // only SCT v1
    }
    let log_id = b.get(1..33)?.to_vec();
    let timestamp = u64::from_be_bytes(b.get(33..41)?.try_into().ok()?);
    let ext_len = u16::from_be_bytes([*b.get(41)?, *b.get(42)?]) as usize;
    let ext_end = 43 + ext_len;
    let extensions = b.get(43..ext_end)?.to_vec();
    // digitally-signed: hash(1), signature-alg(1), u16 length, signature. Enforce
    // the declared algorithm ids we actually verify with.
    if *b.get(ext_end)? != 4 || *b.get(ext_end + 1)? != 3 {
        return None; // want SHA-256 (4) + ECDSA (3)
    }
    let sig_len = u16::from_be_bytes([*b.get(ext_end + 2)?, *b.get(ext_end + 3)?]) as usize;
    let sig_start = ext_end + 4;
    let signature = b.get(sig_start..sig_start + sig_len)?.to_vec();
    // No trailing bytes may follow the signature within this SCT.
    if sig_start + sig_len != b.len() {
        return None;
    }
    Some(Sct {
        log_id,
        timestamp,
        extensions,
        signature,
    })
}

/// The RFC 6962 signed structure for a precertificate entry:
/// version(0) | sig_type(0) | timestamp | entry_type(precert=1) |
/// issuer_key_hash(32) | tbs_len(u24) | tbs | extensions(u16 len + bytes).
fn sct_signed_entry(sct: &Sct, issuer_key_hash: &[u8], precert_tbs: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(precert_tbs.len() + issuer_key_hash.len() + 16);
    out.push(0); // sct_version v1
    out.push(0); // signature_type certificate_timestamp
    out.extend_from_slice(&sct.timestamp.to_be_bytes());
    out.extend_from_slice(&[0, 1]); // LogEntryType precert_entry
    out.extend_from_slice(issuer_key_hash);
    let len = precert_tbs.len() as u32;
    out.extend_from_slice(&len.to_be_bytes()[1..]); // u24 length
    out.extend_from_slice(precert_tbs);
    out.extend_from_slice(&(sct.extensions.len() as u16).to_be_bytes());
    out.extend_from_slice(&sct.extensions);
    out
}

/// Rekor Signed Entry Timestamp verification. Canonicalizes
/// `{body, integratedTime, logID, logIndex}` (RFC 8785 field order), matches the
/// entry's log id to an embedded Rekor key, and verifies the SET within that
/// key's validity window. Handles ECDSA-P256 (v1 log) and Ed25519 (Log2025).
fn verify_set(entry: &Value, root: &TrustedRoot, integrated_time: i64) -> bool {
    let Some(set_sig) = entry
        .pointer("/inclusionPromise/signedEntryTimestamp")
        .and_then(Value::as_str)
        .and_then(b64)
    else {
        return false;
    };
    let Some(body) = entry.get("canonicalizedBody").and_then(Value::as_str) else {
        return false;
    };
    let Some(log_index) = entry.get("logIndex").and_then(as_i64) else {
        return false;
    };
    let Some(key_id) = entry
        .pointer("/logId/keyId")
        .and_then(Value::as_str)
        .and_then(b64)
    else {
        return false;
    };
    let canon = match serde_json::to_vec(&SetPayload {
        body,
        integrated_time,
        log_id: hex_lower(&key_id),
        log_index,
    }) {
        Ok(c) => c,
        Err(_) => return false,
    };

    for tlog in &root.tlogs {
        let Some(tlog_key_id) = b64(&tlog.log_id.key_id) else {
            continue;
        };
        if tlog_key_id != key_id {
            continue;
        }
        if !window_covers(&tlog.public_key.valid_for, integrated_time) {
            continue;
        }
        let Some(spki) = b64(&tlog.public_key.raw_bytes) else {
            continue;
        };
        let ok = match tlog.public_key.key_details.as_str() {
            "PKIX_ED25519" => ed25519_verify(&spki, &canon, &set_sig),
            "PKIX_ECDSA_P256_SHA_256" => ecdsa_verify(&spki, &canon, &set_sig),
            _ => false,
        };
        if ok {
            return true;
        }
    }
    false
}

/// The Rekor SET signing payload. Field declaration order equals the sorted key
/// order (`body` < `integratedTime` < `logID` < `logIndex`), and serde_json's
/// compact output matches the canonical JSON the log signed.
#[derive(serde::Serialize)]
struct SetPayload<'a> {
    body: &'a str,
    #[serde(rename = "integratedTime")]
    integrated_time: i64,
    #[serde(rename = "logID")]
    log_id: String,
    #[serde(rename = "logIndex")]
    log_index: i64,
}

// ---- crypto primitives (audited RustCrypto; only the composition is ours) ----

/// Verify an ECDSA signature (DER) over `msg` with an SPKI public key, trying
/// P-256 (SHA-256) then P-384 (SHA-384) — the curve's own default digest, which
/// matches every Fulcio/Rekor/CT usage here. Any parse failure is a non-verify.
fn ecdsa_verify(spki_der: &[u8], msg: &[u8], der_sig: &[u8]) -> bool {
    use x509_cert::spki::DecodePublicKey;
    if let Ok(vk) = p256::ecdsa::VerifyingKey::from_public_key_der(spki_der) {
        return p256::ecdsa::Signature::from_der(der_sig)
            .map(|sig| vk.verify(msg, &sig).is_ok())
            .unwrap_or(false);
    }
    if let Ok(vk) = p384::ecdsa::VerifyingKey::from_public_key_der(spki_der) {
        return p384::ecdsa::Signature::from_der(der_sig)
            .map(|sig| vk.verify(msg, &sig).is_ok())
            .unwrap_or(false);
    }
    false
}

/// Verify a raw Ed25519 signature over `msg` with an SPKI public key. The raw
/// 32-byte key is read from the SPKI's subjectPublicKey bit string.
fn ed25519_verify(spki_der: &[u8], msg: &[u8], sig: &[u8]) -> bool {
    let Ok(spki) = x509_cert::spki::SubjectPublicKeyInfoOwned::from_der(spki_der) else {
        return false;
    };
    let Some(raw) = spki.subject_public_key.as_bytes() else {
        return false;
    };
    let Ok(arr) = <[u8; 32]>::try_from(raw) else {
        return false;
    };
    let Ok(vk) = ed25519_dalek::VerifyingKey::from_bytes(&arr) else {
        return false;
    };
    let Ok(sig) = ed25519_dalek::Signature::from_slice(sig) else {
        return false;
    };
    vk.verify_strict(msg, &sig).is_ok()
}

// ---- helpers ----

fn b64(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(s.trim())
        .ok()
}

/// Accept an integer that may arrive as a JSON number or (proto3 int64 JSON) a
/// numeric string.
fn as_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// True if `t` (unix seconds) is within `[start, end]`; an *absent* bound is
/// open, but a bound that is *present and unparseable* fails closed — a
/// malformed date in our own embedded trust root must never silently widen
/// trust to an open window.
fn window_covers(vf: &Option<ValidFor>, t: i64) -> bool {
    let Some(vf) = vf else {
        return true;
    };
    if let Some(s) = vf.start.as_deref() {
        match rfc3339_unix(s) {
            Some(start) if t >= start => {}
            _ => return false,
        }
    }
    if let Some(e) = vf.end.as_deref() {
        match rfc3339_unix(e) {
            Some(end) if t <= end => {}
            _ => return false,
        }
    }
    true
}

fn rfc3339_unix(s: &str) -> Option<i64> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|dt| dt.unix_timestamp())
}

// ---- embedded trust root ----

/// The Sigstore trust root baked into the binary, parsed once. `None` only if
/// the embedded asset is unparseable — which fails all verification closed,
/// never a startup error.
///
/// Stored as plain JSON (7 KB) rather than gzip so a refresh shows up as a
/// readable `git diff`; `dev/scripts/fetch-trust-root.sh` regenerates it from a
/// pinned sigstore/root-signing commit.
fn embedded_trust_root() -> Option<&'static TrustedRoot> {
    static ROOT: OnceLock<Option<TrustedRoot>> = OnceLock::new();
    ROOT.get_or_init(|| serde_json::from_slice(include_bytes!("assets/trusted_root.json")).ok())
        .as_ref()
}

#[derive(Deserialize)]
struct TrustedRoot {
    #[serde(rename = "certificateAuthorities", default)]
    certificate_authorities: Vec<CertAuthority>,
    #[serde(default)]
    tlogs: Vec<TlogKey>,
    #[serde(default)]
    ctlogs: Vec<TlogKey>,
}

#[derive(Deserialize)]
struct CertAuthority {
    #[serde(rename = "certChain")]
    cert_chain: CertChain,
    #[serde(rename = "validFor", default)]
    valid_for: Option<ValidFor>,
}

#[derive(Deserialize)]
struct CertChain {
    certificates: Vec<RawCert>,
}

#[derive(Deserialize)]
struct RawCert {
    #[serde(rename = "rawBytes")]
    raw_bytes: String,
}

#[derive(Deserialize)]
struct TlogKey {
    #[serde(rename = "logId")]
    log_id: LogId,
    #[serde(rename = "publicKey")]
    public_key: PublicKey,
}

#[derive(Deserialize)]
struct LogId {
    #[serde(rename = "keyId")]
    key_id: String,
}

#[derive(Deserialize)]
struct PublicKey {
    #[serde(rename = "rawBytes")]
    raw_bytes: String,
    #[serde(rename = "keyDetails", default)]
    key_details: String,
    #[serde(rename = "validFor", default)]
    valid_for: Option<ValidFor>,
}

#[derive(Deserialize)]
struct ValidFor {
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real PyPI provenance object: pypa/sampleproject 4.0.0's wheel, published
    /// via GitHub Trusted Publishing (P-256 Rekor v1 log). Its leaf cert was valid
    /// for ~10 minutes in 2024 — long expired by wall clock — so a passing verify
    /// proves validity is evaluated at the Rekor integrated time.
    const SAMPLEPROJECT: &[u8] = include_bytes!("testdata/sampleproject.provenance.json");
    const SAMPLEPROJECT_SHA: &str =
        "c23e447ea90d796d1e645c35c4b2de125040add12a845825546f91c93f391b6b";

    fn mutate(f: impl FnOnce(&mut Value)) -> Vec<u8> {
        let mut v: Value = serde_json::from_slice(SAMPLEPROJECT).unwrap();
        f(&mut v);
        serde_json::to_vec(&v).unwrap()
    }

    fn att_field<'a>(v: &'a mut Value, ptr: &str) -> &'a mut Value {
        v.pointer_mut(&format!("/attestation_bundles/0/attestations/0{ptr}"))
            .unwrap()
    }

    fn sampleproject_leaf() -> (Vec<u8>, x509_cert::Certificate) {
        let v: Value = serde_json::from_slice(SAMPLEPROJECT).unwrap();
        let der = b64(v
            .pointer("/attestation_bundles/0/attestations/0/verification_material/certificate")
            .unwrap()
            .as_str()
            .unwrap())
        .unwrap();
        let cert = x509_cert::Certificate::from_der(&der).unwrap();
        (der, cert)
    }

    fn sampleproject_payload_and_sig() -> (Vec<u8>, Vec<u8>) {
        let v: Value = serde_json::from_slice(SAMPLEPROJECT).unwrap();
        let env = v
            .pointer("/attestation_bundles/0/attestations/0/envelope")
            .unwrap();
        (
            b64(env.get("statement").unwrap().as_str().unwrap()).unwrap(),
            b64(env.get("signature").unwrap().as_str().unwrap()).unwrap(),
        )
    }

    fn pem_of(der: &[u8]) -> String {
        use x509_cert::der::EncodePem;
        let cert = x509_cert::Certificate::from_der(der).unwrap();
        cert.to_pem(x509_cert::der::pem::LineEnding::LF).unwrap()
    }

    #[test]
    fn entry_binding_requires_our_leaf_as_the_recorded_verifier() {
        // FIX 1: the entry's recorded signer (`verifier`) must be our leaf cert.
        // A body with the right payloadHash + signature but a DIFFERENT verifier
        // cert must NOT bind — the old code checked neither.
        let (leaf_der, _leaf) = sampleproject_leaf();
        let (payload, dsse_sig) = sampleproject_payload_and_sig();
        let payload_hash = hex_lower(&Sha256::digest(&payload));
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(&dsse_sig);
        let make_entry = |verifier_der: &[u8]| -> Value {
            let body = serde_json::json!({
                "spec": {
                    "payloadHash": {"algorithm": "sha256", "value": payload_hash},
                    "signatures": [{
                        "signature": sig_b64,
                        "verifier": base64::engine::general_purpose::STANDARD
                            .encode(pem_of(verifier_der)),
                    }],
                }
            });
            serde_json::json!({
                "canonicalizedBody": base64::engine::general_purpose::STANDARD
                    .encode(serde_json::to_vec(&body).unwrap()),
            })
        };
        // Our leaf as the verifier → binds.
        let good = make_entry(&leaf_der);
        assert!(entry_binds_attestation(
            &good, &leaf_der, &payload, &dsse_sig
        ));
        // A different real certificate (an embedded Fulcio CA) as verifier → rejected.
        let root = embedded_trust_root().unwrap();
        let foreign =
            b64(&root.certificate_authorities[1].cert_chain.certificates[0].raw_bytes).unwrap();
        let bad = make_entry(&foreign);
        assert!(!entry_binds_attestation(
            &bad, &leaf_der, &payload, &dsse_sig
        ));
        // A garbage verifier → rejected, no panic.
        let mut junk = good.clone();
        let body = serde_json::json!({
            "spec": {
                "payloadHash": {"algorithm": "sha256", "value": payload_hash},
                "signatures": [{"signature": sig_b64, "verifier": "!!not base64!!"}],
            }
        });
        junk["canonicalizedBody"] = Value::String(
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&body).unwrap()),
        );
        assert!(!entry_binds_attestation(
            &junk, &leaf_der, &payload, &dsse_sig
        ));
    }

    #[test]
    fn leaf_missing_issuer_oid_is_not_a_verified_signer() {
        // FIX 2: the OIDC issuer is part of the verified identity. A leaf with a
        // SAN but no Fulcio issuer OID must not be a verified signer.
        let (_der, mut leaf) = sampleproject_leaf();
        assert!(
            signer_identity(&leaf).is_some(),
            "the genuine leaf yields a signer with an issuer"
        );
        if let Some(exts) = leaf.tbs_certificate.extensions.as_mut() {
            exts.retain(|e| {
                let o = e.extn_id.to_string();
                o != OID_FULCIO_ISSUER_V2 && o != OID_FULCIO_ISSUER_V1
            });
        }
        assert!(
            signer_identity(&leaf).is_none(),
            "a leaf without an issuer OID must not be a verified signer"
        );
    }

    #[test]
    fn leaf_with_unknown_critical_extension_is_rejected() {
        // FIX 3: RFC 5280 — an unrecognized critical extension must fail closed.
        use x509_cert::der::asn1::{ObjectIdentifier, OctetString};
        let (_der, mut leaf) = sampleproject_leaf();
        assert!(
            leaf_constraints_ok(&leaf),
            "the genuine leaf passes constraints"
        );
        let ext = x509_cert::ext::Extension {
            extn_id: ObjectIdentifier::new_unwrap("1.2.3.4.5.6.7.8"),
            critical: true,
            extn_value: OctetString::new(vec![0x05, 0x00]).unwrap(),
        };
        leaf.tbs_certificate.extensions.as_mut().unwrap().push(ext);
        assert!(
            !leaf_constraints_ok(&leaf),
            "an unknown critical extension must reject the leaf"
        );
    }

    #[test]
    fn leaf_with_malformed_basic_constraints_is_rejected() {
        // FIX 3: a basicConstraints that will not decode must not slip past as
        // "absent / non-CA".
        use x509_cert::der::asn1::{ObjectIdentifier, OctetString};
        let (_der, mut leaf) = sampleproject_leaf();
        let ext = x509_cert::ext::Extension {
            extn_id: ObjectIdentifier::new_unwrap(OID_BASIC_CONSTRAINTS),
            critical: false,
            extn_value: OctetString::new(vec![0x01, 0x02, 0x03]).unwrap(), // not a SEQUENCE
        };
        leaf.tbs_certificate.extensions.as_mut().unwrap().push(ext);
        assert!(
            !leaf_constraints_ok(&leaf),
            "a malformed basicConstraints must reject the leaf"
        );
    }

    #[test]
    fn malformed_trust_root_window_fails_closed() {
        // NIT 5: a present-but-unparseable validity bound is a closed window, not
        // an open one — a bad embedded root must never silently widen trust.
        let open = ValidFor {
            start: None,
            end: None,
        };
        assert!(window_covers(&Some(open), 1_700_000_000));
        let bad_start = ValidFor {
            start: Some("not-a-date".into()),
            end: None,
        };
        assert!(!window_covers(&Some(bad_start), 1_700_000_000));
        let bad_end = ValidFor {
            start: Some("2020-01-01T00:00:00Z".into()),
            end: Some("garbage".into()),
        };
        assert!(!window_covers(&Some(bad_end), 1_700_000_000));
    }

    #[test]
    fn oversized_provenance_is_refused_before_crunching() {
        // BOUND 4: a blob larger than the render cap is refused outright.
        let big = vec![b'{'; MAX_PROVENANCE_BYTES + 1];
        assert!(verify(&big, SAMPLEPROJECT_SHA).is_none());
    }

    #[test]
    fn real_bundle_verifies_with_cert_derived_identity() {
        let signer = verify(SAMPLEPROJECT, SAMPLEPROJECT_SHA)
            .expect("a genuine PyPI Sigstore bundle must verify offline against the embedded root");
        // The identity is drawn from the certificate (SAN + Fulcio OIDs), not the
        // unsigned publisher JSON: the signing workflow, its source repo, its OIDC
        // issuer.
        assert_eq!(
            signer.identity,
            "https://github.com/pypa/sampleproject/.github/workflows/release.yml@refs/heads/main"
        );
        assert_eq!(
            signer.source_repo.as_deref(),
            Some("https://github.com/pypa/sampleproject")
        );
        assert_eq!(
            signer.issuer.as_deref(),
            Some("https://token.actions.githubusercontent.com")
        );
        // Case folds on the hex digest.
        assert!(verify(SAMPLEPROJECT, &SAMPLEPROJECT_SHA.to_ascii_uppercase()).is_some());
    }

    #[test]
    fn spoofed_publisher_json_does_not_change_verified_identity() {
        // A genuine, fully-valid bundle whose UNSIGNED `publisher` field is spoofed
        // to a different project. Verification must still succeed AND report the
        // identity from the certificate (pypa/sampleproject), never the spoofed
        // "pypa/pip" — the publisher JSON is not covered by any signature.
        let bytes = mutate(|v| {
            let pubv = v.pointer_mut("/attestation_bundles/0/publisher").unwrap();
            pubv["repository"] = Value::String("pypa/pip".into());
            pubv["kind"] = Value::String("GitHub".into());
        });
        let signer = verify(&bytes, SAMPLEPROJECT_SHA)
            .expect("a spoofed publisher must not break a genuine signature");
        assert!(signer.identity.contains("pypa/sampleproject"));
        assert_eq!(
            signer.source_repo.as_deref(),
            Some("https://github.com/pypa/sampleproject")
        );
        // The spoofed identity appears nowhere in the verified signer.
        assert!(!signer.identity.contains("pip"));
        assert_ne!(
            signer.source_repo.as_deref(),
            Some("https://github.com/pypa/pip")
        );
    }

    #[test]
    fn wrong_artifact_digest_does_not_verify() {
        assert!(verify(
            SAMPLEPROJECT,
            "deadbeef00000000000000000000000000000000000000000000000000000000"
        )
        .is_none());
        assert!(verify(SAMPLEPROJECT, "").is_none());
    }

    #[test]
    fn tampered_dsse_signature_does_not_verify() {
        let bytes = mutate(|v| {
            let sig = att_field(v, "/envelope/signature");
            // Flip the last base64 char — a different, invalid signature.
            let mut s = sig.as_str().unwrap().to_string();
            let last = s.pop().unwrap();
            s.push(if last == 'A' { 'B' } else { 'A' });
            *sig = Value::String(s);
        });
        assert!(verify(&bytes, SAMPLEPROJECT_SHA).is_none());
    }

    #[test]
    fn tampered_statement_does_not_verify() {
        // Re-base64 a statement with a different subject digest: the DSSE
        // signature no longer covers it, and the binding breaks.
        let bytes = mutate(|v| {
            let stmt = att_field(v, "/envelope/statement");
            let raw = b64(stmt.as_str().unwrap()).unwrap();
            let mut s: Value = serde_json::from_slice(&raw).unwrap();
            *s.pointer_mut("/subject/0/digest/sha256").unwrap() = Value::String("0".repeat(64));
            let re =
                base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&s).unwrap());
            *stmt = Value::String(re);
        });
        assert!(verify(&bytes, SAMPLEPROJECT_SHA).is_none());
    }

    #[test]
    fn tampered_certificate_does_not_verify() {
        let bytes = mutate(|v| {
            let cert = att_field(v, "/verification_material/certificate");
            let mut der = b64(cert.as_str().unwrap()).unwrap();
            // Corrupt a middle byte: parse may survive but the chain signature won't.
            let mid = der.len() / 2;
            der[mid] ^= 0xff;
            *cert = Value::String(base64::engine::general_purpose::STANDARD.encode(der));
        });
        assert!(verify(&bytes, SAMPLEPROJECT_SHA).is_none());
    }

    #[test]
    fn expired_at_integrated_time_does_not_verify() {
        // Push the integrated time a year past the leaf's ~10-minute window: the
        // certificate is no longer valid at that instant.
        let bytes = mutate(|v| {
            let te = att_field(
                v,
                "/verification_material/transparency_entries/0/integratedTime",
            );
            let t: i64 = te.as_str().unwrap().parse().unwrap();
            *te = Value::String((t + 31_536_000).to_string());
        });
        assert!(verify(&bytes, SAMPLEPROJECT_SHA).is_none());
    }

    #[test]
    fn unknown_log_key_does_not_verify() {
        // A log id that matches no embedded Rekor key: transparency can't be shown.
        let bytes = mutate(|v| {
            let kid = att_field(
                v,
                "/verification_material/transparency_entries/0/logId/keyId",
            );
            *kid = Value::String(base64::engine::general_purpose::STANDARD.encode([0u8; 32]));
        });
        assert!(verify(&bytes, SAMPLEPROJECT_SHA).is_none());
    }

    #[test]
    fn garbage_and_truncated_inputs_never_panic() {
        assert!(verify(b"", SAMPLEPROJECT_SHA).is_none());
        assert!(verify(b"not json", SAMPLEPROJECT_SHA).is_none());
        assert!(verify(b"{}", SAMPLEPROJECT_SHA).is_none());
        assert!(verify(br#"{"attestation_bundles":[]}"#, SAMPLEPROJECT_SHA).is_none());
        // Prefixes of the real bundle: truncation must never index-panic.
        for cut in [1, 10, 100, 500, 2000, SAMPLEPROJECT.len() / 2] {
            assert!(verify(&SAMPLEPROJECT[..cut], SAMPLEPROJECT_SHA).is_none());
        }
        // Byte flips across the whole bundle must only ever yield false.
        for step in (0..SAMPLEPROJECT.len()).step_by(97) {
            let mut b = SAMPLEPROJECT.to_vec();
            b[step] ^= 0xff;
            assert!(verify(&b, SAMPLEPROJECT_SHA).is_none());
        }
    }

    #[test]
    fn set_verifies_ed25519_log_by_key_type() {
        // PyPI still logs to the P-256 v1 log, so no real bundle exercises the
        // Ed25519 "Log2025" path. Prove that path with a synthetic trust root and
        // a SET signed by a generated Ed25519 key — the dispatch-by-key-type and
        // the Ed25519 primitive are the load-bearing parts.
        use ed25519_dalek::{Signer, SigningKey};

        let sk = SigningKey::from_bytes(&[7u8; 32]);
        // Ed25519 SubjectPublicKeyInfo: fixed prefix + the 32-byte raw key.
        let mut spki = vec![
            0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
        ];
        spki.extend_from_slice(sk.verifying_key().as_bytes());
        let key_id = [9u8; 32];
        let entry = serde_json::json!({
            "logIndex": "42",
            "logId": {"keyId": base64::engine::general_purpose::STANDARD.encode(key_id)},
            "canonicalizedBody": "Zm9vYmFy",
        });
        let integrated_time = 1_780_000_000i64;
        let canon = serde_json::to_vec(&SetPayload {
            body: "Zm9vYmFy",
            integrated_time,
            log_id: hex_lower(&key_id),
            log_index: 42,
        })
        .unwrap();
        let sig = sk.sign(&canon);

        let good = |set_b64: String| -> Value {
            let mut e = entry.clone();
            e["inclusionPromise"] = serde_json::json!({ "signedEntryTimestamp": set_b64 });
            e
        };
        let root = TrustedRoot {
            certificate_authorities: vec![],
            ctlogs: vec![],
            tlogs: vec![TlogKey {
                log_id: LogId {
                    key_id: base64::engine::general_purpose::STANDARD.encode(key_id),
                },
                public_key: PublicKey {
                    raw_bytes: base64::engine::general_purpose::STANDARD.encode(&spki),
                    key_details: "PKIX_ED25519".to_string(),
                    valid_for: None,
                },
            }],
        };
        let set_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
        assert!(verify_set(&good(set_b64), &root, integrated_time));
        // A tampered SET fails; a mismatched integrated time (breaks canon) fails.
        let bad = base64::engine::general_purpose::STANDARD.encode([0u8; 64]);
        assert!(!verify_set(&good(bad.clone()), &root, integrated_time));
        let set_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
        assert!(!verify_set(&good(set_b64), &root, integrated_time + 1));
    }

    #[test]
    fn embedded_root_loads() {
        let root = embedded_trust_root().expect("embedded trust root must parse");
        assert!(!root.certificate_authorities.is_empty());
        assert!(!root.tlogs.is_empty());
        assert!(!root.ctlogs.is_empty());
    }
}
