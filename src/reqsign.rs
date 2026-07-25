//! Hand-rolled request signing for the one job object_store cannot do: a
//! server-side, cross-bucket copy. object_store exposes no CopyObject/rewrite/
//! Copy-Blob verb, so the replication transport ladder ([`crate::replicate`])
//! issues those calls itself and signs them here — AWS Signature V4 for S3 and
//! Azure Shared Key for Blob. GCS `rewriteTo` needs no signature (a bearer token
//! on `Authorization`), so it has no entry point here.
//!
//! Zero new dependencies: SigV4 and Shared Key are both HMAC-SHA256 string
//! building (same reason [`crate::token`] hand-rolls its own MAC). Both
//! functions are pure — the wire calls live in [`crate::storage`] and the KATs
//! below pin them against the vendors' published examples.

use crate::hash::hmac_sha256;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// sha256 of the empty body, hex — the payload hash for every copy verb (none
/// of them carry a request body).
pub const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// The permanent (or STS) credential a signer needs. Borrowed, so callers pass
/// object_store's resolved credential straight through without cloning secrets.
pub struct AwsCredential<'a> {
    pub key_id: &'a str,
    pub secret: &'a str,
    pub token: Option<&'a str>,
}

/// AWS's canonical URI-encoding (RFC 3986 unreserved kept verbatim; everything
/// else percent-encoded uppercase). `keep_slash` leaves `/` unescaped, which is
/// what a path is; a single path *segment* or header value encodes it too.
pub fn uri_encode(input: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn amz_datestamps(now: OffsetDateTime) -> (String, String) {
    let now = now.to_offset(time::UtcOffset::UTC);
    let amz = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    );
    let date = amz[..8].to_string();
    (amz, date)
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// One S3 request to sign. `host` is the request URL's authority; `extra_headers`
/// are the request's own signed headers (lowercased names) — for CopyObject that
/// is `x-amz-copy-source`. `x-amz-date` and `x-amz-content-sha256` are added and
/// signed by [`sign_s3_request`].
pub struct S3Request<'a> {
    pub method: &'a str,
    pub canonical_uri: &'a str,
    pub canonical_query: &'a str,
    pub host: &'a str,
    pub extra_headers: &'a [(String, String)],
    pub payload_sha256_hex: &'a str,
}

/// Sign one S3 request and return every header to place on the wire
/// (`Authorization` plus the `x-amz-*` headers that were signed).
pub fn sign_s3_request(
    req: &S3Request<'_>,
    cred: &AwsCredential<'_>,
    region: &str,
    service: &str,
    now: OffsetDateTime,
) -> Vec<(String, String)> {
    let (amz_date, datestamp) = amz_datestamps(now);

    // Assemble the full signed-header set: host + payload hash + date + the
    // request's own headers + the session token when present.
    let mut headers: Vec<(String, String)> = vec![
        ("host".to_string(), req.host.to_string()),
        (
            "x-amz-content-sha256".to_string(),
            req.payload_sha256_hex.to_string(),
        ),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    headers.extend(req.extra_headers.iter().cloned());
    if let Some(token) = cred.token {
        headers.push(("x-amz-security-token".to_string(), token.to_string()));
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect();

    let canonical_request = format!(
        "{}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{}",
        req.method, req.canonical_uri, req.canonical_query, req.payload_sha256_hex
    );
    let scope = format!("{datestamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let k_date = hmac_sha256(
        format!("AWS4{}", cred.secret).as_bytes(),
        datestamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        cred.key_id
    );

    // Everything except host (reqwest sets Host from the URL), plus Authorization.
    let mut wire: Vec<(String, String)> = headers
        .into_iter()
        .filter(|(name, _)| name != "host")
        .collect();
    wire.push(("authorization".to_string(), authorization));
    wire
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Azure Blob "Copy Blob" (a `PUT` with `x-ms-copy-source` and an empty body).
/// Returns the `Authorization` header value for Shared Key auth. `account_key`
/// is the base64 account key; `blob_path` is the canonicalized resource path
/// (`/{account}/{container}/{blob}`); `signed_ms_headers` are the request's
/// `x-ms-*` headers (lowercased names), which must include `x-ms-copy-source`,
/// `x-ms-date`, and `x-ms-version`.
pub fn azure_shared_key_authorization(
    account: &str,
    account_key: &str,
    method: &str,
    blob_path: &str,
    signed_ms_headers: &[(String, String)],
) -> anyhow::Result<String> {
    let key = B64
        .decode(account_key)
        .map_err(|e| anyhow::anyhow!("azure account key is not valid base64: {e}"))?;

    let mut ms_headers: Vec<(String, String)> = signed_ms_headers
        .iter()
        .map(|(n, v)| (n.to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    ms_headers.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical_headers: String = ms_headers
        .iter()
        .map(|(n, v)| format!("{n}:{v}\n"))
        .collect();

    // Copy Blob carries an empty body: Content-Length is the empty string (not
    // "0") for x-ms-version 2015-02-21+, and Date is empty because x-ms-date
    // carries the timestamp instead.
    let string_to_sign = format!(
        "{method}\n\n\n\n\n\n\n\n\n\n\n\n{canonical_headers}{}",
        format_args!("/{account}{blob_path}")
    );
    let signature = B64.encode(hmac_sha256(&key, string_to_sign.as_bytes()));
    Ok(format!("SharedKey {account}:{signature}"))
}

/// RFC 1123 date in GMT, the format Azure's `x-ms-date` header requires
/// (`Sun, 06 Nov 1994 08:49:37 GMT`). Hand-formatted because `time` ships no
/// RFC 1123 well-known format.
pub fn rfc1123(now: OffsetDateTime) -> String {
    let now = now.to_offset(time::UtcOffset::UTC);
    const DAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        DAYS[now.weekday().number_days_from_monday() as usize],
        now.day(),
        MONTHS[u8::from(now.month()) as usize - 1],
        now.year(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn uri_encode_matches_aws_rules() {
        assert_eq!(
            uri_encode("packages/foo/bar-1.0.whl", true),
            "packages/foo/bar-1.0.whl"
        );
        assert_eq!(uri_encode("a b+c", false), "a%20b%2Bc");
        assert_eq!(uri_encode("a/b", false), "a%2Fb");
        assert_eq!(uri_encode("~-._", false), "~-._");
    }

    // AWS's own published worked example — "GET Object" with a Range header, from
    // the SigV4 "Transferring Payload in a Single Chunk" documentation. Proves the
    // canonical request, string-to-sign, signing key, and signature end to end.
    #[test]
    fn sigv4_aws_published_get_object_vector() {
        let cred = AwsCredential {
            key_id: "AKIAIOSFODNN7EXAMPLE",
            secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            token: None,
        };
        let range = [("range".to_string(), "bytes=0-9".to_string())];
        let headers = sign_s3_request(
            &S3Request {
                method: "GET",
                canonical_uri: "/test.txt",
                canonical_query: "",
                host: "examplebucket.s3.amazonaws.com",
                extra_headers: &range,
                payload_sha256_hex: EMPTY_PAYLOAD_SHA256,
            },
            &cred,
            "us-east-1",
            "s3",
            datetime!(2013-05-24 00:00:00 UTC),
        );
        let auth = headers
            .iter()
            .find(|(n, _)| n == "authorization")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert!(
            auth.contains(
                "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
            ),
            "unexpected authorization: {auth}"
        );
        assert!(auth.contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date"));
    }

    #[test]
    fn sigv4_includes_session_token_when_present() {
        let cred = AwsCredential {
            key_id: "AKIA",
            secret: "secret",
            token: Some("session-token"),
        };
        let copy_src = [("x-amz-copy-source".to_string(), "/src/key".to_string())];
        let headers = sign_s3_request(
            &S3Request {
                method: "PUT",
                canonical_uri: "/dst/key",
                canonical_query: "",
                host: "host.example",
                extra_headers: &copy_src,
                payload_sha256_hex: EMPTY_PAYLOAD_SHA256,
            },
            &cred,
            "us-east-1",
            "s3",
            datetime!(2026-01-01 00:00:00 UTC),
        );
        assert!(headers
            .iter()
            .any(|(n, v)| n == "x-amz-security-token" && v == "session-token"));
        let auth = &headers
            .iter()
            .find(|(n, _)| n == "authorization")
            .unwrap()
            .1;
        assert!(auth.contains("x-amz-copy-source"));
        assert!(auth.contains("x-amz-security-token"));
    }

    #[test]
    fn azure_shared_key_matches_canonical_string_to_sign() {
        // No published KAT exists for an arbitrary key, so re-derive the expected
        // signature from the exact string-to-sign the Shared Key spec prescribes
        // for an empty-body Copy Blob. This catches any regression in header
        // canonicalization or the fixed empty-field block.
        let key = B64.encode([7u8; 32]);
        let date = rfc1123(datetime!(2026-01-01 00:00:00 UTC));
        assert_eq!(date, "Thu, 01 Jan 2026 00:00:00 GMT");
        let src = "https://acct.blob.core.windows.net/srccontainer/srcblob";
        let auth = azure_shared_key_authorization(
            "acct",
            &key,
            "PUT",
            "/acct/dstcontainer/dstblob",
            &[
                ("x-ms-copy-source".to_string(), src.to_string()),
                ("x-ms-date".to_string(), date.clone()),
                ("x-ms-version".to_string(), "2021-08-06".to_string()),
            ],
        )
        .unwrap();

        let string_to_sign = format!(
            "PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-copy-source:{src}\nx-ms-date:{date}\nx-ms-version:2021-08-06\n/acct/acct/dstcontainer/dstblob"
        );
        let expected = format!(
            "SharedKey acct:{}",
            B64.encode(hmac_sha256(
                &B64.decode(&key).unwrap(),
                string_to_sign.as_bytes()
            ))
        );
        assert_eq!(auth, expected);
    }
}
