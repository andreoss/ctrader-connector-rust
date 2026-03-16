//! Token pairs and their refresh over the token endpoint.
//!
//! A refresh rotates the pair: the old access and refresh tokens stop working once the server
//! has answered, so the caller must store the new pair before doing anything else with it.
//!
//! The endpoint is queried with a single HTTP `GET` over the same TLS stack the connection uses;
//! the reply is a small JSON document.

use std::fmt::Write as _;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use crate::common::config::{AccessToken, ConfigurationOpenApi, RefreshToken};
use crate::common::errors::{ConfigError, ConnectorError, TokenError, TransportError};
use crate::open_api::messages::ProtoOaRefreshTokenRes;
use crate::open_api::transport::Connect;

const HTTPS_PORT: u16 = 443;
const MAX_RESPONSE_LENGTH: usize = 64 * 1024;
const READ_CHUNK: usize = 4 * 1024;
const HEAD_END: &[u8] = b"\r\n\r\n";
const LINE_END: &[u8] = b"\r\n";

/// Access token together with the refresh token that renews it.
///
/// Both tokens are redacted in `Debug` output.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TokenPair {
    /// Token authorising access to trading accounts.
    pub access: AccessToken,
    /// Token exchanged for the next pair.
    pub refresh: RefreshToken,
    /// Time the access token stays valid, counted from the refresh.
    pub expires_in: Duration,
    /// Kind of the access token, `bearer` in practice.
    pub token_type: String,
}

impl TokenPair {
    fn new(
        access: String,
        refresh: String,
        expires_in: u64,
        token_type: String,
    ) -> Result<Self, TokenError> {
        Ok(Self {
            access: AccessToken::new(access).map_err(|_| TokenError::Malformed("accessToken"))?,
            refresh: RefreshToken::new(refresh)
                .map_err(|_| TokenError::Malformed("refreshToken"))?,
            expires_in: Duration::from_secs(expires_in),
            token_type,
        })
    }
}

impl TryFrom<ProtoOaRefreshTokenRes> for TokenPair {
    type Error = TokenError;

    fn try_from(response: ProtoOaRefreshTokenRes) -> Result<Self, Self::Error> {
        let expires_in =
            u64::try_from(response.expires_in).map_err(|_| TokenError::Malformed("expiresIn"))?;
        Self::new(
            response.access_token,
            response.refresh_token,
            expires_in,
            response.token_type,
        )
    }
}

/// Reads a token pair from the JSON document the token endpoint answers with.
///
/// # Errors
///
/// Returns [`TokenError::Refused`] when the document reports an error and
/// [`TokenError::Malformed`] when it is not JSON or lacks a field. The document itself is
/// never copied into the error.
pub fn parse_token_response(body: &str) -> Result<TokenPair, TokenError> {
    let document: Value = serde_json::from_str(body).map_err(|_| TokenError::Malformed("JSON"))?;
    let text = |key: &str| document.get(key).and_then(Value::as_str).map(str::to_owned);
    if let Some(code) = text("errorCode") {
        return Err(TokenError::Refused {
            code,
            description: text("description").unwrap_or_default(),
        });
    }
    let expires_in = document
        .get("expiresIn")
        .and_then(Value::as_u64)
        .ok_or(TokenError::Malformed("expiresIn"))?;
    TokenPair::new(
        text("accessToken").ok_or(TokenError::Malformed("accessToken"))?,
        text("refreshToken").ok_or(TokenError::Malformed("refreshToken"))?,
        expires_in,
        text("tokenType").unwrap_or_else(|| "bearer".to_owned()),
    )
}

#[derive(Debug, PartialEq, Eq)]
struct Endpoint {
    host: String,
    port: u16,
    path: String,
}

impl Endpoint {
    fn parse(url: &str) -> Result<Self, ConfigError> {
        let invalid = || ConfigError::InvalidTokenUrl(url.to_owned());
        let rest = url.strip_prefix("https://").ok_or_else(invalid)?;
        let (authority, path) = match rest.find(['/', '?']) {
            Some(index) => rest.split_at(index),
            None => (rest, ""),
        };
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => (host, port.parse().map_err(|_| invalid())?),
            None => (authority, HTTPS_PORT),
        };
        if host.is_empty() || port == 0 {
            return Err(invalid());
        }
        let path = if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("/{path}")
        };
        Ok(Self {
            host: host.to_owned(),
            port,
            path,
        })
    }

    fn target(&self, parameters: &[(&str, &str)]) -> String {
        let mut target = self.path.clone();
        let mut separator = if target.contains('?') { '&' } else { '?' };
        for (name, value) in parameters {
            target.push(separator);
            target.push_str(name);
            target.push('=');
            percent_encode(value, &mut target);
            separator = '&';
        }
        target
    }
}

fn percent_encode(value: &str, out: &mut String) {
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn unchunk(mut rest: &[u8]) -> Result<Vec<u8>, TokenError> {
    let malformed = TokenError::Malformed("chunked body");
    let mut body = Vec::new();
    loop {
        let line_end = find(rest, LINE_END).ok_or(malformed.clone())?;
        let (line, after) = rest.split_at(line_end);
        let size = std::str::from_utf8(line)
            .ok()
            .and_then(|line| line.split(';').next())
            .and_then(|digits| usize::from_str_radix(digits.trim(), 16).ok())
            .ok_or(malformed.clone())?;
        let data = after.get(LINE_END.len()..).ok_or(malformed.clone())?;
        if size == 0 {
            return Ok(body);
        }
        let (chunk, after) = data.split_at_checked(size).ok_or(malformed.clone())?;
        body.extend_from_slice(chunk);
        rest = after.get(LINE_END.len()..).ok_or(malformed.clone())?;
    }
}

fn http_body(raw: &[u8]) -> Result<(u16, String), TokenError> {
    let malformed = TokenError::Malformed("HTTP response");
    let head_end = find(raw, HEAD_END).ok_or(malformed.clone())?;
    let (head, rest) = raw.split_at(head_end);
    let head = std::str::from_utf8(head).map_err(|_| malformed.clone())?;
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .ok_or(malformed.clone())?;
    let chunked = head.lines().skip(1).any(|line| {
        let line = line.to_ascii_lowercase();
        line.starts_with("transfer-encoding:") && line.contains("chunked")
    });
    let body = rest.get(HEAD_END.len()..).unwrap_or_default();
    let body = if chunked {
        unchunk(body)?
    } else {
        body.to_vec()
    };
    let body = String::from_utf8(body).map_err(|_| malformed)?;
    Ok((status, body))
}

async fn exchange<C: Connect>(
    connector: &C,
    endpoint: &Endpoint,
    request: &str,
) -> Result<Vec<u8>, TransportError> {
    let mut stream = connector.connect(&endpoint.host, endpoint.port).await?;
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;
    let mut raw = Vec::new();
    let mut chunk = [0_u8; READ_CHUNK];
    while raw.len() < MAX_RESPONSE_LENGTH {
        match stream.read(&mut chunk).await {
            Ok(read) => match chunk.get(..read) {
                Some(bytes) if read > 0 => raw.extend_from_slice(bytes),
                _ => break,
            },
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(raw)
}

/// Exchanges a refresh token for a new pair at the token endpoint of the configuration.
///
/// The endpoint is reached through `connector`, which is the TLS connector outside tests. The
/// old pair is invalid once this returns a new one.
///
/// # Errors
///
/// Returns [`ConnectorError::Token`] when the endpoint refuses the refresh or answers with
/// something else than a token document, [`ConnectorError::Timeout`] when it does not answer
/// within the timeout of the configuration, [`ConnectorError::Transport`] when the exchange
/// fails, and [`ConnectorError::Config`] when the token URL has no usable host.
pub async fn refresh_over_http<C: Connect>(
    connector: &C,
    configuration: &ConfigurationOpenApi,
    refresh: &RefreshToken,
) -> Result<TokenPair, ConnectorError> {
    let endpoint = Endpoint::parse(configuration.token_url())?;
    let target = endpoint.target(&[
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh.expose()),
        ("client_id", configuration.client_id().as_str()),
        ("client_secret", configuration.client_secret().expose()),
    ]);
    let request = format!(
        "GET {target} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nUser-Agent: \
         ctrader-connector/{}\r\nConnection: close\r\n\r\n",
        endpoint.host,
        crate::VERSION
    );
    let limit = configuration.timeout();
    let raw = timeout(limit, exchange(connector, &endpoint, &request))
        .await
        .map_err(|_| ConnectorError::Timeout(limit))??;
    let (status, body) = http_body(&raw)?;
    match parse_token_response(&body) {
        Err(TokenError::Malformed(_)) if !(200..300).contains(&status) => {
            Err(TokenError::Status(status).into())
        }
        outcome => Ok(outcome?),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Endpoint, TokenPair, http_body, parse_token_response, percent_encode, unchunk};
    use crate::common::errors::{ConfigError, TokenError};
    use crate::open_api::messages::ProtoOaRefreshTokenRes;

    #[test]
    fn token_document_is_parsed() {
        let pair = parse_token_response(
            r#"{"accessToken":"a1","refreshToken":"r1","expiresIn":2628000,"tokenType":"bearer","errorCode":null}"#,
        )
        .expect("pair");
        assert_eq!(pair.access.expose(), "a1");
        assert_eq!(pair.refresh.expose(), "r1");
        assert_eq!(pair.expires_in, Duration::from_secs(2_628_000));
        assert_eq!(pair.token_type, "bearer");
        let debug = format!("{pair:?}");
        assert!(!debug.contains("a1") && !debug.contains("r1"), "{debug}");
        let untyped =
            parse_token_response(r#"{"accessToken":"a","refreshToken":"r","expiresIn":1}"#)
                .expect("pair");
        assert_eq!(untyped.token_type, "bearer");
    }

    #[test]
    fn refusal_keeps_code_and_description() {
        assert_eq!(
            parse_token_response(r#"{"errorCode":"ACCESS_DENIED","description":"no"}"#),
            Err(TokenError::Refused {
                code: "ACCESS_DENIED".to_owned(),
                description: "no".to_owned(),
            })
        );
        assert_eq!(
            parse_token_response(r#"{"errorCode":"X"}"#),
            Err(TokenError::Refused {
                code: "X".to_owned(),
                description: String::new(),
            })
        );
    }

    #[test]
    fn incomplete_documents_name_the_missing_part_only() {
        let cases = [
            ("not json", "JSON"),
            (r#"{"refreshToken":"r","expiresIn":1}"#, "accessToken"),
            (r#"{"accessToken":"a","expiresIn":1}"#, "refreshToken"),
            (r#"{"accessToken":"a","refreshToken":"r"}"#, "expiresIn"),
            (
                r#"{"accessToken":"","refreshToken":"r","expiresIn":1}"#,
                "accessToken",
            ),
            (
                r#"{"accessToken":"a","refreshToken":" ","expiresIn":1}"#,
                "refreshToken",
            ),
        ];
        for (body, part) in cases {
            assert_eq!(
                parse_token_response(body),
                Err(TokenError::Malformed(part)),
                "{part}"
            );
        }
    }

    #[test]
    fn socket_response_becomes_a_pair() {
        let response = ProtoOaRefreshTokenRes {
            payload_type: None,
            access_token: "a".to_owned(),
            token_type: "bearer".to_owned(),
            expires_in: 60,
            refresh_token: "r".to_owned(),
        };
        let pair = TokenPair::try_from(response.clone()).expect("pair");
        assert_eq!(pair.expires_in, Duration::from_secs(60));
        let negative = ProtoOaRefreshTokenRes {
            expires_in: -1,
            ..response
        };
        assert_eq!(
            TokenPair::try_from(negative),
            Err(TokenError::Malformed("expiresIn"))
        );
    }

    #[test]
    fn endpoint_is_split_into_host_port_and_path() {
        let endpoint = |host: &str, port, path: &str| Endpoint {
            host: host.to_owned(),
            port,
            path: path.to_owned(),
        };
        assert_eq!(
            Endpoint::parse("https://example.com/apps/token"),
            Ok(endpoint("example.com", 443, "/apps/token"))
        );
        assert_eq!(
            Endpoint::parse("https://example.com:8443"),
            Ok(endpoint("example.com", 8443, "/"))
        );
        assert_eq!(
            Endpoint::parse("https://example.com?x=1"),
            Ok(endpoint("example.com", 443, "/?x=1"))
        );
        for url in [
            "http://example.com/",
            "https://:1/",
            "https://example.com:0/",
            "https://example.com:port/",
        ] {
            assert_eq!(
                Endpoint::parse(url),
                Err(ConfigError::InvalidTokenUrl(url.to_owned()))
            );
        }
    }

    #[test]
    fn parameters_are_percent_encoded_into_the_target() {
        let endpoint = Endpoint::parse("https://example.com/token").expect("endpoint");
        assert_eq!(
            endpoint.target(&[("a", "x y"), ("b", "1+2/3=~")]),
            "/token?a=x%20y&b=1%2B2%2F3%3D~"
        );
        let with_query = Endpoint::parse("https://example.com/token?v=2").expect("endpoint");
        assert_eq!(with_query.target(&[("a", "b")]), "/token?v=2&a=b");
        let mut out = String::new();
        percent_encode("A-z_0.9~é", &mut out);
        assert_eq!(out, "A-z_0.9~%C3%A9");
    }

    #[test]
    fn http_response_is_split_into_status_and_body() {
        assert_eq!(
            http_body(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"),
            Ok((200, "{}".to_owned()))
        );
        assert_eq!(
            http_body(
                b"HTTP/1.1 400 Bad\r\nTransfer-Encoding: chunked\r\n\r\n3\r\n{\"a\r\n4;x=1\r\n\":1}\r\n0\r\n\r\n"
            ),
            Ok((400, "{\"a\":1}".to_owned()))
        );
        assert_eq!(
            http_body(b"HTTP/1.1 204 No Content\r\n\r\n"),
            Ok((204, String::new()))
        );
        for raw in [
            &b"HTTP/1.1 200 OK\r\n"[..],
            b"HTTP/1.1\r\n\r\n",
            b"HTTP/1.1 abc OK\r\n\r\n",
            b"\xff\xfe 200\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n\r\n\xff",
        ] {
            assert_eq!(http_body(raw), Err(TokenError::Malformed("HTTP response")));
        }
    }

    #[test]
    fn broken_chunks_are_rejected() {
        for raw in [
            &b"5\r\nab"[..],
            b"zz\r\nab\r\n0\r\n\r\n",
            b"2\r\nab",
            b"2",
            b"\xff\r\n",
            b"2\r\nab\r\n",
        ] {
            assert_eq!(unchunk(raw), Err(TokenError::Malformed("chunked body")));
        }
        assert_eq!(unchunk(b"0\r\n\r\n"), Ok(Vec::new()));
    }
}
