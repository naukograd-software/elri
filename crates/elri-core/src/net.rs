//! Outgoing HTTP for the core: one client per proxy configuration. The
//! proxy comes from the device settings: the operating system (GNOME or
//! KDE on Linux, the system configuration on macOS and Windows), the
//! environment, a URL given by the user, or none.

use std::sync::RwLock;
use std::time::{Duration, Instant};

use elri_proto::elri::v1::settings::{ProxyMode, ProxySettings};

/// Network setup failures.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    /// The proxy URL does not parse.
    #[error("proxy {0}: not a valid http, https or socks5 URL")]
    BadProxy(String),
    /// The HTTP client could not be built.
    #[error("http client: {0}")]
    Client(String),
}

/// Where requests go, decided from the settings and the platform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// No proxy.
    Direct,
    /// Through `url`, except hosts in `no_proxy`.
    Via {
        /// Proxy URL.
        url: String,
        /// NO_PROXY-style bypass list.
        no_proxy: String,
        /// `manual`, `environment`, `gnome`, `kde`.
        source: &'static str,
    },
    /// Whatever the platform's own resolver says (macOS, Windows), with
    /// the environment as fallback.
    Platform,
}

/// A proxy the desktop environment configures, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesktopProxy {
    /// Proxy URL.
    pub url: String,
    /// Bypass list.
    pub no_proxy: String,
    /// `gnome` or `kde`.
    pub source: &'static str,
}

/// The environment's proxy, HTTPS first.
fn env_proxy(env: &dyn Fn(&str) -> Option<String>) -> Option<(String, String)> {
    let get = |names: &[&str]| {
        names
            .iter()
            .find_map(|n| env(n).filter(|v| !v.trim().is_empty()))
    };
    let url = get(&[
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ])?;
    let no_proxy = get(&["NO_PROXY", "no_proxy"]).unwrap_or_default();
    Some((url.trim().to_owned(), no_proxy))
}

/// Decides where requests go. `env` reads environment variables and
/// `desktop` is the desktop environment's proxy, when known.
pub fn plan(
    settings: &ProxySettings,
    env: &dyn Fn(&str) -> Option<String>,
    desktop: Option<DesktopProxy>,
) -> Plan {
    match settings.mode() {
        ProxyMode::None => Plan::Direct,
        ProxyMode::Manual => {
            if settings.url.trim().is_empty() {
                Plan::Direct
            } else {
                Plan::Via {
                    url: settings.url.trim().to_owned(),
                    no_proxy: settings.no_proxy.clone(),
                    source: "manual",
                }
            }
        }
        ProxyMode::Environment => match env_proxy(env) {
            Some((url, no_proxy)) => Plan::Via {
                url,
                no_proxy,
                source: "environment",
            },
            None => Plan::Direct,
        },
        ProxyMode::System | ProxyMode::Unspecified => {
            if let Some(d) = desktop {
                return Plan::Via {
                    url: d.url,
                    no_proxy: d.no_proxy,
                    source: d.source,
                };
            }
            if cfg!(any(target_os = "macos", target_os = "windows")) {
                return Plan::Platform;
            }
            match env_proxy(env) {
                Some((url, no_proxy)) => Plan::Via {
                    url,
                    no_proxy,
                    source: "environment",
                },
                None => Plan::Direct,
            }
        }
    }
}

/// The proxy GNOME or KDE configures, on Linux.
pub fn desktop_proxy() -> Option<DesktopProxy> {
    #[cfg(all(target_os = "linux", not(target_os = "android")))]
    {
        gnome_proxy().or_else(kde_proxy)
    }
    #[cfg(not(all(target_os = "linux", not(target_os = "android"))))]
    {
        None
    }
}

#[cfg(all(target_os = "linux", not(target_os = "android")))]
fn gsettings(schema: &str, key: &str) -> Option<String> {
    let out = std::process::Command::new("gsettings")
        .args(["get", schema, key])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

#[cfg(all(target_os = "linux", not(target_os = "android")))]
fn gnome_proxy() -> Option<DesktopProxy> {
    if gsettings("org.gnome.system.proxy", "mode")?.trim_matches('\'') != "manual" {
        return None;
    }
    let unquote = |s: String| s.trim_matches('\'').to_owned();
    for (schema, scheme) in [
        ("org.gnome.system.proxy.https", "http"),
        ("org.gnome.system.proxy.http", "http"),
        ("org.gnome.system.proxy.socks", "socks5"),
    ] {
        let host = unquote(gsettings(schema, "host")?);
        let port = gsettings(schema, "port")?;
        if host.is_empty() || port == "0" {
            continue;
        }
        let ignore = gsettings("org.gnome.system.proxy", "ignore-hosts").unwrap_or_default();
        let no_proxy = ignore
            .trim_start_matches('@')
            .trim_start_matches("as ")
            .trim_matches(|c| c == '[' || c == ']')
            .split(',')
            .map(|h| h.trim().trim_matches('\'').to_owned())
            .filter(|h| !h.is_empty())
            .collect::<Vec<_>>()
            .join(",");
        return Some(DesktopProxy {
            url: format!("{scheme}://{host}:{port}"),
            no_proxy,
            source: "gnome",
        });
    }
    None
}

#[cfg(all(target_os = "linux", not(target_os = "android")))]
fn kde_proxy() -> Option<DesktopProxy> {
    let home = std::env::var_os("HOME")?;
    let path = std::path::Path::new(&home).join(".config/kioslaverc");
    parse_kioslaverc(&std::fs::read_to_string(path).ok()?)
}

/// Reads KDE's proxy section: type 1 is a manual proxy.
pub fn parse_kioslaverc(text: &str) -> Option<DesktopProxy> {
    let mut in_section = false;
    let mut kind = None;
    let mut https = None;
    let mut http = None;
    let mut socks = None;
    let mut no_proxy = String::new();
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_section = line == "[Proxy Settings]";
            continue;
        }
        if !in_section {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let v = v.trim().to_owned();
        match k.trim() {
            "ProxyType" => kind = Some(v),
            "httpsProxy" => https = Some(v),
            "httpProxy" => http = Some(v),
            "socksProxy" => socks = Some(v),
            "NoProxyFor" => no_proxy = v,
            _ => {}
        }
    }
    if kind.as_deref() != Some("1") {
        return None;
    }
    // KDE writes `http://host port` or `http://host:port`.
    let normalize = |s: String| {
        let s = s.trim().to_owned();
        match s.rsplit_once(' ') {
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => format!("{h}:{p}"),
            _ => s,
        }
    };
    let url = https
        .filter(|s| !s.is_empty())
        .or(http.filter(|s| !s.is_empty()))
        .map(normalize)
        .or_else(|| {
            socks
                .filter(|s| !s.is_empty())
                .map(normalize)
                .map(|s| s.replacen("socks://", "socks5://", 1))
        })?;
    Some(DesktopProxy {
        url,
        no_proxy,
        source: "kde",
    })
}

/// Secret-store key of a manual proxy's password.
pub const PROXY_PASSWORD_KEY: &str = "proxy-password";

/// Splits a proxy URL into the URL without its password and the password.
pub fn split_password(url: &str) -> (String, Option<String>) {
    match reqwest::Url::parse(url) {
        Ok(mut u) if u.password().is_some() => {
            let pw = u.password().map(urlish_decode);
            let _ = u.set_password(None);
            (u.to_string(), pw)
        }
        _ => (url.to_owned(), None),
    }
}

/// Puts a password back into a proxy URL that names a user.
pub fn with_password(url: &str, password: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut u) if !u.username().is_empty() => {
            let _ = u.set_password(Some(password));
            u.to_string()
        }
        _ => url.to_owned(),
    }
}

/// A URL with its password replaced, for display.
pub fn redacted(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut u) if u.password().is_some() => {
            let _ = u.set_password(Some("***"));
            u.to_string()
        }
        Ok(u) => u.to_string(),
        Err(_) => url.to_owned(),
    }
}

/// Whether `host` matches a `NO_PROXY` list.
pub fn bypassed(host: &str, no_proxy: &str) -> bool {
    let host = host.to_ascii_lowercase();
    no_proxy
        .split(',')
        .map(|e| e.trim().to_ascii_lowercase())
        .filter(|e| !e.is_empty())
        .any(|e| {
            if e == "*" {
                return true;
            }
            let e = e.trim_start_matches("*.").trim_start_matches('.');
            let e = e.split(':').next().unwrap_or(e);
            host == e || host.ends_with(&format!(".{e}"))
        })
}

fn build(plan: &Plan) -> Result<reqwest::Client, NetError> {
    let mut b = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(20))
        .user_agent(concat!("elri/", env!("CARGO_PKG_VERSION")));
    #[cfg(target_os = "android")]
    {
        // The platform verifier needs a JVM handle on Android; the
        // bundled roots do not.
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        b = b.tls_backend_preconfigured(config);
    }
    b = match plan {
        Plan::Direct => b.no_proxy(),
        Plan::Platform => b,
        Plan::Via { url, no_proxy, .. } => {
            let proxy = reqwest::Proxy::all(url.as_str())
                .map_err(|_| NetError::BadProxy(redacted(url)))?
                .no_proxy(reqwest::NoProxy::from_string(no_proxy));
            b.proxy(proxy)
        }
    };
    b.build()
        .map_err(|e| NetError::Client(e.without_url().to_string()))
}

fn base64(input: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(char::from(A[(n >> 18) as usize & 63]));
        out.push(char::from(A[(n >> 12) as usize & 63]));
        out.push(if chunk.len() > 1 {
            char::from(A[(n >> 6) as usize & 63])
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            char::from(A[n as usize & 63])
        } else {
            '='
        });
    }
    out
}

/// Opens a TCP stream to `host:port` through `proxy` (`http://` CONNECT
/// or `socks5://`).
pub async fn tunnel(proxy: &str, host: &str, port: u16) -> std::io::Result<tokio::net::TcpStream> {
    let parsed = reqwest::Url::parse(proxy).map_err(|_| invalid_input("proxy url"))?;
    let proxy_host = parsed
        .host_str()
        .ok_or_else(|| invalid_input("proxy host"))?
        .to_owned();
    let creds = (
        urlish_decode(parsed.username()),
        urlish_decode(parsed.password().unwrap_or("")),
    );
    match parsed.scheme() {
        "http" => {
            let via = (proxy_host.as_str(), parsed.port().unwrap_or(80));
            http_connect(via, &creds, host, port).await
        }
        "socks5" | "socks5h" => {
            let via = (proxy_host.as_str(), parsed.port().unwrap_or(1080));
            socks5_connect(via, &creds, host, port).await
        }
        other => Err(invalid_input(&format!("proxy scheme {other} for tunnels"))),
    }
}

fn invalid_input(m: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, m.to_owned())
}

async fn http_connect(
    via: (&str, u16),
    creds: &(String, String),
    host: &str,
    port: u16,
) -> std::io::Result<tokio::net::TcpStream> {
    use std::fmt::Write as _;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(via).await?;
    let mut request = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n");
    if !creds.0.is_empty() {
        let token = base64(format!("{}:{}", creds.0, creds.1).as_bytes());
        let _ = write!(request, "Proxy-Authorization: Basic {token}\r\n");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).await? == 0 || head.len() > 16 * 1024 {
            return Err(std::io::Error::other("proxy closed during CONNECT"));
        }
        head.push(byte[0]);
    }
    let text = String::from_utf8_lossy(&head);
    let first = text.lines().next().unwrap_or("");
    if first.split_whitespace().nth(1) == Some("200") {
        Ok(stream)
    } else {
        Err(std::io::Error::other(format!(
            "proxy refused CONNECT: {first}"
        )))
    }
}

async fn socks5_connect(
    via: (&str, u16),
    creds: &(String, String),
    host: &str,
    port: u16,
) -> std::io::Result<tokio::net::TcpStream> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(via).await?;
    let with_auth = !creds.0.is_empty();
    stream
        .write_all(if with_auth { &[5, 1, 2] } else { &[5, 1, 0] })
        .await?;
    let mut reply = [0u8; 2];
    stream.read_exact(&mut reply).await?;
    match (reply[0], reply[1]) {
        (5, 0) => {}
        (5, 2) if with_auth => {
            let mut auth = vec![
                1,
                u8::try_from(creds.0.len()).map_err(|_| invalid_input("user"))?,
            ];
            auth.extend_from_slice(creds.0.as_bytes());
            auth.push(u8::try_from(creds.1.len()).map_err(|_| invalid_input("password"))?);
            auth.extend_from_slice(creds.1.as_bytes());
            stream.write_all(&auth).await?;
            stream.read_exact(&mut reply).await?;
            if reply[1] != 0 {
                return Err(std::io::Error::other("socks5 authentication failed"));
            }
        }
        _ => return Err(std::io::Error::other("socks5 method refused")),
    }
    let mut connect = vec![
        5,
        1,
        0,
        3,
        u8::try_from(host.len()).map_err(|_| invalid_input("host"))?,
    ];
    connect.extend_from_slice(host.as_bytes());
    connect.extend_from_slice(&port.to_be_bytes());
    stream.write_all(&connect).await?;
    let mut head = [0u8; 4];
    stream.read_exact(&mut head).await?;
    if head[1] != 0 {
        return Err(std::io::Error::other(format!(
            "socks5 connect failed: {}",
            head[1]
        )));
    }
    let address = match head[3] {
        1 => 4,
        4 => 16,
        3 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            usize::from(len[0])
        }
        _ => return Err(std::io::Error::other("socks5 reply")),
    };
    let mut rest = vec![0u8; address + 2];
    stream.read_exact(&mut rest).await?;
    Ok(stream)
}

fn urlish_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

struct Built {
    settings: ProxySettings,
    plan: Plan,
    client: reqwest::Client,
    at: Instant,
}

/// How long a client built from the system's proxy is trusted before the
/// system is asked again.
const SYSTEM_TTL: Duration = Duration::from_secs(60);

/// The core's HTTP clients.
pub struct Net {
    current: RwLock<Built>,
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn plan_now(settings: &ProxySettings) -> Plan {
    let desktop = match settings.mode() {
        ProxyMode::System | ProxyMode::Unspecified => desktop_proxy(),
        _ => None,
    };
    plan(settings, &read_env, desktop)
}

impl Net {
    /// Clients for these proxy settings.
    pub fn new(settings: ProxySettings) -> Result<Self, NetError> {
        let plan = plan_now(&settings);
        let client = build(&plan)?;
        Ok(Self {
            current: RwLock::new(Built {
                settings,
                plan,
                client,
                at: Instant::now(),
            }),
        })
    }

    /// Switches to new proxy settings; a bad URL keeps the old ones.
    pub fn reconfigure(&self, settings: ProxySettings) -> Result<(), NetError> {
        let plan = plan_now(&settings);
        let client = build(&plan)?;
        let mut g = self
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *g = Built {
            settings,
            plan,
            client,
            at: Instant::now(),
        };
        Ok(())
    }

    /// The client to send with. A system-derived one is rebuilt now and
    /// then so a changed system proxy is picked up.
    pub fn client(&self) -> reqwest::Client {
        let stale = {
            let g = self
                .current
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let system = matches!(
                g.settings.mode(),
                ProxyMode::System | ProxyMode::Unspecified
            );
            if !system || g.at.elapsed() < SYSTEM_TTL {
                return g.client.clone();
            }
            g.settings.clone()
        };
        if let Err(e) = self.reconfigure(stale) {
            tracing::warn!(error = %e, "system proxy changed to something unusable");
        }
        self.current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .client
            .clone()
    }

    /// The proxy URL to tunnel a raw connection to `host` through, if
    /// any. The platform resolver of macOS and Windows is not consulted
    /// for raw tunnels; the environment is.
    pub fn tunnel_proxy(&self, host: &str) -> Option<String> {
        let plan = self
            .current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .plan
            .clone();
        match plan {
            Plan::Direct => None,
            Plan::Platform => env_proxy(&read_env)
                .filter(|(_, no)| !bypassed(host, no))
                .map(|(u, _)| u),
            Plan::Via { url, no_proxy, .. } => (!bypassed(host, &no_proxy)).then_some(url),
        }
    }

    /// The proxy a request to `url` goes through, redacted, and why.
    pub fn resolve(&self, url: &str) -> (String, String) {
        let plan = self
            .current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .plan
            .clone();
        let host = reqwest::Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned))
            .unwrap_or_default();
        match plan {
            Plan::Direct => (String::new(), "none".to_owned()),
            Plan::Platform => (String::new(), "system".to_owned()),
            Plan::Via {
                url: proxy,
                no_proxy,
                source,
            } => {
                if bypassed(&host, &no_proxy) {
                    (String::new(), "bypass".to_owned())
                } else {
                    (redacted(&proxy), source.to_owned())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    fn settings(mode: ProxyMode, url: &str) -> ProxySettings {
        ProxySettings {
            mode: mode.into(),
            url: url.into(),
            no_proxy: "localhost,.corp".into(),
        }
    }

    #[test]
    fn plans_follow_the_mode() {
        let env = |k: &str| match k {
            "https_proxy" => Some("http://env:3128".to_owned()),
            "NO_PROXY" => Some("internal".to_owned()),
            _ => None,
        };
        let none = |_: &str| None;
        assert_eq!(
            plan(&settings(ProxyMode::None, "x"), &env, None),
            Plan::Direct
        );
        assert_eq!(
            plan(&settings(ProxyMode::Manual, "http://m:8080"), &env, None),
            Plan::Via {
                url: "http://m:8080".into(),
                no_proxy: "localhost,.corp".into(),
                source: "manual"
            }
        );
        assert_eq!(
            plan(&settings(ProxyMode::Environment, ""), &env, None),
            Plan::Via {
                url: "http://env:3128".into(),
                no_proxy: "internal".into(),
                source: "environment"
            }
        );
        assert_eq!(
            plan(&settings(ProxyMode::Environment, ""), &none, None),
            Plan::Direct
        );
        let desktop = DesktopProxy {
            url: "http://gnome:1".into(),
            no_proxy: String::new(),
            source: "gnome",
        };
        assert_eq!(
            plan(&settings(ProxyMode::System, ""), &env, Some(desktop)),
            Plan::Via {
                url: "http://gnome:1".into(),
                no_proxy: String::new(),
                source: "gnome"
            }
        );
        if cfg!(target_os = "linux") {
            assert_eq!(
                plan(&settings(ProxyMode::Unspecified, ""), &env, None),
                Plan::Via {
                    url: "http://env:3128".into(),
                    no_proxy: "internal".into(),
                    source: "environment"
                }
            );
        }
    }

    #[test]
    fn kde_and_bypass_and_redaction() {
        let rc = "[General]\nx=1\n[Proxy Settings]\nProxyType=1\nhttpsProxy=http://kde.example 8080\nNoProxyFor=localhost,.lan\n";
        assert_eq!(
            parse_kioslaverc(rc),
            Some(DesktopProxy {
                url: "http://kde.example:8080".into(),
                no_proxy: "localhost,.lan".into(),
                source: "kde"
            })
        );
        assert_eq!(parse_kioslaverc("[Proxy Settings]\nProxyType=0\n"), None);
        assert!(bypassed("api.corp", "localhost,.corp"));
        assert!(bypassed("corp", "corp"));
        assert!(!bypassed("api.openai.com", "localhost,.corp"));
        assert!(bypassed("anything", "*"));
        assert_eq!(redacted("http://u:secret@p:1/"), "http://u:***@p:1/");
        let (bare, pw) = split_password("http://u:p%40ss@p:1/");
        assert_eq!(bare, "http://u@p:1/");
        assert_eq!(pw.as_deref(), Some("p@ss"));
        assert_eq!(with_password(&bare, "p@ss"), "http://u:p%40ss@p:1/");
        assert_eq!(
            split_password("http://p:1/"),
            ("http://p:1/".to_owned(), None)
        );
    }

    #[test]
    fn base64_matches_the_standard() {
        assert_eq!(base64(b"user:pass"), "dXNlcjpwYXNz");
        assert_eq!(base64(b"a"), "YQ==");
        assert_eq!(base64(b"ab"), "YWI=");
        assert_eq!(urlish_decode("p%40ss"), "p@ss");
    }

    #[tokio::test]
    async fn connect_tunnels_reach_the_target() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Target: echoes one line back.
        let target = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let tport = target.local_addr().expect("addr").port();
        tokio::spawn(async move {
            let (mut c, _) = target.accept().await.expect("accept");
            let mut buf = [0u8; 5];
            c.read_exact(&mut buf).await.expect("read");
            c.write_all(&buf).await.expect("write");
        });
        // Proxy: checks auth, answers 200, splices.
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let pport = proxy.local_addr().expect("addr").port();
        tokio::spawn(async move {
            let (mut c, _) = proxy.accept().await.expect("accept");
            let mut head = Vec::new();
            let mut b = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                c.read_exact(&mut b).await.expect("read");
                head.push(b[0]);
            }
            let text = String::from_utf8_lossy(&head).into_owned();
            assert!(
                text.starts_with(&format!("CONNECT 127.0.0.1:{tport} ")),
                "{text}"
            );
            assert!(
                text.contains("Proxy-Authorization: Basic dTpwQHNz"),
                "{text}"
            );
            let mut up = tokio::net::TcpStream::connect(("127.0.0.1", tport))
                .await
                .expect("up");
            c.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .expect("w");
            let _ = tokio::io::copy_bidirectional(&mut c, &mut up).await;
        });
        let mut s = tunnel(
            &format!("http://u:p%40ss@127.0.0.1:{pport}"),
            "127.0.0.1",
            tport,
        )
        .await
        .expect("tunnel");
        s.write_all(b"hello").await.expect("write");
        let mut back = [0u8; 5];
        s.read_exact(&mut back).await.expect("read");
        assert_eq!(&back, b"hello");
    }

    #[test]
    fn clients_build_for_every_plan_and_bad_urls_are_refused() {
        let net = Net::new(settings(ProxyMode::Manual, "http://127.0.0.1:9")).expect("net");
        assert_eq!(
            net.resolve("https://api.openai.com/"),
            ("http://127.0.0.1:9/".to_owned(), "manual".to_owned())
        );
        assert_eq!(net.resolve("http://x.corp/").1, "bypass");
        net.reconfigure(settings(ProxyMode::None, ""))
            .expect("none");
        assert_eq!(net.resolve("https://a/").1, "none");
        assert!(net
            .reconfigure(settings(ProxyMode::Manual, "::not a url::"))
            .is_err());
        assert_eq!(
            net.resolve("https://a/").1,
            "none",
            "a bad url keeps the old"
        );
        let _ = net.client();
    }
}
