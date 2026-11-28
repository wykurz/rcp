//! Transport security for Source↔Destination TCP connections.
//!
//! This module provides:
//! - Ephemeral self-signed certificate generation and mutual TLS 1.3 with fingerprint pinning for
//!   direct TCP connections (source is server; destination presents a client certificate, and both
//!   sides verify each other's SSH-distributed fingerprints).
//! - Loopback-only 1-RTT mutual HMAC-SHA256 authentication for SSH-tunneled connections, providing
//!   multi-user loopback isolation without adding a second bulk encryption layer inside SSH.
//! - The single bounded handshake funnel ([`accept_control_bounded`], [`accept_bounded`], and
//!   [`connect_bounded`]) used by every production control and data connection.
//!
//! Master↔rcpd control and tracing travel directly over SSH stdio and do not use this layer.
use anyhow::Context;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A certificate fingerprint (SHA-256 of DER-encoded certificate).
pub type Fingerprint = [u8; 32];

/// rcp pins TLS 1.3 for every connection — TLS 1.2 is never negotiated. This is safe to do
/// unconditionally: TLS 1.3 has been available at both ends since the TLS layer was introduced
/// (rustls has always offered it), so pinning never excludes a legitimate rcp/rcpd peer. A peer
/// that cannot negotiate 1.3 fails the handshake — the correct outcome — rather than silently
/// downgrading. This defense remains independent of the compatibility checks at discovery and
/// deployment boundaries.
const TLS_VERSIONS: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS13];

/// A certified key pair (certificate + private key) with its fingerprint.
#[derive(Clone)]
pub struct CertifiedKey {
    pub cert_der: Vec<u8>,
    pub key_der: Vec<u8>,
    pub fingerprint: Fingerprint,
}

/// Generates an ephemeral self-signed certificate using Ed25519.
///
/// The certificate is valid for 1 day (doesn't matter since ephemeral).
/// Returns the certificate, private key, and fingerprint.
pub fn generate_self_signed_cert() -> anyhow::Result<CertifiedKey> {
    use rcgen::{CertificateParams, KeyPair};
    // generate Ed25519 key pair
    let key_pair = KeyPair::generate_for(&rcgen::PKCS_ED25519)?;
    // create certificate parameters with random subject
    let mut params = CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        format!("rcp-{}", rand::random::<u64>()),
    );
    // self-sign the certificate
    let cert = params.self_signed(&key_pair)?;
    let cert_der = cert.der().to_vec();
    let key_der = key_pair.serialize_der();
    // compute fingerprint
    let fingerprint = compute_fingerprint(&cert_der);
    Ok(CertifiedKey {
        cert_der,
        key_der,
        fingerprint,
    })
}

/// Returns the ring provider's signature-verification algorithms, used to check the peer's
/// TLS `CertificateVerify` signature. Fingerprint pinning proves the peer presented the
/// expected certificate; this proves the peer also holds the matching private key.
fn signature_verification_algorithms() -> rustls::crypto::WebPkiSupportedAlgorithms {
    rustls::crypto::ring::default_provider().signature_verification_algorithms
}

/// Computes SHA-256 fingerprint of a DER-encoded certificate.
pub fn compute_fingerprint(cert_der: &[u8]) -> Fingerprint {
    let mut hasher = Sha256::new();
    hasher.update(cert_der);
    hasher.finalize().into()
}

/// Converts a fingerprint to lowercase hex string (64 characters).
pub fn fingerprint_to_hex(fp: &Fingerprint) -> String {
    hex::encode(fp)
}

/// Parses a fingerprint from hex string.
pub fn fingerprint_from_hex(s: &str) -> anyhow::Result<Fingerprint> {
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        anyhow::bail!(
            "fingerprint must be 32 bytes (64 hex chars), got {}",
            bytes.len()
        );
    }
    let mut fp = [0u8; 32];
    fp.copy_from_slice(&bytes);
    Ok(fp)
}

/// Creates a TLS server config without client authentication.
///
/// Test-only — every production listener requires a verified client certificate
/// (see [`create_server_config_with_client_auth`]).
#[cfg(test)]
fn create_server_config(cert_key: &CertifiedKey) -> anyhow::Result<Arc<ServerConfig>> {
    let cert = CertificateDer::from(cert_key.cert_der.clone());
    let key = PrivateKeyDer::try_from(cert_key.key_der.clone())
        .map_err(|e| anyhow::anyhow!("invalid private key: {e}"))?;
    let config = ServerConfig::builder_with_protocol_versions(TLS_VERSIONS)
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    Ok(Arc::new(config))
}

/// Creates a TLS server config with client certificate verification.
///
/// Used for source's listeners for the destination (verifies destination's client cert).
pub fn create_server_config_with_client_auth(
    cert_key: &CertifiedKey,
    expected_client_fingerprint: Fingerprint,
) -> anyhow::Result<Arc<ServerConfig>> {
    let cert = CertificateDer::from(cert_key.cert_der.clone());
    let key = PrivateKeyDer::try_from(cert_key.key_der.clone())
        .map_err(|e| anyhow::anyhow!("invalid private key: {e}"))?;
    let client_verifier = Arc::new(FingerprintClientCertVerifier::new(
        expected_client_fingerprint,
    ));
    let config = ServerConfig::builder_with_protocol_versions(TLS_VERSIONS)
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(vec![cert], key)?;
    Ok(Arc::new(config))
}

/// Creates a TLS client config that verifies the server's certificate fingerprint
/// without presenting a client certificate.
///
/// Test-only — production clients always present a client certificate
/// (see [`create_client_config_with_cert`]).
#[cfg(test)]
fn create_client_config(expected_server_fingerprint: Fingerprint) -> Arc<ClientConfig> {
    let verifier = Arc::new(FingerprintServerCertVerifier::new(
        expected_server_fingerprint,
    ));
    let config = ClientConfig::builder_with_protocol_versions(TLS_VERSIONS)
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Arc::new(config)
}

/// Creates a TLS client config with a client certificate.
///
/// Used for destination→source connections (destination presents its certificate for source to
/// verify).
pub fn create_client_config_with_cert(
    client_cert_key: &CertifiedKey,
    expected_server_fingerprint: Fingerprint,
) -> anyhow::Result<Arc<ClientConfig>> {
    let verifier = Arc::new(FingerprintServerCertVerifier::new(
        expected_server_fingerprint,
    ));
    let cert = CertificateDer::from(client_cert_key.cert_der.clone());
    let key = PrivateKeyDer::try_from(client_cert_key.key_der.clone())
        .map_err(|e| anyhow::anyhow!("invalid private key: {e}"))?;
    let config = ClientConfig::builder_with_protocol_versions(TLS_VERSIONS)
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(vec![cert], key)?;
    Ok(Arc::new(config))
}

/// Server certificate verifier that checks the certificate's fingerprint.
#[derive(Debug)]
struct FingerprintServerCertVerifier {
    expected_fingerprint: Fingerprint,
}

impl FingerprintServerCertVerifier {
    fn new(expected_fingerprint: Fingerprint) -> Self {
        Self {
            expected_fingerprint,
        }
    }
}

impl ServerCertVerifier for FingerprintServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let actual_fingerprint = compute_fingerprint(end_entity.as_ref());
        if actual_fingerprint == self.expected_fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            tracing::error!(
                "TLS server certificate fingerprint mismatch: expected {}, got {}",
                fingerprint_to_hex(&self.expected_fingerprint),
                fingerprint_to_hex(&actual_fingerprint)
            );
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::BadSignature,
            ))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // the fingerprint check above pins WHICH certificate we accept; this verifies the peer
        // signed the handshake with that certificate's private key (proof of possession)
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &signature_verification_algorithms(),
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &signature_verification_algorithms(),
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        signature_verification_algorithms().supported_schemes()
    }
}

/// Client certificate verifier that checks the certificate's fingerprint.
#[derive(Debug)]
struct FingerprintClientCertVerifier {
    expected_fingerprint: Fingerprint,
}

impl FingerprintClientCertVerifier {
    fn new(expected_fingerprint: Fingerprint) -> Self {
        Self {
            expected_fingerprint,
        }
    }
}

impl ClientCertVerifier for FingerprintClientCertVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let actual_fingerprint = compute_fingerprint(end_entity.as_ref());
        if actual_fingerprint == self.expected_fingerprint {
            Ok(ClientCertVerified::assertion())
        } else {
            tracing::error!(
                "TLS client certificate fingerprint mismatch: expected {}, got {}",
                fingerprint_to_hex(&self.expected_fingerprint),
                fingerprint_to_hex(&actual_fingerprint)
            );
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::BadSignature,
            ))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // the fingerprint check above pins WHICH certificate we accept; this verifies the peer
        // signed the handshake with that certificate's private key (proof of possession)
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &signature_verification_algorithms(),
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &signature_verification_algorithms(),
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        signature_verification_algorithms().supported_schemes()
    }
    fn client_auth_mandatory(&self) -> bool {
        true
    }
}

// ============================================================================
// Bounded handshakes (mutual TLS 1.3 and loopback SSH-tunnel HMAC-SHA256)
// ============================================================================
//
// EVERY production TLS or SSH-auth handshake goes through `accept_control_bounded` /
// `accept_bounded` / `connect_bounded`. A bare `TlsAcceptor::accept` / `TlsConnector::connect`
// outside this module is a bug and is rejected by `scripts/check-tls-handshake-timeout.sh`.
//
// Why a shared helper rather than a timeout at each call site: the handshake runs AFTER the TCP
// accept/connect has already been bounded, so a peer that completes TCP and then sends no bytes
// stalls the future forever — and on the rcpd listener, whose accepts are sequential and inline,
// one such peer blocks the legitimate peer from ever connecting. That timeout was added at three
// call sites and forgotten at two, which is exactly the failure mode a single funnel removes. The
// helpers also own the identical `split` → `Box` → `SendStream`/`RecvStream` wrapping (and the
// SSH-auth / plaintext paths) that every site otherwise repeats.

const SSH_AUTH_MAGIC: [u8; 4] = *b"RCPA";
const TLS_HANDSHAKE_RECORD_TYPE: u8 = 0x16;
const SSH_AUTH_SERVER_DOMAIN: &[u8] = b"rcp-ssh-auth-v1:server";
const SSH_AUTH_CLIENT_DOMAIN: &[u8] = b"rcp-ssh-auth-v1:client";

/// Logical stream purpose bound into the SSH-tunnel HMAC authentication transcript so a control
/// handshake cannot be replayed onto a data stream or vice versa.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind {
    Control,
    Data,
}

impl StreamKind {
    const fn domain_byte(self) -> u8 {
        match self {
            Self::Control => 0x01,
            Self::Data => 0x02,
        }
    }
}

/// Concrete single-mode server stream acceptor (used for data connections after control resolves).
#[derive(Clone)]
pub enum StreamAcceptor {
    Tls(Arc<tokio_rustls::TlsAcceptor>),
    SshAuth(crate::protocol::SshAuthSecret),
    Plaintext,
}

impl StreamAcceptor {
    /// Human-readable label for logging.
    #[must_use]
    pub const fn description(&self) -> &'static str {
        match self {
            Self::Tls(_) => "mutual TLS 1.3",
            Self::SshAuth(_) => "SSH loopback HMAC-SHA256 auth",
            Self::Plaintext => "disabled (plaintext)",
        }
    }
}

/// Server stream acceptor for the initial source control connection.
///
/// In `--remote-transport=auto`, [`Self::Auto`] inspects the first client byte on the control
/// connection (requiring a loopback peer IP for [`StreamAcceptor::SshAuth`]) and locks the
/// resolved concrete [`StreamAcceptor`] for all subsequent data connections.
#[derive(Clone)]
pub enum ControlStreamAcceptor {
    Tls(Arc<tokio_rustls::TlsAcceptor>),
    SshAuth(crate::protocol::SshAuthSecret),
    Auto {
        tls: Arc<tokio_rustls::TlsAcceptor>,
        ssh_auth: crate::protocol::SshAuthSecret,
    },
    Plaintext,
}

impl ControlStreamAcceptor {
    /// Construct the source control acceptor from the daemon's local TLS certificate and the
    /// master-supplied [`crate::protocol::PeerAuth`] policy.
    pub fn from_peer_auth(
        cert_key: Option<&CertifiedKey>,
        peer_auth: &crate::protocol::PeerAuth,
    ) -> anyhow::Result<Self> {
        peer_auth.validate_cert_presence(cert_key.is_some())?;
        match (peer_auth, cert_key) {
            (crate::protocol::PeerAuth::Tls { peer_fingerprint }, Some(cert)) => {
                let server_config = create_server_config_with_client_auth(cert, *peer_fingerprint)
                    .context("failed to create TLS server config with client auth")?;
                Ok(Self::Tls(Arc::new(tokio_rustls::TlsAcceptor::from(
                    server_config,
                ))))
            }
            (crate::protocol::PeerAuth::SshSharedSecret { secret }, None) => {
                Ok(Self::SshAuth(secret.clone()))
            }
            (
                crate::protocol::PeerAuth::AutoTlsOrSshSecret {
                    peer_fingerprint,
                    secret,
                },
                Some(cert),
            ) => {
                let server_config = create_server_config_with_client_auth(cert, *peer_fingerprint)
                    .context("failed to create TLS server config with client auth")?;
                Ok(Self::Auto {
                    tls: Arc::new(tokio_rustls::TlsAcceptor::from(server_config)),
                    ssh_auth: secret.clone(),
                })
            }
            (crate::protocol::PeerAuth::None, None) => Ok(Self::Plaintext),
            _ => unreachable!("validate_cert_presence checked certificate alignment"),
        }
    }

    /// Human-readable label for logging.
    #[must_use]
    pub const fn description(&self) -> &'static str {
        match self {
            Self::Tls(_) => "mutual TLS 1.3",
            Self::SshAuth(_) => "SSH loopback HMAC-SHA256 auth",
            Self::Auto { .. } => "auto (mutual TLS 1.3 or SSH loopback HMAC-SHA256 auth)",
            Self::Plaintext => "disabled (plaintext)",
        }
    }
}

/// Client stream connector used by destination for both control and data streams.
#[derive(Clone)]
pub enum StreamConnector {
    Tls(Arc<tokio_rustls::TlsConnector>),
    SshAuth(crate::protocol::SshAuthSecret),
    Plaintext,
}

impl StreamConnector {
    /// Construct the destination connector once transport selection knows whether the connection
    /// traverses an SSH tunnel (`via_ssh_tunnel`).
    pub fn from_peer_auth(
        cert_key: Option<&CertifiedKey>,
        peer_auth: &crate::protocol::PeerAuth,
        via_ssh_tunnel: bool,
    ) -> anyhow::Result<Self> {
        peer_auth.validate_cert_presence(cert_key.is_some())?;
        match (peer_auth, cert_key, via_ssh_tunnel) {
            (crate::protocol::PeerAuth::Tls { peer_fingerprint }, Some(cert), _) => {
                let client_config = create_client_config_with_cert(cert, *peer_fingerprint)
                    .context("failed to create TLS client config")?;
                Ok(Self::Tls(Arc::new(tokio_rustls::TlsConnector::from(
                    client_config,
                ))))
            }
            (crate::protocol::PeerAuth::SshSharedSecret { secret }, None, _) => {
                Ok(Self::SshAuth(secret.clone()))
            }
            (
                crate::protocol::PeerAuth::AutoTlsOrSshSecret {
                    peer_fingerprint, ..
                },
                Some(cert),
                false,
            ) => {
                let client_config = create_client_config_with_cert(cert, *peer_fingerprint)
                    .context("failed to create TLS client config")?;
                Ok(Self::Tls(Arc::new(tokio_rustls::TlsConnector::from(
                    client_config,
                ))))
            }
            (crate::protocol::PeerAuth::AutoTlsOrSshSecret { secret, .. }, Some(_), true) => {
                Ok(Self::SshAuth(secret.clone()))
            }
            (crate::protocol::PeerAuth::None, None, _) => Ok(Self::Plaintext),
            _ => unreachable!("validate_cert_presence checked certificate alignment"),
        }
    }

    /// True when [`connect_bounded`] performs a mutual end-to-end authentication handshake with
    /// source `rcpd` (so completion proves the peer is reachable, unlike plaintext TCP over `-L`).
    #[must_use]
    pub const fn authenticates_peer(&self) -> bool {
        matches!(self, Self::Tls(_) | Self::SshAuth(_))
    }

    /// Human-readable label for logging.
    #[must_use]
    pub const fn description(&self) -> &'static str {
        match self {
            Self::Tls(_) => "mutual TLS 1.3",
            Self::SshAuth(_) => "SSH loopback HMAC-SHA256 auth",
            Self::Plaintext => "disabled (plaintext)",
        }
    }
}

fn hmac_sha256(key: &[u8; 32], chunks: &[&[u8]]) -> [u8; 32] {
    const BLOCK_SIZE: usize = 64;
    let mut ipad = [0x36u8; BLOCK_SIZE];
    let mut opad = [0x5cu8; BLOCK_SIZE];
    for (idx, &byte) in key.iter().enumerate() {
        ipad[idx] ^= byte;
        opad[idx] ^= byte;
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    for chunk in chunks {
        inner.update(chunk);
    }
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    outer.finalize().into()
}

fn ct_eq_32(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for idx in 0..32 {
        diff |= left[idx] ^ right[idx];
    }
    std::hint::black_box(diff) == 0
}

fn ssh_auth_tag(
    secret: &crate::protocol::SshAuthSecret,
    domain: &[u8],
    kind: StreamKind,
    client_nonce: &[u8; 32],
    server_nonce: &[u8; 32],
) -> [u8; 32] {
    let kind_byte = [kind.domain_byte()];
    hmac_sha256(
        secret.as_bytes(),
        &[domain, &kind_byte, client_nonce, server_nonce],
    )
}

/// Return true when `ip` is a loopback address (IPv4 `127.0.0.0/8`, IPv6 `::1`, or IPv4-mapped
/// `::ffff:127.0.0.0/104` delivered by dual-stack IPv6 listeners).
#[must_use]
pub fn is_loopback_peer(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => v4.is_loopback(),
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

fn ensure_loopback_peer(ip: std::net::IpAddr, purpose: &str) -> anyhow::Result<()> {
    if !is_loopback_peer(ip) {
        anyhow::bail!(
            "refusing non-TLS SSH authentication handshake on {purpose} connection from non-loopback peer {ip}"
        );
    }
    Ok(())
}

struct PrefixedStream<S> {
    prefix: Option<u8>,
    inner: S,
}

impl<S> PrefixedStream<S> {
    fn new(first_byte: u8, inner: S) -> Self {
        Self {
            prefix: Some(first_byte),
            inner,
        }
    }
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for PrefixedStream<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Some(byte) = self.prefix {
            if buf.remaining() > 0 {
                self.prefix = None;
                buf.put_slice(&[byte]);
            }
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for PrefixedStream<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

async fn accept_tls_stream<S>(
    acceptor: &tokio_rustls::TlsAcceptor,
    stream: S,
    purpose: &str,
) -> anyhow::Result<(
    crate::streams::BoxedSendStream,
    crate::streams::BoxedRecvStream,
)>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let tls_stream = acceptor
        .accept(stream)
        .await
        .with_context(|| format!("TLS handshake failed for {purpose} connection"))?;
    let (read_half, write_half) = tokio::io::split(tls_stream);
    Ok(split_into_streams(
        Box::new(write_half) as crate::streams::BoxedWrite,
        Box::new(read_half) as crate::streams::BoxedRead,
    ))
}

async fn accept_ssh_auth_stream(
    mut stream: crate::transport::BoxedTransportStream,
    first_byte: Option<u8>,
    secret: &crate::protocol::SshAuthSecret,
    kind: StreamKind,
    purpose: &str,
) -> anyhow::Result<(
    crate::streams::BoxedSendStream,
    crate::streams::BoxedRecvStream,
)> {
    let peer_ip = stream
        .peer_addr()
        .with_context(|| format!("failed to inspect peer address for {purpose} connection"))?
        .ip();
    ensure_loopback_peer(peer_ip, purpose)?;
    let mut client_hello = [0u8; 36];
    let read_result = match first_byte {
        Some(byte) => {
            client_hello[0] = byte;
            stream.read_exact(&mut client_hello[1..]).await
        }
        None => stream.read_exact(&mut client_hello).await,
    };
    read_result.with_context(|| {
        format!("SSH auth handshake failed reading client hello for {purpose} connection")
    })?;
    if client_hello[..4] != SSH_AUTH_MAGIC {
        anyhow::bail!("SSH auth handshake failed for {purpose} connection: invalid protocol magic");
    }
    let client_nonce: [u8; 32] = client_hello[4..36]
        .try_into()
        .expect("slice length is fixed at 32 bytes");
    let server_nonce: [u8; 32] = rand::random();
    let server_tag = ssh_auth_tag(
        secret,
        SSH_AUTH_SERVER_DOMAIN,
        kind,
        &client_nonce,
        &server_nonce,
    );
    let mut server_hello = [0u8; 64];
    server_hello[..32].copy_from_slice(&server_nonce);
    server_hello[32..64].copy_from_slice(&server_tag);
    stream.write_all(&server_hello).await.with_context(|| {
        format!("SSH auth handshake failed sending server challenge for {purpose} connection")
    })?;
    stream.flush().await.with_context(|| {
        format!("SSH auth handshake failed flushing server challenge for {purpose} connection")
    })?;
    let mut client_tag = [0u8; 32];
    stream.read_exact(&mut client_tag).await.with_context(|| {
        format!("SSH auth handshake failed reading client proof for {purpose} connection")
    })?;
    let expected_client_tag = ssh_auth_tag(
        secret,
        SSH_AUTH_CLIENT_DOMAIN,
        kind,
        &client_nonce,
        &server_nonce,
    );
    if !ct_eq_32(&client_tag, &expected_client_tag) {
        anyhow::bail!(
            "SSH auth handshake failed for {purpose} connection: client authentication tag mismatch"
        );
    }
    let (read_half, write_half) = stream.into_split();
    Ok(split_into_streams(write_half, read_half))
}

async fn connect_ssh_auth_stream(
    mut stream: crate::transport::BoxedTransportStream,
    secret: &crate::protocol::SshAuthSecret,
    kind: StreamKind,
    purpose: &str,
) -> anyhow::Result<(
    crate::streams::BoxedSendStream,
    crate::streams::BoxedRecvStream,
)> {
    let peer_ip = stream
        .peer_addr()
        .with_context(|| format!("failed to inspect peer address for {purpose} connection"))?
        .ip();
    ensure_loopback_peer(peer_ip, purpose)?;
    let client_nonce: [u8; 32] = rand::random();
    let mut client_hello = [0u8; 36];
    client_hello[..4].copy_from_slice(&SSH_AUTH_MAGIC);
    client_hello[4..36].copy_from_slice(&client_nonce);
    stream.write_all(&client_hello).await.with_context(|| {
        format!("SSH auth handshake failed sending client hello for {purpose} connection")
    })?;
    stream.flush().await.with_context(|| {
        format!("SSH auth handshake failed flushing client hello for {purpose} connection")
    })?;
    let mut server_hello = [0u8; 64];
    stream
        .read_exact(&mut server_hello)
        .await
        .with_context(|| {
            format!("SSH auth handshake failed reading server response for {purpose} connection")
        })?;
    let server_nonce: [u8; 32] = server_hello[..32]
        .try_into()
        .expect("slice length is fixed at 32 bytes");
    let server_tag: [u8; 32] = server_hello[32..64]
        .try_into()
        .expect("slice length is fixed at 32 bytes");
    let expected_server_tag = ssh_auth_tag(
        secret,
        SSH_AUTH_SERVER_DOMAIN,
        kind,
        &client_nonce,
        &server_nonce,
    );
    if !ct_eq_32(&server_tag, &expected_server_tag) {
        anyhow::bail!(
            "SSH auth handshake failed for {purpose} connection: server authentication tag mismatch"
        );
    }
    let client_tag = ssh_auth_tag(
        secret,
        SSH_AUTH_CLIENT_DOMAIN,
        kind,
        &client_nonce,
        &server_nonce,
    );
    stream.write_all(&client_tag).await.with_context(|| {
        format!("SSH auth handshake failed sending client proof for {purpose} connection")
    })?;
    stream.flush().await.with_context(|| {
        format!("SSH auth handshake failed flushing client proof for {purpose} connection")
    })?;
    let (read_half, write_half) = stream.into_split();
    Ok(split_into_streams(write_half, read_half))
}

/// Authenticate and split an accepted source control connection, resolving [`ControlStreamAcceptor::Auto`]
/// into the concrete [`StreamAcceptor`] to enforce on all subsequent data connections.
///
/// The entire handshake (including first-byte dispatch in `Auto` mode) is bounded by `timeout`.
pub async fn accept_control_bounded(
    acceptor: &ControlStreamAcceptor,
    stream: impl Into<crate::transport::BoxedTransportStream>,
    timeout: std::time::Duration,
    purpose: &str,
) -> anyhow::Result<(
    crate::streams::BoxedSendStream,
    crate::streams::BoxedRecvStream,
    StreamAcceptor,
)> {
    let mut stream = stream.into();
    match acceptor {
        ControlStreamAcceptor::Plaintext => {
            let (read_half, write_half) = stream.into_split();
            let (send, recv) = split_into_streams(write_half, read_half);
            Ok((send, recv, StreamAcceptor::Plaintext))
        }
        ControlStreamAcceptor::Tls(tls) => {
            let (send, recv) =
                tokio::time::timeout(timeout, accept_tls_stream(tls, stream, purpose))
                    .await
                    .with_context(|| {
                        format!("TLS handshake timed out for {purpose} connection")
                    })??;
            Ok((send, recv, StreamAcceptor::Tls(Arc::clone(tls))))
        }
        ControlStreamAcceptor::SshAuth(secret) => {
            let (send, recv) = tokio::time::timeout(
                timeout,
                accept_ssh_auth_stream(stream, None, secret, StreamKind::Control, purpose),
            )
            .await
            .with_context(|| format!("SSH auth handshake timed out for {purpose} connection"))??;
            Ok((send, recv, StreamAcceptor::SshAuth(secret.clone())))
        }
        ControlStreamAcceptor::Auto { tls, ssh_auth } => tokio::time::timeout(timeout, async {
            let mut first_byte = [0u8; 1];
            let read_len = stream
                .read(&mut first_byte)
                .await
                .with_context(|| format!("handshake read failed for {purpose} connection"))?;
            if read_len == 0 {
                anyhow::bail!("peer closed {purpose} connection before starting handshake");
            }
            match first_byte[0] {
                TLS_HANDSHAKE_RECORD_TYPE => {
                    let prefixed = PrefixedStream::new(first_byte[0], stream);
                    let (send, recv) = accept_tls_stream(tls, prefixed, purpose).await?;
                    Ok((send, recv, StreamAcceptor::Tls(Arc::clone(tls))))
                }
                b if b == SSH_AUTH_MAGIC[0] => {
                    let (send, recv) = accept_ssh_auth_stream(
                        stream,
                        Some(b),
                        ssh_auth,
                        StreamKind::Control,
                        purpose,
                    )
                    .await?;
                    Ok((send, recv, StreamAcceptor::SshAuth(ssh_auth.clone())))
                }
                other => {
                    anyhow::bail!(
                        "unexpected handshake prefix byte 0x{other:02x} for {purpose} connection"
                    );
                }
            }
        })
        .await
        .with_context(|| format!("stream handshake timed out for {purpose} connection"))?,
    }
}

/// Authenticate an accepted connection under a concrete [`StreamAcceptor`] and split it into
/// framed streams, bounding the handshake by `timeout`.
///
/// `purpose` names the connection in error messages (e.g. "control", "data").
pub async fn accept_bounded(
    acceptor: &StreamAcceptor,
    stream: impl Into<crate::transport::BoxedTransportStream>,
    timeout: std::time::Duration,
    kind: StreamKind,
    purpose: &str,
) -> anyhow::Result<(
    crate::streams::BoxedSendStream,
    crate::streams::BoxedRecvStream,
)> {
    let stream = stream.into();
    match acceptor {
        StreamAcceptor::Plaintext => {
            let (read_half, write_half) = stream.into_split();
            Ok(split_into_streams(write_half, read_half))
        }
        StreamAcceptor::Tls(tls) => {
            tokio::time::timeout(timeout, accept_tls_stream(tls, stream, purpose))
                .await
                .with_context(|| format!("TLS handshake timed out for {purpose} connection"))?
        }
        StreamAcceptor::SshAuth(secret) => tokio::time::timeout(
            timeout,
            accept_ssh_auth_stream(stream, None, secret, kind, purpose),
        )
        .await
        .with_context(|| format!("SSH auth handshake timed out for {purpose} connection"))?,
    }
}

/// Authenticate a connected stream under [`StreamConnector`] and split it into framed streams,
/// bounding the handshake by `timeout`.
///
/// `server_name` is only an SNI placeholder for TLS — peers are verified by certificate
/// fingerprint, not hostname (see [`create_client_config_with_cert`]).
pub async fn connect_bounded(
    connector: &StreamConnector,
    server_name: &'static str,
    stream: impl Into<crate::transport::BoxedTransportStream>,
    timeout: std::time::Duration,
    kind: StreamKind,
    purpose: &str,
) -> anyhow::Result<(
    crate::streams::BoxedSendStream,
    crate::streams::BoxedRecvStream,
)> {
    let stream = stream.into();
    match connector {
        StreamConnector::Plaintext => {
            let (read_half, write_half) = stream.into_split();
            Ok(split_into_streams(write_half, read_half))
        }
        StreamConnector::Tls(tls) => {
            let server_name = ServerName::try_from(server_name)
                .with_context(|| format!("invalid server name for {purpose} connection"))?;
            let tls_stream = tokio::time::timeout(timeout, tls.connect(server_name, stream))
                .await
                .with_context(|| format!("TLS handshake timed out for {purpose} connection"))?
                .with_context(|| format!("TLS handshake failed for {purpose} connection"))?;
            let (read_half, write_half) = tokio::io::split(tls_stream);
            Ok(split_into_streams(
                Box::new(write_half) as crate::streams::BoxedWrite,
                Box::new(read_half) as crate::streams::BoxedRead,
            ))
        }
        StreamConnector::SshAuth(secret) => tokio::time::timeout(
            timeout,
            connect_ssh_auth_stream(stream, secret, kind, purpose),
        )
        .await
        .with_context(|| format!("SSH auth handshake timed out for {purpose} connection"))?,
    }
}

/// SNI placeholder for destination→source connections. Authentication is by certificate
/// fingerprint, so the name itself is never checked.
pub const SERVER_NAME_SOURCE: &str = "rcp";

fn split_into_streams(
    write_half: crate::streams::BoxedWrite,
    read_half: crate::streams::BoxedRead,
) -> (
    crate::streams::BoxedSendStream,
    crate::streams::BoxedRecvStream,
) {
    (
        crate::streams::SendStream::new(write_half),
        crate::streams::RecvStream::new(read_half),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install_crypto_provider() {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok(); // ignore if already installed
    }

    #[test]
    fn test_generate_cert_and_fingerprint() {
        install_crypto_provider();
        let cert_key = generate_self_signed_cert().unwrap();
        assert_eq!(cert_key.fingerprint.len(), 32);
        assert!(!cert_key.cert_der.is_empty());
        assert!(!cert_key.key_der.is_empty());
        // fingerprint should be deterministic
        let fp2 = compute_fingerprint(&cert_key.cert_der);
        assert_eq!(cert_key.fingerprint, fp2);
    }

    #[test]
    fn test_fingerprint_hex_roundtrip() {
        install_crypto_provider();
        let cert_key = generate_self_signed_cert().unwrap();
        let hex = fingerprint_to_hex(&cert_key.fingerprint);
        assert_eq!(hex.len(), 64);
        let fp2 = fingerprint_from_hex(&hex).unwrap();
        assert_eq!(cert_key.fingerprint, fp2);
    }

    #[test]
    fn test_fingerprint_from_hex_invalid() {
        // wrong length
        assert!(fingerprint_from_hex("abcd").is_err());
        // invalid hex
        assert!(fingerprint_from_hex("zzzz").is_err());
    }

    #[test]
    fn test_create_server_config() {
        install_crypto_provider();
        let cert_key = generate_self_signed_cert().unwrap();
        let config = create_server_config(&cert_key).unwrap();
        assert!(config.alpn_protocols.is_empty());
    }

    #[test]
    fn test_create_client_config() {
        install_crypto_provider();
        let fp = [0u8; 32];
        let config = create_client_config(fp);
        assert!(config.alpn_protocols.is_empty());
    }

    #[test]
    fn test_server_fingerprint_verifier_accepts_matching() {
        install_crypto_provider();
        let cert_key = generate_self_signed_cert().unwrap();
        let verifier = FingerprintServerCertVerifier::new(cert_key.fingerprint);
        let cert = CertificateDer::from(cert_key.cert_der);
        let server_name = ServerName::try_from("rcp").unwrap();
        let result = verifier.verify_server_cert(&cert, &[], &server_name, &[], UnixTime::now());
        assert!(result.is_ok());
    }

    #[test]
    fn test_server_fingerprint_verifier_rejects_mismatch() {
        install_crypto_provider();
        let cert_key = generate_self_signed_cert().unwrap();
        // use a different fingerprint (all zeros)
        let wrong_fingerprint = [0u8; 32];
        let verifier = FingerprintServerCertVerifier::new(wrong_fingerprint);
        let cert = CertificateDer::from(cert_key.cert_der);
        let server_name = ServerName::try_from("rcp").unwrap();
        let result = verifier.verify_server_cert(&cert, &[], &server_name, &[], UnixTime::now());
        assert!(result.is_err());
        // verify it's the right error type
        match result {
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::BadSignature)) => {}
            other => panic!("expected BadSignature error, got: {:?}", other),
        }
    }

    #[test]
    fn test_client_fingerprint_verifier_accepts_matching() {
        install_crypto_provider();
        let cert_key = generate_self_signed_cert().unwrap();
        let verifier = FingerprintClientCertVerifier::new(cert_key.fingerprint);
        let cert = CertificateDer::from(cert_key.cert_der);
        let result = verifier.verify_client_cert(&cert, &[], UnixTime::now());
        assert!(result.is_ok());
    }

    #[test]
    fn test_client_fingerprint_verifier_rejects_mismatch() {
        install_crypto_provider();
        let cert_key = generate_self_signed_cert().unwrap();
        // use a different fingerprint (all zeros)
        let wrong_fingerprint = [0u8; 32];
        let verifier = FingerprintClientCertVerifier::new(wrong_fingerprint);
        let cert = CertificateDer::from(cert_key.cert_der);
        let result = verifier.verify_client_cert(&cert, &[], UnixTime::now());
        assert!(result.is_err());
        // verify it's the right error type
        match result {
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::BadSignature)) => {}
            other => panic!("expected BadSignature error, got: {:?}", other),
        }
    }

    #[test]
    fn test_client_verifier_requires_auth() {
        install_crypto_provider();
        let verifier = FingerprintClientCertVerifier::new([0u8; 32]);
        assert!(verifier.client_auth_mandatory());
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    fn install_crypto_provider() {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
    }

    /// Test TLS handshake succeeds with correct fingerprints.
    #[tokio::test]
    async fn test_tls_handshake_success_with_matching_fingerprint() {
        install_crypto_provider();
        // generate server certificate
        let server_cert = generate_self_signed_cert().unwrap();
        let server_config = create_server_config(&server_cert).unwrap();
        let acceptor = TlsAcceptor::from(server_config);
        // create client config with correct fingerprint
        let client_config = create_client_config(server_cert.fingerprint);
        let connector = TlsConnector::from(client_config);
        // bind server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // spawn server task
        let server_acceptor = acceptor.clone();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls_stream = server_acceptor.accept(stream).await.unwrap();
            tls_stream.write_all(b"hello").await.unwrap();
            tls_stream.shutdown().await.unwrap();
        });
        // client connects
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("rcp").unwrap();
        let mut tls_stream = connector.connect(server_name, stream).await.unwrap();
        let mut buf = [0u8; 5];
        tls_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        server_task.await.unwrap();
    }

    /// Test TLS handshake fails when client has wrong server fingerprint.
    #[tokio::test]
    async fn test_tls_handshake_fails_with_wrong_server_fingerprint() {
        install_crypto_provider();
        // generate server certificate
        let server_cert = generate_self_signed_cert().unwrap();
        let server_config = create_server_config(&server_cert).unwrap();
        let acceptor = TlsAcceptor::from(server_config);
        // create client config with WRONG fingerprint
        let wrong_fingerprint = [0xAB; 32];
        let client_config = create_client_config(wrong_fingerprint);
        let connector = TlsConnector::from(client_config);
        // bind server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // spawn server task (will fail when client rejects cert)
        let server_acceptor = acceptor.clone();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            // server accept may fail when client aborts handshake
            let _ = server_acceptor.accept(stream).await;
        });
        // client connects - should fail due to fingerprint mismatch
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("rcp").unwrap();
        let result = connector.connect(server_name, stream).await;
        assert!(result.is_err(), "expected TLS handshake to fail");
        let err = result.unwrap_err();
        // the error should indicate certificate validation failed
        assert!(
            err.to_string().contains("certificate")
                || err.to_string().contains("Certificate")
                || err.to_string().contains("invalid"),
            "expected certificate error, got: {}",
            err
        );
        server_task.await.unwrap();
    }

    /// Test mutual TLS handshake fails when server has wrong client fingerprint.
    #[tokio::test]
    async fn test_mutual_tls_fails_with_wrong_client_fingerprint() {
        install_crypto_provider();
        // generate server and client certificates
        let server_cert = generate_self_signed_cert().unwrap();
        let client_cert = generate_self_signed_cert().unwrap();
        // server expects WRONG client fingerprint
        let wrong_fingerprint = [0xCD; 32];
        let server_config =
            create_server_config_with_client_auth(&server_cert, wrong_fingerprint).unwrap();
        let acceptor = TlsAcceptor::from(server_config);
        // client has correct server fingerprint
        let client_config =
            create_client_config_with_cert(&client_cert, server_cert.fingerprint).unwrap();
        let connector = TlsConnector::from(client_config);
        // bind server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // spawn server task - will fail when verifying client cert
        let server_acceptor = acceptor.clone();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let result = server_acceptor.accept(stream).await;
            assert!(result.is_err(), "expected server to reject client cert");
        });
        // client connects
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("rcp").unwrap();
        // in TLS 1.3, client cert verification happens after client considers handshake done.
        // the failure shows up as either: connect() error, or subsequent read/write error.
        match connector.connect(server_name, stream).await {
            Ok(mut tls_stream) => {
                // handshake appeared to succeed from client's view, but server will reject.
                // try to read - server's rejection will cause connection to fail.
                let mut buf = [0u8; 1];
                let read_result = tls_stream.read(&mut buf).await;
                assert!(
                    read_result.is_err() || read_result.unwrap() == 0,
                    "expected read to fail or return EOF after server rejection"
                );
            }
            Err(_) => {
                // handshake failed directly - also acceptable
            }
        }
        server_task.await.unwrap();
    }

    /// Test mutual TLS handshake succeeds with correct fingerprints.
    #[tokio::test]
    async fn test_mutual_tls_success_with_matching_fingerprints() {
        install_crypto_provider();
        // generate server and client certificates
        let server_cert = generate_self_signed_cert().unwrap();
        let client_cert = generate_self_signed_cert().unwrap();
        // server expects correct client fingerprint
        let server_config =
            create_server_config_with_client_auth(&server_cert, client_cert.fingerprint).unwrap();
        let acceptor = TlsAcceptor::from(server_config);
        // client has correct server fingerprint
        let client_config =
            create_client_config_with_cert(&client_cert, server_cert.fingerprint).unwrap();
        let connector = TlsConnector::from(client_config);
        // bind server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // spawn server task
        let server_acceptor = acceptor.clone();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls_stream = server_acceptor.accept(stream).await.unwrap();
            tls_stream.write_all(b"mutual").await.unwrap();
            tls_stream.shutdown().await.unwrap();
        });
        // client connects
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("rcp").unwrap();
        let mut tls_stream = connector.connect(server_name, stream).await.unwrap();
        let mut buf = [0u8; 6];
        tls_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"mutual");
        server_task.await.unwrap();
    }

    /// A peer that replays the pinned certificate's bytes but does NOT hold its private key must
    /// fail the handshake. The certificate presented matches the pinned fingerprint, but the
    /// `CertificateVerify` signature is produced with a different key, so proof-of-possession must
    /// reject it. Guards against regressing the signature verifiers back to unconditional
    /// acceptance (which would reduce authentication to "presented a known certificate").
    #[tokio::test]
    async fn test_tls_handshake_fails_when_cert_replayed_without_private_key() {
        install_crypto_provider();
        // the legitimate server certificate the client pins
        let legit_cert = generate_self_signed_cert().unwrap();
        // a separate certificate whose private key the attacker actually controls
        let attacker_cert = generate_self_signed_cert().unwrap();
        // present the legit certificate but sign with the attacker's key. building the
        // `CertifiedKey` directly (rather than via `with_single_cert`) bypasses the cert/key
        // consistency check, mimicking a hand-crafted malicious peer.
        let attacker_signing_key = rustls::crypto::ring::sign::any_supported_type(
            &PrivateKeyDer::try_from(attacker_cert.key_der.clone()).unwrap(),
        )
        .unwrap();
        let replayed = std::sync::Arc::new(rustls::sign::CertifiedKey::new(
            vec![CertificateDer::from(legit_cert.cert_der.clone())],
            attacker_signing_key,
        ));
        #[derive(Debug)]
        struct StaticResolver(std::sync::Arc<rustls::sign::CertifiedKey>);
        impl rustls::server::ResolvesServerCert for StaticResolver {
            fn resolve(
                &self,
                _client_hello: rustls::server::ClientHello<'_>,
            ) -> Option<std::sync::Arc<rustls::sign::CertifiedKey>> {
                Some(self.0.clone())
            }
        }
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(std::sync::Arc::new(StaticResolver(replayed)));
        let acceptor = TlsAcceptor::from(std::sync::Arc::new(server_config));
        // client pins the legit certificate's fingerprint, which the presented cert matches
        let client_config = create_client_config(legit_cert.fingerprint);
        let connector = TlsConnector::from(client_config);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_acceptor = acceptor.clone();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            // handshake aborts once the client rejects the CertificateVerify signature
            let _ = server_acceptor.accept(stream).await;
        });
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("rcp").unwrap();
        let result = connector.connect(server_name, stream).await;
        assert!(
            result.is_err(),
            "handshake must fail: cert fingerprint matched but the peer did not prove possession \
             of the private key"
        );
        server_task.await.unwrap();
    }

    /// The mutual-TLS mirror of the previous test, for the client-certificate path: a client that
    /// replays the pinned client certificate's bytes but signs with a different key must be
    /// rejected by the server's client-cert verifier. Guards the source<->destination and
    /// master<->rcpd client-auth direction.
    #[tokio::test]
    async fn test_mutual_tls_fails_when_client_cert_replayed_without_private_key() {
        install_crypto_provider();
        let server_cert = generate_self_signed_cert().unwrap();
        // the legitimate client certificate the server pins
        let legit_client_cert = generate_self_signed_cert().unwrap();
        // a separate certificate whose private key the attacker actually controls
        let attacker_cert = generate_self_signed_cert().unwrap();
        // server requires a client certificate matching the legit client's fingerprint
        let server_config =
            create_server_config_with_client_auth(&server_cert, legit_client_cert.fingerprint)
                .unwrap();
        let acceptor = TlsAcceptor::from(server_config);
        // client presents the legit client certificate but signs with the attacker's key
        let attacker_signing_key = rustls::crypto::ring::sign::any_supported_type(
            &PrivateKeyDer::try_from(attacker_cert.key_der.clone()).unwrap(),
        )
        .unwrap();
        let replayed = std::sync::Arc::new(rustls::sign::CertifiedKey::new(
            vec![CertificateDer::from(legit_client_cert.cert_der.clone())],
            attacker_signing_key,
        ));
        #[derive(Debug)]
        struct ReplayResolver(std::sync::Arc<rustls::sign::CertifiedKey>);
        impl rustls::client::ResolvesClientCert for ReplayResolver {
            fn resolve(
                &self,
                _root_hint_subjects: &[&[u8]],
                _sigschemes: &[SignatureScheme],
            ) -> Option<std::sync::Arc<rustls::sign::CertifiedKey>> {
                Some(self.0.clone())
            }
            fn has_certs(&self) -> bool {
                true
            }
        }
        // client pins the real server certificate (so the server side is authenticated normally)
        let server_verifier =
            std::sync::Arc::new(FingerprintServerCertVerifier::new(server_cert.fingerprint));
        let client_config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(server_verifier)
            .with_client_cert_resolver(std::sync::Arc::new(ReplayResolver(replayed)));
        let connector = TlsConnector::from(std::sync::Arc::new(client_config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_acceptor = acceptor.clone();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let result = server_acceptor.accept(stream).await;
            assert!(
                result.is_err(),
                "server must reject a client that replayed the pinned certificate without its key"
            );
        });
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("rcp").unwrap();
        // in TLS 1.3 the server verifies the client cert after the client's flight, so the
        // rejection surfaces as either a connect() error or a subsequent read error. if connect()
        // fails outright that already proves rejection; otherwise the first read must fail/EOF
        if let Ok(mut tls_stream) = connector.connect(server_name, stream).await {
            let mut buf = [0u8; 1];
            let read_result = tls_stream.read(&mut buf).await;
            assert!(
                read_result.is_err() || read_result.unwrap() == 0,
                "expected read to fail or EOF after the server rejects the replayed client cert"
            );
        }
        server_task.await.unwrap();
    }

    /// TLS 1.3 is pinned: a peer that offers only TLS 1.2 cannot complete a handshake. Guards the
    /// version pin so a downgrade to 1.2 (whose separate signature callbacks would otherwise
    /// apply) cannot be silently reintroduced.
    #[tokio::test]
    async fn test_tls_handshake_fails_when_peer_offers_only_tls12() {
        install_crypto_provider();
        let server_cert = generate_self_signed_cert().unwrap();
        // production server config, pinned to TLS 1.3
        let server_config = create_server_config(&server_cert).unwrap();
        let acceptor = TlsAcceptor::from(server_config);
        // client that offers ONLY TLS 1.2
        let verifier =
            std::sync::Arc::new(FingerprintServerCertVerifier::new(server_cert.fingerprint));
        let client_config =
            ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
                .dangerous()
                .with_custom_certificate_verifier(verifier)
                .with_no_client_auth();
        let connector = TlsConnector::from(std::sync::Arc::new(client_config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_acceptor = acceptor.clone();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            // handshake fails: no protocol version in common
            let _ = server_acceptor.accept(stream).await;
        });
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("rcp").unwrap();
        let result = connector.connect(server_name, stream).await;
        assert!(
            result.is_err(),
            "a server pinned to TLS 1.3 must reject a client that offers only TLS 1.2"
        );
        server_task.await.unwrap();
    }

    #[test]
    fn hmac_sha256_matches_rfc4231_test_vectors() {
        // RFC 4231 test case 1: 20-byte key zero-padded to 32 bytes (identical to 64-byte HMAC pad)
        let mut key1 = [0u8; 32];
        key1[..20].fill(0x0b);
        assert_eq!(
            hex::encode(hmac_sha256(&key1, &[b"Hi There"])),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // RFC 4231 test case 2: "Jefe"
        let mut key2 = [0u8; 32];
        key2[..4].copy_from_slice(b"Jefe");
        assert_eq!(
            hex::encode(hmac_sha256(&key2, &[b"what do ya want ", b"for nothing?"])),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // RFC 4231 test case 4: 25-byte key
        let mut key4 = [0u8; 32];
        for (idx, slot) in key4[..25].iter_mut().enumerate() {
            *slot = (idx + 1) as u8;
        }
        let data4 = [0xcd_u8; 50];
        assert_eq!(
            hex::encode(hmac_sha256(&key4, &[&data4])),
            "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b"
        );
        // Full 32-byte non-zero key so every key byte is exercised
        let key_full: [u8; 32] = std::array::from_fn(|i| (i + 1) as u8);
        assert_eq!(
            hex::encode(hmac_sha256(&key_full, &[b"rcp-32-byte-key-test"])),
            "406badabb4f9ff36cdb969aa2cd5e7d2c6f7bb333e2eabf82d3131863e22377f"
        );
    }

    #[test]
    fn loopback_peer_check_accepts_ipv4_ipv6_and_mapped_loopback_only() {
        for addr in ["127.0.0.1", "127.0.0.42", "::1", "::ffff:127.0.0.1"] {
            let ip: std::net::IpAddr = addr.parse().unwrap();
            assert!(is_loopback_peer(ip), "expected loopback for {addr}");
            assert!(ensure_loopback_peer(ip, "control").is_ok());
        }
        for addr in [
            "0.0.0.0",
            "::",
            "::127.0.0.1",
            "10.0.0.1",
            "192.0.2.5",
            "2001:db8::1",
            "::ffff:10.0.0.1",
        ] {
            let ip: std::net::IpAddr = addr.parse().unwrap();
            assert!(!is_loopback_peer(ip), "expected non-loopback for {addr}");
            assert!(ensure_loopback_peer(ip, "control").is_err());
        }
    }

    #[test]
    fn peer_auth_constructors_enforce_certificate_alignment_and_auth_flag() {
        install_crypto_provider();
        let cert = generate_self_signed_cert().unwrap();
        let peer_fp = [0x42; 32];
        let secret = crate::protocol::SshAuthSecret::from_bytes([0x99; 32]);
        let tls_auth = crate::protocol::PeerAuth::Tls {
            peer_fingerprint: peer_fp,
        };
        let ssh_auth = crate::protocol::PeerAuth::SshSharedSecret {
            secret: secret.clone(),
        };
        let auto_auth = crate::protocol::PeerAuth::AutoTlsOrSshSecret {
            peer_fingerprint: peer_fp,
            secret,
        };
        let none_auth = crate::protocol::PeerAuth::None;
        assert!(ControlStreamAcceptor::from_peer_auth(Some(&cert), &tls_auth).is_ok());
        assert!(ControlStreamAcceptor::from_peer_auth(None, &tls_auth).is_err());
        assert!(ControlStreamAcceptor::from_peer_auth(None, &ssh_auth).is_ok());
        assert!(ControlStreamAcceptor::from_peer_auth(Some(&cert), &ssh_auth).is_err());
        assert!(ControlStreamAcceptor::from_peer_auth(Some(&cert), &auto_auth).is_ok());
        assert!(ControlStreamAcceptor::from_peer_auth(None, &auto_auth).is_err());
        assert!(ControlStreamAcceptor::from_peer_auth(None, &none_auth).is_ok());
        assert!(ControlStreamAcceptor::from_peer_auth(Some(&cert), &none_auth).is_err());
        let c_tls = StreamConnector::from_peer_auth(Some(&cert), &tls_auth, false).unwrap();
        assert!(c_tls.authenticates_peer());
        let c_ssh = StreamConnector::from_peer_auth(None, &ssh_auth, true).unwrap();
        assert!(c_ssh.authenticates_peer());
        let c_auto_direct =
            StreamConnector::from_peer_auth(Some(&cert), &auto_auth, false).unwrap();
        assert!(matches!(c_auto_direct, StreamConnector::Tls(_)));
        assert!(c_auto_direct.authenticates_peer());
        let c_auto_ssh = StreamConnector::from_peer_auth(Some(&cert), &auto_auth, true).unwrap();
        assert!(matches!(c_auto_ssh, StreamConnector::SshAuth(_)));
        assert!(c_auto_ssh.authenticates_peer());
        let c_plain = StreamConnector::from_peer_auth(None, &none_auth, true).unwrap();
        assert!(!c_plain.authenticates_peer());
    }

    #[tokio::test]
    async fn ssh_auth_round_trip_and_framed_io_succeed() {
        let secret = crate::protocol::SshAuthSecret::generate();
        let acceptor = ControlStreamAcceptor::SshAuth(secret.clone());
        let connector = StreamConnector::SshAuth(secret);
        let timeout = std::time::Duration::from_secs(5);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut send, mut recv, data_acceptor) =
                accept_control_bounded(&acceptor, stream, timeout, "control")
                    .await
                    .unwrap();
            assert!(matches!(data_acceptor, StreamAcceptor::SshAuth(_)));
            let msg: String = recv.recv_object().await.unwrap().unwrap();
            assert_eq!(msg, "hello-from-dest");
            send.send_control_message(&"hello-from-source".to_string())
                .await
                .unwrap();
            send.close().await.unwrap();
        });
        let stream = TcpStream::connect(addr).await.unwrap();
        let (mut send, mut recv) = connect_bounded(
            &connector,
            SERVER_NAME_SOURCE,
            stream,
            timeout,
            StreamKind::Control,
            "control",
        )
        .await
        .unwrap();
        send.send_control_message(&"hello-from-dest".to_string())
            .await
            .unwrap();
        let reply: String = recv.recv_object().await.unwrap().unwrap();
        assert_eq!(reply, "hello-from-source");
        send.close().await.unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn ssh_auth_rejects_wrong_secret_reflection_cross_kind_and_stall() {
        let secret_a = crate::protocol::SshAuthSecret::from_bytes([0x11; 32]);
        let secret_b = crate::protocol::SshAuthSecret::from_bytes([0x22; 32]);
        let timeout = std::time::Duration::from_secs(2);
        // 1. Wrong secret rejected by client (and server sees disconnect)
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server_acceptor = StreamAcceptor::SshAuth(secret_a.clone());
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                assert!(
                    accept_bounded(&server_acceptor, stream, timeout, StreamKind::Data, "data")
                        .await
                        .is_err()
                );
            });
            let stream = TcpStream::connect(addr).await.unwrap();
            let client_connector = StreamConnector::SshAuth(secret_b);
            assert!(
                connect_bounded(
                    &client_connector,
                    SERVER_NAME_SOURCE,
                    stream,
                    timeout,
                    StreamKind::Data,
                    "data",
                )
                .await
                .is_err()
            );
            server.await.unwrap();
        }
        // 2. Reflection attack (echoing server_tag back as client_tag) rejected by server
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server_acceptor = StreamAcceptor::SshAuth(secret_a.clone());
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let Err(err) = accept_bounded(
                    &server_acceptor,
                    stream,
                    timeout,
                    StreamKind::Control,
                    "control",
                )
                .await
                else {
                    panic!("reflection must fail");
                };
                assert!(
                    format!("{err:#}").contains("tag mismatch"),
                    "unexpected error: {err:#}"
                );
            });
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let mut hello = [0u8; 36];
            hello[..4].copy_from_slice(&SSH_AUTH_MAGIC);
            stream.write_all(&hello).await.unwrap();
            let mut server_hello = [0u8; 64];
            stream.read_exact(&mut server_hello).await.unwrap();
            // reflect server's tag right back as client_tag
            stream.write_all(&server_hello[32..64]).await.unwrap();
            server.await.unwrap();
        }
        // 3. Cross-kind mismatch (client Control vs server Data) rejected
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server_acceptor = StreamAcceptor::SshAuth(secret_a.clone());
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                assert!(
                    accept_bounded(&server_acceptor, stream, timeout, StreamKind::Data, "data")
                        .await
                        .is_err()
                );
            });
            let stream = TcpStream::connect(addr).await.unwrap();
            let client_connector = StreamConnector::SshAuth(secret_a.clone());
            assert!(
                connect_bounded(
                    &client_connector,
                    SERVER_NAME_SOURCE,
                    stream,
                    timeout,
                    StreamKind::Control,
                    "control",
                )
                .await
                .is_err()
            );
            server.await.unwrap();
        }
        // 4. Stalled peer times out cleanly
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server_acceptor = ControlStreamAcceptor::SshAuth(secret_a);
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let Err(err) = accept_control_bounded(
                    &server_acceptor,
                    stream,
                    std::time::Duration::from_millis(100),
                    "control",
                )
                .await
                else {
                    panic!("stalled peer must time out");
                };
                assert!(
                    format!("{err:#}").contains("timed out"),
                    "expected timeout error: {err:#}"
                );
            });
            let _silent_client = TcpStream::connect(addr).await.unwrap();
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn auto_control_acceptor_dispatches_and_locks_resolved_data_mode() {
        install_crypto_provider();
        let server_cert = generate_self_signed_cert().unwrap();
        let client_cert = generate_self_signed_cert().unwrap();
        let secret = crate::protocol::SshAuthSecret::generate();
        let server_auth = crate::protocol::PeerAuth::AutoTlsOrSshSecret {
            peer_fingerprint: client_cert.fingerprint,
            secret: secret.clone(),
        };
        let client_auth = crate::protocol::PeerAuth::AutoTlsOrSshSecret {
            peer_fingerprint: server_cert.fingerprint,
            secret,
        };
        let auto_acceptor =
            ControlStreamAcceptor::from_peer_auth(Some(&server_cert), &server_auth).unwrap();
        let tls_connector =
            StreamConnector::from_peer_auth(Some(&client_cert), &client_auth, false).unwrap();
        let ssh_connector =
            StreamConnector::from_peer_auth(Some(&client_cert), &client_auth, true).unwrap();
        let timeout = std::time::Duration::from_secs(5);
        // Path 1: direct TCP connects with TLS -> control resolves to StreamAcceptor::Tls, which
        // accepts TLS data and rejects SshAuth data
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let acceptor = auto_acceptor.clone();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (_send, _recv, data_acceptor) =
                    accept_control_bounded(&acceptor, stream, timeout, "control")
                        .await
                        .unwrap();
                assert!(matches!(data_acceptor, StreamAcceptor::Tls(_)));
                let (bad_data, _) = listener.accept().await.unwrap();
                assert!(
                    accept_bounded(&data_acceptor, bad_data, timeout, StreamKind::Data, "data")
                        .await
                        .is_err(),
                    "locked TLS data acceptor must reject SshAuth data connection"
                );
            });
            let ctrl = TcpStream::connect(addr).await.unwrap();
            let _ = connect_bounded(
                &tls_connector,
                SERVER_NAME_SOURCE,
                ctrl,
                timeout,
                StreamKind::Control,
                "control",
            )
            .await
            .unwrap();
            let bad_data = TcpStream::connect(addr).await.unwrap();
            assert!(
                connect_bounded(
                    &ssh_connector,
                    SERVER_NAME_SOURCE,
                    bad_data,
                    timeout,
                    StreamKind::Data,
                    "data",
                )
                .await
                .is_err()
            );
            server.await.unwrap();
        }
        // Path 2: SSH fallback connects with SshAuth on loopback -> control resolves to
        // StreamAcceptor::SshAuth, which accepts SshAuth data and rejects TLS data
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let acceptor = auto_acceptor.clone();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (_send, _recv, data_acceptor) =
                    accept_control_bounded(&acceptor, stream, timeout, "control")
                        .await
                        .unwrap();
                assert!(matches!(data_acceptor, StreamAcceptor::SshAuth(_)));
                let (bad_data, _) = listener.accept().await.unwrap();
                assert!(
                    accept_bounded(&data_acceptor, bad_data, timeout, StreamKind::Data, "data")
                        .await
                        .is_err(),
                    "locked SshAuth data acceptor must reject TLS data connection"
                );
            });
            let ctrl = TcpStream::connect(addr).await.unwrap();
            let _ = connect_bounded(
                &ssh_connector,
                SERVER_NAME_SOURCE,
                ctrl,
                timeout,
                StreamKind::Control,
                "control",
            )
            .await
            .unwrap();
            let bad_data = TcpStream::connect(addr).await.unwrap();
            assert!(
                connect_bounded(
                    &tls_connector,
                    SERVER_NAME_SOURCE,
                    bad_data,
                    timeout,
                    StreamKind::Data,
                    "data",
                )
                .await
                .is_err()
            );
            server.await.unwrap();
        }
        // Path 3: Auto peek handles silent timeout, immediate EOF, and unexpected prefix byte
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let acceptor = auto_acceptor.clone();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let Err(err) = accept_control_bounded(
                    &acceptor,
                    stream,
                    std::time::Duration::from_millis(100),
                    "control",
                )
                .await
                else {
                    panic!("silent Auto peer must time out during peek");
                };
                assert!(format!("{err:#}").contains("timed out"), "{err:#}");
            });
            let _silent = TcpStream::connect(addr).await.unwrap();
            server.await.unwrap();
        }
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let acceptor = auto_acceptor.clone();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let Err(err) = accept_control_bounded(&acceptor, stream, timeout, "control").await
                else {
                    panic!("EOF before first byte must fail");
                };
                assert!(
                    format!("{err:#}").contains("before starting handshake"),
                    "{err:#}"
                );
            });
            let client = TcpStream::connect(addr).await.unwrap();
            drop(client);
            server.await.unwrap();
        }
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let acceptor = auto_acceptor;
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let Err(err) = accept_control_bounded(&acceptor, stream, timeout, "control").await
                else {
                    panic!("unexpected first byte must fail");
                };
                assert!(format!("{err:#}").contains("0x00"), "{err:#}");
            });
            let mut client = TcpStream::connect(addr).await.unwrap();
            client.write_all(&[0x00]).await.unwrap();
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn prefixed_stream_preserves_prefix_on_zero_capacity_read_and_forwards_vectored_writes() {
        use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
        struct VectoredMock {
            read_data: std::io::Cursor<Vec<u8>>,
            vectored_calls: usize,
            written: Vec<u8>,
        }
        impl tokio::io::AsyncRead for VectoredMock {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::pin::Pin::new(&mut self.read_data).poll_read(cx, buf)
            }
        }
        impl tokio::io::AsyncWrite for VectoredMock {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.written.extend_from_slice(buf);
                std::task::Poll::Ready(Ok(buf.len()))
            }

            fn poll_write_vectored(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                bufs: &[std::io::IoSlice<'_>],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.vectored_calls += 1;
                let mut total = 0;
                for slice in bufs {
                    self.written.extend_from_slice(slice);
                    total += slice.len();
                }
                std::task::Poll::Ready(Ok(total))
            }

            fn is_write_vectored(&self) -> bool {
                true
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }

            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }
        let mut stream = PrefixedStream::new(
            0x16,
            VectoredMock {
                read_data: std::io::Cursor::new(vec![0x03, 0x01]),
                vectored_calls: 0,
                written: Vec::new(),
            },
        );
        // zero-capacity read must not drop the prefix byte
        let mut empty = [];
        assert_eq!(stream.read(&mut empty).await.unwrap(), 0);
        let mut out = [0u8; 3];
        stream.read_exact(&mut out).await.unwrap();
        assert_eq!(out, [0x16, 0x03, 0x01]);
        // vectored writes must pass through so tokio-rustls keeps writev batching
        assert!(stream.is_write_vectored());
        let bufs = [
            std::io::IoSlice::new(b"tls-"),
            std::io::IoSlice::new(b"batch"),
        ];
        assert_eq!(stream.write_vectored(&bufs).await.unwrap(), 9);
        assert_eq!(stream.inner.vectored_calls, 1);
        assert_eq!(stream.inner.written, b"tls-batch");
    }
}
