//! TLS support for encrypted and authenticated connections.
//!
//! This module provides certificate generation and TLS configuration for:
//! - Master↔rcpd connections (rcpd is server, master verifies fingerprint)
//! - Source↔Destination connections (mutual TLS with client certificates)
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// A certificate fingerprint (SHA-256 of DER-encoded certificate).
pub type Fingerprint = [u8; 32];

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

/// Creates a TLS server config for rcpd (no client authentication required).
///
/// Used for master→rcpd connections where master verifies rcpd's certificate.
pub fn create_server_config(cert_key: &CertifiedKey) -> anyhow::Result<Arc<ServerConfig>> {
    let cert = CertificateDer::from(cert_key.cert_der.clone());
    let key = PrivateKeyDer::try_from(cert_key.key_der.clone())
        .map_err(|e| anyhow::anyhow!("invalid private key: {e}"))?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    Ok(Arc::new(config))
}

/// Creates a TLS server config with client certificate verification.
///
/// Used for source→destination connections where source verifies destination's client cert.
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
    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(vec![cert], key)?;
    Ok(Arc::new(config))
}

/// Creates a TLS client config that verifies the server's certificate fingerprint.
///
/// Used for master→rcpd connections where master has no client certificate.
pub fn create_client_config(expected_server_fingerprint: Fingerprint) -> Arc<ClientConfig> {
    let verifier = Arc::new(FingerprintServerCertVerifier::new(
        expected_server_fingerprint,
    ));
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Arc::new(config)
}

/// Creates a TLS client config with a client certificate.
///
/// Used for destination→source connections where destination presents its certificate.
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
    let config = ClientConfig::builder()
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
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // we trust the certificate based on fingerprint, not signature chain
        Ok(HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // we trust the certificate based on fingerprint, not signature chain
        Ok(HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ED25519,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
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
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ED25519,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
    fn client_auth_mandatory(&self) -> bool {
        true
    }
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
}
