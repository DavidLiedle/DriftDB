//! TLS/SSL support for secure connections

#![allow(dead_code)]

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tokio_rustls::{rustls, TlsAcceptor};
use tracing::{debug, error, info, warn};

use crate::errors::internal_error;

/// TLS configuration for the server
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to certificate file (PEM format)
    pub cert_path: PathBuf,
    /// Path to private key file (PEM format)
    pub key_path: PathBuf,
    /// Require TLS for all connections
    pub require_tls: bool,
    /// TLS protocols to support
    pub protocols: Vec<String>,
    /// Cipher suites to support
    pub cipher_suites: Option<Vec<String>>,
}

impl TlsConfig {
    pub fn new<P: AsRef<Path>>(cert_path: P, key_path: P) -> Self {
        Self {
            cert_path: cert_path.as_ref().to_path_buf(),
            key_path: key_path.as_ref().to_path_buf(),
            require_tls: false,
            protocols: vec!["TLSv1.2".to_string(), "TLSv1.3".to_string()],
            cipher_suites: None,
        }
    }

    pub fn require_tls(mut self, require: bool) -> Self {
        self.require_tls = require;
        self
    }

    pub fn protocols(mut self, protocols: Vec<String>) -> Self {
        self.protocols = protocols;
        self
    }
}

/// Stream wrapper that can handle both plain TCP and TLS connections
#[allow(clippy::large_enum_variant)]
pub enum SecureStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl SecureStream {
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            SecureStream::Plain(stream) => stream.peer_addr(),
            SecureStream::Tls(stream) => stream.get_ref().0.peer_addr(),
        }
    }

    pub fn is_tls(&self) -> bool {
        matches!(self, SecureStream::Tls(_))
    }
}

impl AsyncRead for SecureStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            SecureStream::Plain(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            SecureStream::Tls(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for SecureStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        match &mut *self {
            SecureStream::Plain(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            SecureStream::Tls(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        match &mut *self {
            SecureStream::Plain(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            SecureStream::Tls(stream) => std::pin::Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        match &mut *self {
            SecureStream::Plain(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            SecureStream::Tls(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// TLS acceptor that manages SSL handshakes
pub struct TlsManager {
    acceptor: Option<TlsAcceptor>,
    config: TlsConfig,
}

impl TlsManager {
    /// Create a new TLS manager with the given configuration
    pub async fn new(config: TlsConfig) -> Result<Self> {
        let acceptor = if config.cert_path.exists() && config.key_path.exists() {
            Some(Self::create_acceptor(&config).await?)
        } else {
            if config.require_tls {
                let error = internal_error(
                    "TLS required but certificate/key files not found",
                    Some(&format!(
                        "cert: {:?}, key: {:?}",
                        config.cert_path, config.key_path
                    )),
                );
                error.log();
                return Err(anyhow!(
                    "TLS configuration error: certificate files not found"
                ));
            }
            warn!(
                "TLS certificate files not found at {:?} and {:?}, TLS disabled",
                config.cert_path, config.key_path
            );
            None
        };

        Ok(Self { acceptor, config })
    }

    /// Create a rustls TLS acceptor from certificate and key files
    async fn create_acceptor(config: &TlsConfig) -> Result<TlsAcceptor> {
        // Read certificate chain
        let cert_file = tokio::fs::read(&config.cert_path).await.map_err(|e| {
            anyhow!(
                "Failed to read certificate file {:?}: {}",
                config.cert_path,
                e
            )
        })?;

        let mut cert_reader = std::io::Cursor::new(cert_file);
        let cert_chain = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow!("Failed to parse certificate: {}", e))?
            .into_iter()
            .collect();

        // Read private key
        let key_file = tokio::fs::read(&config.key_path).await.map_err(|e| {
            anyhow!(
                "Failed to read private key file {:?}: {}",
                config.key_path,
                e
            )
        })?;

        let mut key_reader = std::io::Cursor::new(key_file);
        let private_key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|e| anyhow!("Failed to parse private key: {}", e))?
            .ok_or_else(|| anyhow!("No private key found in key file"))?;

        // Create TLS configuration
        let tls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| anyhow!("Failed to create TLS config: {}", e))?;

        info!(
            "TLS configured with certificate: {:?}, key: {:?}",
            config.cert_path, config.key_path
        );

        Ok(TlsAcceptor::from(Arc::new(tls_config)))
    }

    /// Accept a connection and potentially upgrade to TLS
    pub async fn accept_connection(&self, tcp_stream: TcpStream) -> Result<SecureStream> {
        let peer_addr = tcp_stream.peer_addr()?;

        match &self.acceptor {
            Some(acceptor) => {
                // Check if client requests TLS by reading the first byte
                let mut tcp_stream = tcp_stream;
                let mut buffer = [0u8; 1];

                // Peek at the first byte to see if it's an SSL request
                match tcp_stream.peek(&mut buffer).await {
                    Ok(0) => {
                        debug!("Connection from {} closed during TLS detection", peer_addr);
                        return Err(anyhow!("Connection closed"));
                    }
                    Ok(_) => {
                        // PostgreSQL SSL request starts with length (8 bytes) then SSL code
                        let mut ssl_request = [0u8; 8];
                        tcp_stream.peek(&mut ssl_request).await?;

                        // Check for PostgreSQL SSL request:
                        // [4 bytes length = 8][4 bytes SSL code = 80877103]
                        let length = u32::from_be_bytes([
                            ssl_request[0],
                            ssl_request[1],
                            ssl_request[2],
                            ssl_request[3],
                        ]);
                        let ssl_code = u32::from_be_bytes([
                            ssl_request[4],
                            ssl_request[5],
                            ssl_request[6],
                            ssl_request[7],
                        ]);

                        if length == 8 && ssl_code == 80877103 {
                            // Client requests SSL - consume the SSL request
                            let mut discard = [0u8; 8];
                            use tokio::io::AsyncReadExt;
                            tcp_stream.read_exact(&mut discard).await?;

                            // Send SSL supported response
                            use tokio::io::AsyncWriteExt;
                            tcp_stream.write_all(b"S").await?;

                            debug!("Upgrading connection from {} to TLS", peer_addr);

                            // Perform TLS handshake
                            match acceptor.accept(tcp_stream).await {
                                Ok(tls_stream) => {
                                    info!("TLS connection established with {}", peer_addr);
                                    return Ok(SecureStream::Tls(tls_stream));
                                }
                                Err(e) => {
                                    warn!("TLS handshake failed with {}: {}", peer_addr, e);
                                    return Err(anyhow!("TLS handshake failed: {}", e));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Error peeking connection from {}: {}", peer_addr, e);
                        return Err(anyhow!("Connection error: {}", e));
                    }
                }

                // Client didn't request SSL
                if self.config.require_tls {
                    warn!(
                        "Rejecting non-TLS connection from {} (TLS required)",
                        peer_addr
                    );
                    // Send SSL not supported response
                    use tokio::io::AsyncWriteExt;
                    let mut tcp_stream = tcp_stream;
                    let _ = tcp_stream.write_all(b"N").await;
                    return Err(anyhow!("TLS required but client did not request SSL"));
                }

                debug!(
                    "Accepting plain connection from {} (TLS available but not requested)",
                    peer_addr
                );
                // Send SSL not supported response for plain connections
                use tokio::io::AsyncWriteExt;
                let mut tcp_stream = tcp_stream;
                let _ = tcp_stream.write_all(b"N").await;
                Ok(SecureStream::Plain(tcp_stream))
            }
            None => {
                // TLS not configured
                if self.config.require_tls {
                    error!("TLS required but not configured");
                    return Err(anyhow!("TLS required but not configured"));
                }

                debug!(
                    "Accepting plain connection from {} (TLS not configured)",
                    peer_addr
                );
                // Still need to handle potential SSL requests even without TLS
                use tokio::io::AsyncWriteExt;
                let mut tcp_stream = tcp_stream;
                let _ = tcp_stream.write_all(b"N").await; // SSL not supported
                Ok(SecureStream::Plain(tcp_stream))
            }
        }
    }

    /// Check if TLS is available
    pub fn is_tls_available(&self) -> bool {
        self.acceptor.is_some()
    }

    /// Check if TLS is required
    pub fn is_tls_required(&self) -> bool {
        self.config.require_tls
    }

    /// Get the TLS configuration
    pub fn config(&self) -> &TlsConfig {
        &self.config
    }
}

/// Generate a self-signed certificate for development/testing
///
/// This generates a self-signed certificate valid for 365 days with the following attributes:
/// - Common Name: localhost
/// - Subject Alternative Names: localhost, 127.0.0.1, ::1
/// - Key Usage: Digital Signature, Key Encipherment
/// - Extended Key Usage: Server Authentication
///
/// WARNING: Self-signed certificates should only be used for development and testing.
/// For production use, obtain certificates from a trusted Certificate Authority.
pub fn generate_self_signed_cert(cert_path: &Path, key_path: &Path) -> Result<()> {
    use rcgen::{CertificateParams, DnType, KeyPair, SanType, PKCS_ECDSA_P256_SHA256};
    use std::fs;

    info!("Generating self-signed certificate for development/testing");

    // Create certificate parameters
    let mut params = CertificateParams::default();

    // Set distinguished name
    params
        .distinguished_name
        .push(DnType::CommonName, "DriftDB Development Certificate");
    params
        .distinguished_name
        .push(DnType::OrganizationName, "DriftDB");
    params.distinguished_name.push(DnType::CountryName, "US");

    // Set validity period (365 days)
    params.not_before = time::OffsetDateTime::now_utc();
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(365);

    // Add subject alternative names
    params.subject_alt_names = vec![
        SanType::DnsName("localhost".into()),
        SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))),
        SanType::IpAddress(std::net::IpAddr::V6(std::net::Ipv6Addr::new(
            0, 0, 0, 0, 0, 0, 0, 1,
        ))),
    ];

    // Set key usages
    params.key_usages = vec![
        rcgen::KeyUsagePurpose::DigitalSignature,
        rcgen::KeyUsagePurpose::KeyEncipherment,
    ];

    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];

    // Use ECDSA P-256 for efficiency
    params.alg = &PKCS_ECDSA_P256_SHA256;

    // Set the key pair
    let key_pair = KeyPair::generate(&PKCS_ECDSA_P256_SHA256)?;
    params.key_pair = Some(key_pair);

    // Generate certificate
    let cert = rcgen::Certificate::from_params(params)?;

    // Write certificate to file
    fs::write(cert_path, cert.serialize_pem()?)
        .map_err(|e| anyhow!("Failed to write certificate to {:?}: {}", cert_path, e))?;

    // Write private key to file
    fs::write(key_path, cert.serialize_private_key_pem())
        .map_err(|e| anyhow!("Failed to write private key to {:?}: {}", key_path, e))?;

    info!(
        "Self-signed certificate generated successfully:\n  Certificate: {:?}\n  Private Key: {:?}\n  Valid for: 365 days",
        cert_path, key_path
    );

    warn!(
        "WARNING: This is a self-signed certificate for development/testing only.\n\
         For production use, obtain certificates from a trusted Certificate Authority."
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_tls_config_creation() {
        let config = TlsConfig::new("cert.pem", "key.pem")
            .require_tls(true)
            .protocols(vec!["TLSv1.3".to_string()]);

        assert_eq!(config.cert_path, PathBuf::from("cert.pem"));
        assert_eq!(config.key_path, PathBuf::from("key.pem"));
        assert!(config.require_tls);
        assert_eq!(config.protocols, vec!["TLSv1.3"]);
    }

    #[tokio::test]
    async fn test_tls_manager_without_certs() {
        let temp_dir = tempdir().unwrap();
        let cert_path = temp_dir.path().join("nonexistent.pem");
        let key_path = temp_dir.path().join("nonexistent.key");

        let config = TlsConfig::new(&cert_path, &key_path);
        let tls_manager = TlsManager::new(config).await.unwrap();

        assert!(!tls_manager.is_tls_available());
        assert!(!tls_manager.is_tls_required());
    }

    #[test]
    fn test_generate_self_signed_cert() {
        let temp_dir = tempdir().unwrap();
        let cert_path = temp_dir.path().join("test_cert.pem");
        let key_path = temp_dir.path().join("test_key.pem");

        // Generate self-signed certificate
        let result = generate_self_signed_cert(&cert_path, &key_path);
        assert!(
            result.is_ok(),
            "Failed to generate self-signed certificate: {:?}",
            result.err()
        );

        // Verify files were created
        assert!(cert_path.exists(), "Certificate file was not created");
        assert!(key_path.exists(), "Key file was not created");

        // Verify certificate content is not empty
        let cert_content = std::fs::read_to_string(&cert_path).unwrap();
        assert!(
            cert_content.contains("BEGIN CERTIFICATE"),
            "Certificate does not contain BEGIN CERTIFICATE"
        );
        assert!(
            cert_content.contains("END CERTIFICATE"),
            "Certificate does not contain END CERTIFICATE"
        );

        // Verify key content is not empty
        let key_content = std::fs::read_to_string(&key_path).unwrap();
        assert!(
            key_content.contains("BEGIN PRIVATE KEY")
                || key_content.contains("BEGIN RSA PRIVATE KEY"),
            "Key does not contain BEGIN PRIVATE KEY"
        );
        assert!(
            key_content.contains("END PRIVATE KEY") || key_content.contains("END RSA PRIVATE KEY"),
            "Key does not contain END PRIVATE KEY"
        );
    }

    #[tokio::test]
    async fn test_tls_manager_with_generated_cert() {
        let temp_dir = tempdir().unwrap();
        let cert_path = temp_dir.path().join("test_cert.pem");
        let key_path = temp_dir.path().join("test_key.pem");

        // Generate self-signed certificate
        generate_self_signed_cert(&cert_path, &key_path).unwrap();

        // Create TLS manager with generated certificates
        let config = TlsConfig::new(&cert_path, &key_path);
        let tls_manager = TlsManager::new(config).await;

        assert!(
            tls_manager.is_ok(),
            "Failed to create TLS manager: {:?}",
            tls_manager.err()
        );
        let tls_manager = tls_manager.unwrap();

        assert!(
            tls_manager.is_tls_available(),
            "TLS should be available with generated certificates"
        );
        assert!(
            !tls_manager.is_tls_required(),
            "TLS should not be required by default"
        );
    }
}
