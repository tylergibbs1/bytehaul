//! QUIC transport layer for ByteHaul.
//!
//! Provides [`QuicServer`] and [`QuicClient`] for establishing QUIC connections,
//! and [`QuicConnection`] for multiplexing control and data streams over a single
//! QUIC connection. TLS 1.3 is always active (provided by QUIC itself). Ephemeral
//! self-signed certificates are generated via `rcgen` for SSH-bootstrapped receivers.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::{
    ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig, VarInt,
};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use tracing::{debug, info};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised by the transport layer.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// Failed to bind the QUIC UDP socket.
    #[error("failed to bind QUIC endpoint: {0}")]
    Bind(#[source] std::io::Error),

    /// Failed to generate a self-signed TLS certificate.
    #[error("failed to generate self-signed certificate: {0}")]
    CertGeneration(#[source] rcgen::Error),

    /// The supplied private key is invalid.
    #[error("invalid private key: {0}")]
    InvalidKey(#[source] rustls::Error),

    /// TLS configuration failed.
    #[error("TLS configuration error: {0}")]
    TlsConfig(#[source] rustls::Error),

    /// A QUIC connection-level error occurred.
    #[error("connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),

    /// A QUIC connect-level error occurred.
    #[error("connect error: {0}")]
    Connect(#[from] quinn::ConnectError),

    /// No incoming connection was available.
    #[error("no incoming connection")]
    NoIncoming,

    /// Failed to open or accept a QUIC stream.
    #[error("stream open error")]
    StreamOpen,

    /// The idle timeout value is too large for QUIC.
    #[error("idle timeout too large")]
    IdleTimeoutTooLarge,
}

/// Convenience type alias.
pub type Result<T> = std::result::Result<T, TransportError>;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Parameters that govern a QUIC endpoint.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Local address to bind the UDP socket to.
    pub bind_addr: SocketAddr,

    /// Maximum number of concurrent bidirectional streams per connection.
    pub max_concurrent_streams: u32,

    /// If set, QUIC keep-alive pings are sent at this interval.
    pub keep_alive_interval: Option<Duration>,

    /// Connection idle timeout. The connection is closed if no data or
    /// keep-alive is exchanged within this window.
    pub idle_timeout: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            max_concurrent_streams: 16,
            keep_alive_interval: None,
            idle_timeout: Duration::from_secs(30),
        }
    }
}

impl TransportConfig {
    /// Build the Quinn-level transport config from our settings.
    fn to_quinn_transport(&self) -> Result<quinn::TransportConfig> {
        let mut transport = quinn::TransportConfig::default();
        // Allow enough concurrent streams for parallel chunk transfer + headroom.
        // The sender opens one stream per in-flight chunk, so we need at least
        // max_parallel_streams. Add headroom for control streams and overlap.
        let stream_limit = self.max_concurrent_streams.max(64);
        transport.max_concurrent_bidi_streams(VarInt::from_u32(stream_limit));
        transport.max_concurrent_uni_streams(VarInt::from_u32(0));
        if let Some(ka) = self.keep_alive_interval {
            transport.keep_alive_interval(Some(ka));
        }
        transport.max_idle_timeout(Some(
            self.idle_timeout
                .try_into()
                .map_err(|_| TransportError::IdleTimeoutTooLarge)?,
        ));

        // ── Bulk transfer optimizations ──
        //
        // Use BBR congestion control with a large initial window.
        // BBR is designed for high-BDP networks and avoids the slow ramp-up
        // that makes Cubic/NewReno take seconds to reach full throughput.
        let mut bbr = quinn::congestion::BbrConfig::default();
        bbr.initial_window(128 * 1024); // 128 KB initial window (vs default ~14 KB)
        transport.congestion_controller_factory(Arc::new(bbr));

        // Large send/receive windows to sustain throughput on high-RTT links.
        // At 1 Gbps and 200ms RTT, BDP = 25 MB. We set connection-level
        // windows to 32 MB and per-stream windows to 8 MB.
        transport.send_window(32 * 1024 * 1024);                            // 32 MB
        transport.receive_window(VarInt::from_u32(32 * 1024 * 1024));       // 32 MB
        transport.stream_receive_window(VarInt::from_u32(8 * 1024 * 1024)); // 8 MB per stream

        Ok(transport)
    }
}

// ---------------------------------------------------------------------------
// Certificate generation
// ---------------------------------------------------------------------------

/// Generate a self-signed TLS certificate suitable for an ephemeral receiver.
///
/// Returns a `(cert_chain, private_key)` pair that can be fed directly into
/// [`ServerConfig::with_single_cert`].
pub fn generate_self_signed_cert(
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let certified_key =
        generate_simple_self_signed(subject_alt_names).map_err(TransportError::CertGeneration)?;

    let cert_der = CertificateDer::from(certified_key.cert.der().to_vec());
    let key_der =
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(certified_key.key_pair.serialize_der()));

    Ok((vec![cert_der], key_der))
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// A QUIC server endpoint that listens for incoming connections.
pub struct QuicServer {
    endpoint: Endpoint,
    local_addr: SocketAddr,
}

impl QuicServer {
    /// Bind a QUIC server on the address specified in `config`.
    ///
    /// A fresh self-signed certificate is generated for every call, making this
    /// appropriate for the SSH-bootstrapped ephemeral receiver flow.
    pub fn bind(config: TransportConfig) -> Result<Self> {
        let (cert_chain, key) = generate_self_signed_cert()?;

        let mut rustls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(TransportError::TlsConfig)?;

        // Enable 0-RTT data acceptance. The application layer must still gate
        // transfer operations on the full handshake completing.
        rustls_config.max_early_data_size = u32::MAX;

        let quic_server_config =
            quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config)
                .map_err(|e| TransportError::TlsConfig(rustls::Error::General(e.to_string())))?;
        let mut server_config = ServerConfig::with_crypto(Arc::new(quic_server_config));

        server_config.transport_config(Arc::new(config.to_quinn_transport()?));

        let endpoint =
            Endpoint::server(server_config, config.bind_addr).map_err(TransportError::Bind)?;

        let local_addr = endpoint.local_addr().map_err(TransportError::Bind)?;
        info!(%local_addr, "QUIC server listening");

        Ok(Self {
            endpoint,
            local_addr,
        })
    }

    /// Accept the next incoming connection and wait for the handshake to finish.
    pub async fn accept(&self) -> Result<QuicConnection> {
        let incoming = self.endpoint.accept().await.ok_or(TransportError::NoIncoming)?;
        let connection = incoming.await?;
        let remote = connection.remote_address();
        info!(%remote, "accepted QUIC connection");
        Ok(QuicConnection { inner: connection, _endpoint: None })
    }

    /// The local socket address the server is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Gracefully shut down the endpoint, refusing new connections.
    pub fn shutdown(&self) {
        self.endpoint.close(VarInt::from_u32(0), b"server shutdown");
        info!("QUIC server shut down");
    }
}

// ---------------------------------------------------------------------------
// Insecure cert verifier (for self-signed certs)
// ---------------------------------------------------------------------------

/// A [`rustls`] certificate verifier that accepts any server certificate.
///
/// This is intentionally insecure and exists solely for the self-signed
/// ephemeral-receiver flow, where the client already trusts the server through
/// the out-of-band SSH bootstrap channel.
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        debug!("skipping server certificate verification (ephemeral self-signed)");
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// A QUIC client that connects to a remote [`QuicServer`].
pub struct QuicClient;

impl QuicClient {
    /// Connect to a QUIC server at `addr`.
    ///
    /// Certificate verification is **disabled** because the server uses an
    /// ephemeral self-signed certificate distributed over the SSH bootstrap
    /// channel. The `server_name` is used for the TLS SNI extension.
    pub async fn connect(addr: SocketAddr, server_name: &str) -> Result<QuicConnection> {
        let mut rustls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();

        // Enable 0-RTT for fast reconnects.
        rustls_config.enable_early_data = true;

        let quic_client_config =
            quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config)
                .map_err(|e| TransportError::TlsConfig(rustls::Error::General(e.to_string())))?;
        let client_config = ClientConfig::new(Arc::new(quic_client_config));

        let bind_addr: SocketAddr = if addr.is_ipv6() {
            SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0))
        } else {
            SocketAddr::from(([0, 0, 0, 0], 0))
        };

        let mut endpoint = Endpoint::client(bind_addr).map_err(TransportError::Bind)?;
        endpoint.set_default_client_config(client_config);

        info!(%addr, %server_name, "connecting to QUIC server");
        let connecting = endpoint.connect(addr, server_name)?;
        let connection = connecting.await?;
        let remote = connection.remote_address();
        info!(%remote, "QUIC connection established");

        Ok(QuicConnection {
            inner: connection,
            _endpoint: Some(endpoint),
        })
    }
}

// ---------------------------------------------------------------------------
// Connection wrapper
// ---------------------------------------------------------------------------

/// A wrapper around a Quinn [`Connection`] that provides stream-oriented helpers
/// aligned to the ByteHaul protocol's control/data stream model.
///
/// - **Control stream** (first bidirectional stream): carries the manifest,
///   resume state, and progress ACKs. It is ordered and reliable.
/// - **Data streams** (subsequent bidirectional streams): carry chunk data in
///   parallel. Each stream is reliable but independent of the others.
pub struct QuicConnection {
    inner: Connection,
    // Keep the client endpoint alive for the connection's lifetime.
    // Server-side connections get `None` (the server owns its endpoint).
    _endpoint: Option<Endpoint>,
}

impl QuicConnection {
    /// Open a bidirectional control stream (initiator side).
    ///
    /// This should be the **first** `open_bi` call on the connection so that
    /// both sides agree stream-0 is the control channel.
    pub async fn open_control_stream(&self) -> Result<(SendStream, RecvStream)> {
        debug!("opening control stream");
        let (send, recv) = self
            .inner
            .open_bi()
            .await
            .map_err(|_| TransportError::StreamOpen)?;
        debug!(stream_id = %send.id(), "control stream opened");
        Ok((send, recv))
    }

    /// Accept the bidirectional control stream (acceptor side).
    ///
    /// The remote peer must have opened the stream *and* written at least one
    /// byte before this future resolves (Quinn requirement for `accept_bi`).
    pub async fn accept_control_stream(&self) -> Result<(SendStream, RecvStream)> {
        debug!("waiting for control stream");
        let (send, recv) = self
            .inner
            .accept_bi()
            .await
            .map_err(|_| TransportError::StreamOpen)?;
        debug!(stream_id = %send.id(), "control stream accepted");
        Ok((send, recv))
    }

    /// Open a bidirectional data stream for transferring file chunks.
    pub async fn open_data_stream(&self) -> Result<(SendStream, RecvStream)> {
        let (send, recv) = self
            .inner
            .open_bi()
            .await
            .map_err(|_| TransportError::StreamOpen)?;
        debug!(stream_id = %send.id(), "data stream opened");
        Ok((send, recv))
    }

    /// Accept the next incoming bidirectional data stream.
    pub async fn accept_data_stream(&self) -> Result<(SendStream, RecvStream)> {
        let (send, recv) = self
            .inner
            .accept_bi()
            .await
            .map_err(|_| TransportError::StreamOpen)?;
        debug!(stream_id = %send.id(), "data stream accepted");
        Ok((send, recv))
    }

    /// Gracefully close the connection.
    pub fn close(&self) {
        info!(remote = %self.inner.remote_address(), "closing QUIC connection");
        self.inner.close(VarInt::from_u32(0), b"done");
    }

    /// The remote peer's socket address.
    pub fn remote_addr(&self) -> SocketAddr {
        self.inner.remote_address()
    }

    /// Obtain a reference to the underlying Quinn [`Connection`].
    ///
    /// Useful for advanced operations (e.g. reading handshake data or
    /// inspecting connection statistics).
    pub fn inner(&self) -> &Connection {
        &self.inner
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn self_signed_cert_generation_succeeds() {
        let (certs, _key) = generate_self_signed_cert().unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn default_transport_config_values() {
        let cfg = TransportConfig::default();
        assert_eq!(cfg.max_concurrent_streams, 16);
        assert_eq!(cfg.idle_timeout, Duration::from_secs(30));
        assert!(cfg.keep_alive_interval.is_none());
    }

    #[tokio::test]
    async fn server_binds_and_reports_addr() {
        let config = TransportConfig {
            bind_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            ..Default::default()
        };
        let server = QuicServer::bind(config).expect("server should bind");
        let addr = server.local_addr();
        assert!(addr.port() != 0, "OS should assign a real port");
        server.shutdown();
    }

    #[tokio::test]
    async fn client_server_echo_round_trip() {
        // Start server
        let server_config = TransportConfig {
            bind_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            ..Default::default()
        };
        let server = QuicServer::bind(server_config).expect("server should bind");
        let server_addr = server.local_addr();

        // Accept in background
        let accept_handle = tokio::spawn(async move {
            let conn = server.accept().await.expect("accept should succeed");
            let (mut send, mut recv) = conn
                .accept_control_stream()
                .await
                .expect("accept control stream");
            // Echo back whatever we receive
            let mut buf = [0u8; 64];
            let n = recv.read(&mut buf).await.unwrap().unwrap();
            send.write_all(&buf[..n]).await.unwrap();
            send.finish().unwrap();
            // Wait for the stream to be fully consumed before closing
            let _ = send.stopped().await;
            conn.close();
        });

        // Connect client
        let conn = QuicClient::connect(server_addr, "localhost")
            .await
            .expect("client connect");
        let (mut send, mut recv) = conn
            .open_control_stream()
            .await
            .expect("open control stream");

        // Send a message
        let msg = b"bytehaul-ping";
        send.write_all(msg).await.unwrap();
        send.finish().unwrap();

        // Read echo
        let response = recv.read_to_end(64).await.unwrap();
        assert_eq!(&response, msg);

        conn.close();
        accept_handle.await.unwrap();
    }
}
