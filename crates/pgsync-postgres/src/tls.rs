//! rustls configuration shared by ordinary and replication connections.

use crate::url::TlsPolicy;
use pgsync_core::config::SafetyConfig;
use pgsync_core::{Error, Result};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::sync::Arc;

pub fn client_config(
    policy: TlsPolicy,
    safety: &SafetyConfig,
    url_ca_file: Option<&str>,
) -> Result<Arc<rustls::ClientConfig>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::config(format!("tls: {e}")))?;

    let config = if policy.verify() {
        let mut roots = rustls::RootCertStore::empty();
        let native = rustls_native_certs::load_native_certs();
        for cert in native.certs {
            let _ = roots.add(cert);
        }
        for path in safety
            .tls_ca_file
            .iter()
            .map(String::as_str)
            .chain(url_ca_file)
        {
            let pem = std::fs::read(path)
                .map_err(|e| Error::config(format!("cannot read CA file {path}: {e}")))?;
            let mut added = 0;
            for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
                let cert =
                    cert.map_err(|e| Error::config(format!("invalid certificate in {path}: {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| Error::config(format!("invalid certificate in {path}: {e}")))?;
                added += 1;
            }
            if added == 0 {
                return Err(Error::config(format!(
                    "no certificates found in CA file {path}"
                )));
            }
        }
        if roots.is_empty() {
            return Err(Error::config(
                "no trusted root certificates available; set safety.tls_ca_file",
            ));
        }
        builder.with_root_certificates(roots).with_no_client_auth()
    } else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
            .with_no_client_auth()
    };
    Ok(Arc::new(config))
}

/// Accepts any certificate. Only reachable for loopback hosts with
/// `safety.insecure_skip_tls_verify: true`.
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

pub fn server_name(host: &str) -> Result<ServerName<'static>> {
    ServerName::try_from(host.to_owned())
        .map_err(|e| Error::config(format!("invalid TLS server name {host:?}: {e}")))
}
