//! A trust store for HTTPS connections, named by the `trust_store_file_path`
//! and `trust_store_password` options.
//!
//! A server is trusted whose own certificate is in the store, or, failing
//! that, whose chain leads to a certificate in it, without checking the
//! server's name.
//!
//! The store may be a PKCS#12 file or a JKS one, of which the trusted
//! certificate entries, and the certificate of each key entry, are read. A
//! PEM file of certificates is taken too.

use std::sync::Arc;

use log::{debug, error};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::{CertificateError, DigitallySignedStruct, RootCertStore, SignatureScheme};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

use crate::{KineticaError, KineticaOptions};

/// The certificates of a trust store.
#[derive(Debug, Clone)]
pub struct TrustStore {
    /// The file the store was read from
    path: String,
    /// Each certificate it trusts, as DER
    certificates: Vec<CertificateDer<'static>>,
}

impl TrustStore {
    /// The certificates the store trusts.
    pub fn certificates(&self) -> &[CertificateDer<'static>] {
        &self.certificates
    }
}

/// The trust store the options name, if they name one. It is used when both
/// its file and its password are given and server certificates are checked.
///
/// # Errors
///
/// Fails if the store cannot be read: a file that cannot be opened or a
/// password that does not open it.
pub(crate) fn from_options(options: &KineticaOptions) -> Result<Option<Arc<TrustStore>>, KineticaError> {
    if options.bypass_ssl_cert_check {
        // Logged once, here, rather than by each HTTP client the connection
        // makes
        debug!("Bypassing SSL certificate check for HTTPS connections");
        return Ok(None);
    }
    let path = &options.trust_store_file_path;
    debug!(
        "Using {} trust store for HTTPS connections: ",
        if path.is_empty() { "null" } else { path.as_str() }
    );
    if path.is_empty() || options.trust_store_password.is_empty() {
        return Ok(None);
    }
    load(path, &options.trust_store_password).map(|store| Some(Arc::new(store))).map_err(|message| {
        error!("Error : {message}");
        KineticaError::Config(message)
    })
}

/// Reads the trust store at `path`, opened with `password`.
pub(crate) fn load(path: &str, password: &str) -> Result<TrustStore, String> {
    let data = std::fs::read(path).map_err(|e| {
        let reason = e.to_string();
        let reason = reason.find(" (os error ").map_or(reason.as_str(), |at| &reason[..at]);
        format!("{path} ({reason})")
    })?;
    let certificates = if data.starts_with(&JKS_MAGIC) {
        jks_certificates(&data, password)?
    } else if is_pem(&data) {
        CertificateDer::pem_slice_iter(&data)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Invalid PEM certificate in {path}: {e}"))?
    } else {
        pkcs12_certificates(&data, password)?
    };
    Ok(TrustStore { path: path.to_string(), certificates })
}

/// Whether the data is PEM text.
fn is_pem(data: &[u8]) -> bool {
    let start = data.iter().position(|b| !b.is_ascii_whitespace()).unwrap_or(data.len());
    data[start..].starts_with(b"-----BEGIN")
}

/// The certificates a PKCS#12 store trusts: certificates marked as trusted,
/// and the first certificate of each key entry.
fn pkcs12_certificates(data: &[u8], password: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    use p12_keystore::{KeyStore, KeyStoreEntry, Pkcs12ImportPolicy};
    let store = KeyStore::from_pkcs12(data, password, Pkcs12ImportPolicy::Strict).map_err(|e| match e {
        p12_keystore::error::Error::MacError(_) => "the trust store password is incorrect".to_string(),
        other => other.to_string(),
    })?;
    Ok(store
        .entries()
        .filter_map(|(_, entry)| match entry {
            KeyStoreEntry::Certificate(certificate) => Some(certificate.as_der().to_vec()),
            KeyStoreEntry::PrivateKeyChain(chain) => chain.certs().first().map(|c| c.as_der().to_vec()),
            _ => None,
        })
        .map(CertificateDer::from)
        .collect())
}

/// The first bytes of a JKS file.
const JKS_MAGIC: [u8; 4] = [0xFE, 0xED, 0xFE, 0xED];

/// The certificates a JKS store trusts: each trusted certificate entry, and
/// the first certificate of each key entry. The store's integrity is checked
/// with the password.
fn jks_certificates(data: &[u8], password: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let invalid = || "the trust store is not a valid JKS store".to_string();
    let mut reader = Reader { data, at: 4 };
    let version = reader.u32().ok_or_else(invalid)?;
    if version != 1 && version != 2 {
        return Err(invalid());
    }
    let count = reader.u32().ok_or_else(invalid)?;
    let mut certificates = Vec::new();
    for _ in 0..count {
        let tag = reader.u32().ok_or_else(invalid)?;
        reader.utf().ok_or_else(invalid)?; // alias
        reader.take(8).ok_or_else(invalid)?; // date
        match tag {
            // A private key, then its certificate chain
            1 => {
                let key_length = reader.u32().ok_or_else(invalid)? as usize;
                reader.take(key_length).ok_or_else(invalid)?;
                let chain_length = reader.u32().ok_or_else(invalid)?;
                for i in 0..chain_length {
                    let certificate = reader.certificate(version).ok_or_else(invalid)?;
                    if i == 0 {
                        certificates.push(certificate);
                    }
                }
            }
            // A trusted certificate
            2 => certificates.push(reader.certificate(version).ok_or_else(invalid)?),
            _ => return Err(format!("the trust store has an entry of unknown kind {tag}")),
        }
    }

    // SHA-1 over the password as UTF-16 code units, "Mighty Aphrodite" and
    // the store itself
    let digest_at = reader.at;
    let stored = reader.take(20).ok_or_else(invalid)?;
    let mut context = ring::digest::Context::new(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY);
    for unit in password.encode_utf16() {
        context.update(&unit.to_be_bytes());
    }
    context.update(b"Mighty Aphrodite");
    context.update(&data[..digest_at]);
    if context.finish().as_ref() != stored {
        return Err("the trust store is damaged, or its password is incorrect".to_string());
    }
    Ok(certificates)
}

/// Reads a JKS file's big-endian fields.
struct Reader<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        let bytes = self.data.get(self.at..self.at.checked_add(length)?)?;
        self.at += length;
        Some(bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    /// A string: its length in two bytes, then its bytes.
    fn utf(&mut self) -> Option<&'a [u8]> {
        let length = u16::from_be_bytes(self.take(2)?.try_into().ok()?) as usize;
        self.take(length)
    }

    /// A certificate: in version 2 its type, then its length and encoding.
    fn certificate(&mut self, version: u32) -> Option<CertificateDer<'static>> {
        if version == 2 {
            self.utf()?;
        }
        let length = self.u32()? as usize;
        Some(CertificateDer::from(self.take(length)?.to_vec()))
    }
}

/// A server certificate verifier trusting what a trust store trusts: a
/// server whose own certificate is in the store, or whose chain leads to a
/// certificate in it. The server's name is not checked.
#[derive(Debug)]
pub(crate) struct TrustStoreVerifier {
    store: Arc<TrustStore>,
    /// Checks a chain against the store's certificates; none if the store
    /// has none
    chain: Option<Arc<WebPkiServerVerifier>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl TrustStoreVerifier {
    pub(crate) fn new(store: Arc<TrustStore>, provider: Arc<rustls::crypto::CryptoProvider>) -> Self {
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(store.certificates.iter().cloned());
        let chain = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone()).build().ok();
        Self { store, chain, provider }
    }
}

impl ServerCertVerifier for TrustStoreVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if self.store.certificates.is_empty() {
            error!("User supplied truststore {} contains no certificates", self.store.path);
        }
        // The server's own certificate, in the store
        if self.store.certificates.iter().any(|c| c.as_ref() == end_entity.as_ref()) {
            return Ok(ServerCertVerified::assertion());
        }
        // Else its chain, leading to one, whatever the server's name
        let Some(chain) = &self.chain else {
            return Err(rustls::Error::General("the trust store holds no certificates to trust".to_string()));
        };
        match chain.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now) {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            outcome => outcome,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hyper_client::{HttpClientConfig, HyperClient, RequestHeaders};

    // The certificate bypass is logged once, at debug, not for each HTTP
    // client a connection makes.
    #[test]
    fn a_certificate_bypass_is_logged_once_at_debug() {
        let options = KineticaOptions { bypass_ssl_cert_check: true, ..Default::default() };
        let (store, records) = crate::test_log::capture(|| from_options(&options));
        assert!(store.unwrap().is_none());
        assert_eq!(records, [(log::Level::Debug, "Bypassing SSL certificate check for HTTPS connections".to_string())]);

        let config = HttpClientConfig { bypass_ssl_cert_check: true, ..Default::default() };
        let (_, records) = crate::test_log::capture(|| HyperClient::with_config(&config));
        assert!(records.iter().all(|(level, _)| *level > log::Level::Info), "{records:?}");
    }
    use rustls_pki_types::PrivateKeyDer;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // A store fixture's path. The stores were made by keytool around
    // `cert.pem`, the certificate of `key.pem`, with the password
    // `changeit`; `openssl.p12` holds it too, but made by OpenSSL, without
    // the attribute that marks it as trusted.
    fn fixture(name: &str) -> String {
        format!("{}/src/testdata/trust_store/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    // Whether the fixtures are here to test with. The public repository leaves
    // them out, as a private key, even a test one, does not belong in it, and
    // there the tests that need them skip. It leaves gpudb-tests out too, so
    // beside gpudb-tests a missing fixture fails the test instead.
    fn fixtures_present() -> bool {
        let crate_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        if crate_dir.join("src/testdata/trust_store").is_dir() {
            return true;
        }
        assert!(
            !crate_dir.join("../gpudb-tests").is_dir(),
            "src/testdata/trust_store is missing from the repository"
        );
        eprintln!("skipped: the trust store fixtures are not in this repository");
        false
    }

    fn certificate() -> CertificateDer<'static> {
        CertificateDer::from_pem_file(fixture("cert.pem")).unwrap()
    }

    #[test]
    fn stores_are_read_with_their_errors() {
        if !fixtures_present() {
            return;
        }
        for name in ["truststore.jks", "truststore.p12", "server.p12", "cert.pem"] {
            let store = load(&fixture(name), "changeit").unwrap();
            assert_eq!(store.certificates(), [certificate()], "{name}");
        }
        assert!(load(&fixture("openssl.p12"), "changeit").unwrap().certificates().is_empty());

        assert_eq!(
            load(&fixture("truststore.jks"), "wrong").unwrap_err(),
            "the trust store is damaged, or its password is incorrect"
        );
        assert_eq!(load(&fixture("truststore.p12"), "wrong").unwrap_err(), "the trust store password is incorrect");
        let missing = fixture("missing.p12");
        assert_eq!(load(&missing, "changeit").unwrap_err(), format!("{missing} (No such file or directory)"));
    }

    /// A TLS server on `host` presenting `chain`, answering every request.
    async fn tls_server(host: &str, chain: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> String {
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = tokio::net::TcpListener::bind(format!("{host}:0")).await.unwrap();
        let url = format!("https://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(stream).await else { return };
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        if tls.read(&mut byte).await.unwrap_or(0) == 0 {
                            return;
                        }
                        head.push(byte[0]);
                    }
                    let _ = tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok").await;
                    let _ = tls.shutdown().await;
                });
            }
        });
        url
    }

    /// A request to `url` trusting `store`, or the system's roots.
    async fn request(url: &str, store: Option<TrustStore>) -> Result<u16, String> {
        let mut config = HttpClientConfig::from_options(&KineticaOptions::default());
        config.trust_store = store.map(Arc::new);
        config.max_retries = 0;
        HyperClient::with_config(&config)
            .post_json_raw(url, None, &RequestHeaders::default())
            .await
            .map(|response| response.status)
            .map_err(|e| e.to_string())
    }

    fn store_of(certificates: Vec<CertificateDer<'static>>) -> TrustStore {
        TrustStore { path: "test".to_string(), certificates }
    }

    // A server is trusted by its own certificate in the store, or by a chain
    // leading to one there, whatever the server's name; anything else is
    // refused.
    #[tokio::test]
    async fn servers_are_trusted_by_certificate_or_chain() {
        if !fixtures_present() {
            return;
        }
        // A store made by keytool, holding the server's own certificate,
        // which names some other host
        let key = PrivateKeyDer::from_pem_file(fixture("key.pem")).unwrap();
        let pinned = tls_server("127.0.0.171", vec![certificate()], key).await;
        assert_eq!(request(&pinned, Some(load(&fixture("truststore.jks"), "changeit").unwrap())).await, Ok(200));
        let refused = request(&pinned, None).await.unwrap_err();
        assert!(
            refused.contains("<No trust store was specified, but server requires SSL certificate validation.>"),
            "{refused}"
        );

        // A server whose certificate, for another name, a CA in the store
        // signed
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.distinguished_name.push(rcgen::DnType::CommonName, "Test CA");
        let ca = rcgen::CertifiedIssuer::self_signed(ca_params, rcgen::KeyPair::generate().unwrap()).unwrap();
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf = rcgen::CertificateParams::new(vec!["elsewhere.example".to_string()])
            .unwrap()
            .signed_by(&leaf_key, &ca)
            .unwrap();
        let chained = tls_server(
            "127.0.0.172",
            vec![leaf.der().clone()],
            PrivateKeyDer::try_from(leaf_key.serialize_der()).unwrap(),
        )
        .await;
        assert_eq!(request(&chained, Some(store_of(vec![ca.der().clone()]))).await, Ok(200));

        // A store trusting neither
        let unrelated = rcgen::generate_simple_self_signed(vec!["x".to_string()]).unwrap();
        let refused = request(&chained, Some(store_of(vec![unrelated.cert.der().clone()]))).await.unwrap_err();
        assert!(refused.contains("<No trust store was specified"), "{refused}");
        let refused = request(&chained, Some(store_of(Vec::new()))).await.unwrap_err();
        assert!(refused.contains("the trust store holds no certificates to trust"), "{refused}");
    }

    // A store named but unreadable fails the connection's creation; one
    // without a password, or with certificates unchecked, is not used.
    #[tokio::test]
    async fn a_store_that_cannot_be_read_fails_the_connection() {
        if !fixtures_present() {
            return;
        }
        let mut options = KineticaOptions::default();
        options.set_trust_store_file_path(fixture("truststore.p12")).set_trust_store_password("wrong");
        let Err(err) = crate::Kinetica::new(vec!["https://127.0.0.173:1".to_string()], options.clone()).await else {
            panic!("the store does not open");
        };
        assert_eq!(err.to_string(), "Configuration error: the trust store password is incorrect");

        options.set_trust_store_password("");
        assert!(from_options(&options).unwrap().is_none());
        options.set_trust_store_password("wrong").set_bypass_ssl_cert_check(true);
        assert!(from_options(&options).unwrap().is_none());
    }
}
