use anyhow::{anyhow, Context};
use rand::Rng;
use tracing::{event, instrument, Level};

pub mod protocol;

#[derive(Debug, PartialEq)]
pub struct SshSession {
    pub user: Option<String>,
    pub host: String,
    pub port: Option<u16>,
}

impl SshSession {
    pub fn local() -> Self {
        Self {
            user: None,
            host: "localhost".to_string(),
            port: None,
        }
    }
}

async fn setup_ssh_session(
    session: &SshSession,
) -> anyhow::Result<std::sync::Arc<openssh::Session>> {
    let host = session.host.as_str();
    let destination = match (session.user.as_deref(), session.port) {
        (Some(user), Some(port)) => format!("ssh://{}@{}:{}", user, host, port),
        (None, Some(port)) => format!("ssh://{}:{}", session.host, port),
        (Some(user), None) => format!("ssh://{}@{}", user, host),
        (None, None) => format!("ssh://{}", host),
    };
    event!(
        Level::DEBUG,
        "Connecting to SSH destination: {}",
        destination
    );
    let session = std::sync::Arc::new(
        openssh::Session::connect(destination, openssh::KnownHosts::Accept)
            .await
            .context("Failed to establish SSH connection")?,
    );
    Ok(session)
}

#[instrument]
pub async fn wait_for_rcpd_process(
    process: openssh::Child<std::sync::Arc<openssh::Session>>,
) -> anyhow::Result<()> {
    event!(Level::INFO, "Waiting on rcpd server on: {:?}", process);
    let output = process
        .wait_with_output()
        .await
        .context("Failed to wait for rcpd server (source) completion")?;
    if !output.status.success() {
        return Err(anyhow!(
            "rcpd command failed on remote host, stdout:\n{:?}\nstderr:\n{:?}",
            output.stdout,
            output.stderr,
        ));
    }
    Ok(())
}

#[instrument]
pub async fn start_rcpd(
    session: &SshSession,
    master_addr: &std::net::SocketAddr,
    master_server_name: &str,
) -> anyhow::Result<openssh::Child<std::sync::Arc<openssh::Session>>> {
    event!(Level::INFO, "Starting rcpd server on: {:?}", session);
    let session = setup_ssh_session(session).await?;
    // Run rcpd command remotely
    let current_exe = std::env::current_exe().context("Failed to get current executable path")?;
    let bin_dir = current_exe
        .parent()
        .context("Failed to get parent directory of current executable")?;
    event!(Level::DEBUG, "Running rcpd from: {:?}", bin_dir,);
    // TODO: if that doesn't work, try an alternative path
    let mut cmd = session.arc_command(format!("{}/rcpd", bin_dir.display()));
    cmd.arg("--master-addr")
        .arg(master_addr.to_string())
        .arg("--server-name")
        .arg(master_server_name)
        .arg("-vv") // TODO: remove this
        .spawn()
        .await
        .context("Failed to spawn rcpd command")
}

fn configure_server(max_concurrent_streams: u32) -> anyhow::Result<quinn::ServerConfig> {
    event!(Level::INFO, "Configuring QUIC server");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let key_der = cert.serialize_private_key_der();
    let cert_der = cert.serialize_der()?;
    let key = rustls::PrivateKey(key_der);
    let cert = rustls::Certificate(cert_der);
    let mut server_config = quinn::ServerConfig::with_single_cert(vec![cert], key)
        .context("Failed to create server config")?;
    std::sync::Arc::get_mut(&mut server_config.transport)
        .expect("Failed to get transport config")
        .max_concurrent_uni_streams(max_concurrent_streams.into())
        .max_idle_timeout(Some(tokio::time::Duration::from_secs(30).try_into()?));
    Ok(server_config)
}

#[instrument]
pub fn get_server(max_concurrent_streams: u32) -> anyhow::Result<quinn::Endpoint> {
    let server_config = configure_server(max_concurrent_streams)?;
    let addr = "0.0.0.0:0".parse::<std::net::SocketAddr>().unwrap();
    quinn::Endpoint::server(server_config, addr).context("Failed to create QUIC endpoint")
}

// Certificate verifier that accepts any server certificate
// Used for development/testing only
struct AcceptAnyCertificate;

impl rustls::client::ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

fn get_local_ip() -> anyhow::Result<std::net::IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    Ok(socket.local_addr()?.ip())
}

#[instrument]
pub fn get_endpoint_addr(endpoint: &quinn::Endpoint) -> anyhow::Result<std::net::SocketAddr> {
    // endpoint is bound to 0.0.0.0 so we need to get the local IP address
    let local_ip = get_local_ip().context("Failed to get local IP address")?;
    let endpoint_addr = endpoint.local_addr()?;
    Ok(std::net::SocketAddr::new(local_ip, endpoint_addr.port()))
}

#[instrument]
pub fn get_random_server_name() -> String {
    rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(20)
        .map(char::from)
        .collect()
}

#[instrument]
pub fn get_client() -> anyhow::Result<quinn::Endpoint> {
    // Create a crypto backend that accepts any server certificate (for development only)
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(std::sync::Arc::new(AcceptAnyCertificate))
        .with_no_client_auth();

    // Create QUIC client config
    let client_config = quinn::ClientConfig::new(std::sync::Arc::new(crypto));

    // Create and configure endpoint
    let endpoint = "0.0.0.0:0".parse::<std::net::SocketAddr>().unwrap();
    let mut endpoint =
        quinn::Endpoint::client(endpoint).context("Failed to create QUIC endpoint")?;
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}
