//! Shared outbound HTTP client.

use std::sync::LazyLock;
use std::time::Duration;

use reqwest::Client;

static CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(60))
        .build()
        .expect("failed to build the shared HTTP client")
});

/// Returns the process-wide client. Cloning is cheap and every clone shares
/// the same connection pool, so TLS handshakes are reused between requests.
pub fn client() -> Client {
    CLIENT.clone()
}
