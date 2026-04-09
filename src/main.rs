mod api;
mod app;
mod config;
mod configuration;
mod logging;
mod server_lifecycle;
mod server_registry;
mod service;
mod shutdown;

use anyhow::Result;

use crate::app::DaemonApp;
use crate::config::DaemonConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let options = CliOptions::parse(std::env::args().skip(1))?;

    if options.help {
        print_help();
        return Ok(());
    }

    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls crypto provider"))?;

    let mut config = DaemonConfig::load()?;
    logging::init(&config.logging, options.log_level_override())?;

    if options.configure {
        config.clear_enrollment()?;
    }

    let config = configuration::ensure_configured(config).await?;

    let app = DaemonApp::new(config);

    app.run().await
}

#[derive(Debug, Default, PartialEq, Eq)]
struct CliOptions {
    configure: bool,
    debug: bool,
    help: bool,
}

impl CliOptions {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut options = Self::default();

        for arg in args {
            match arg.as_str() {
                "--configure" => {
                    options.configure = true;
                }
                "--debug" | "-d" => {
                    options.debug = true;
                }
                "--help" | "-h" => {
                    options.help = true;
                }
                _ => {
                    anyhow::bail!("unknown argument: {arg}");
                }
            }
        }

        Ok(options)
    }

    fn log_level_override(&self) -> Option<&'static str> {
        if self.debug { Some("debug") } else { None }
    }
}

fn print_help() {
    println!("skyportd {}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Usage: skyportd [OPTIONS]");
    println!();
    println!("Options:");
    println!("      --configure  Re-run the configuration prompt");
    println!("  -d, --debug      Enable verbose debug logging");
    println!("  -h, --help       Show this help message");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_configure_flag() {
        let options = CliOptions::parse(["--configure".to_string()]).unwrap();

        assert_eq!(
            options,
            CliOptions {
                configure: true,
                debug: false,
                help: false,
            }
        );
    }

    #[test]
    fn parse_debug_flag() {
        let options = CliOptions::parse(["--debug".to_string()]).unwrap();

        assert_eq!(
            options,
            CliOptions {
                configure: false,
                debug: true,
                help: false,
            }
        );
        assert_eq!(options.log_level_override(), Some("debug"));
    }

    #[test]
    fn parse_help_flag() {
        let options = CliOptions::parse(["--help".to_string()]).unwrap();

        assert_eq!(
            options,
            CliOptions {
                configure: false,
                debug: false,
                help: true,
            }
        );
    }

    #[test]
    fn reject_unknown_flags() {
        let error = CliOptions::parse(["--verbose".to_string()]).unwrap_err();

        assert!(error.to_string().contains("unknown argument"));
    }
}
