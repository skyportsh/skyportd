use std::fmt;

use anyhow::Result;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::field::Visit;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::{FormatTime, UtcTime};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

use crate::config::{LogFormat, LoggingSection};

const ORANGE: &str = "\x1b[38;2;240;90;36m";
const ORANGE_DARK: &str = "\x1b[38;2;217;36;0m";
const GRAY: &str = "\x1b[38;2;120;120;120m";
const RESET: &str = "\x1b[0m";

pub fn init(config: &LoggingSection) -> Result<()> {
    let env_filter =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(config.level.as_str()))?;

    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_timer(UtcTime::rfc_3339());

    match config.format {
        LogFormat::Pretty => builder
            .event_format(SkyportPrettyFormatter::default())
            .try_init()
            .map_err(anyhow::Error::msg)?,
        LogFormat::Json => builder.json().try_init().map_err(anyhow::Error::msg)?,
    }

    Ok(())
}

#[derive(Default)]
struct SkyportPrettyFormatter;

impl<S, N> FormatEvent<S, N> for SkyportPrettyFormatter
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let level = *metadata.level();

        write!(writer, "{GRAY}")?;
        UtcTime::rfc_3339().format_time(&mut writer)?;
        write!(writer, "{RESET} ")?;
        write!(
            writer,
            "{}{:<5}{RESET} ",
            level_color(level),
            level_label(level)
        )?;
        write!(writer, "{GRAY}{}{RESET}", metadata.target())?;

        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        if let Some(message) = visitor.message.take() {
            write!(writer, " {ORANGE}{message}{RESET}")?;
        }

        if !visitor.fields.is_empty() {
            write!(writer, " {GRAY}")?;
            write!(writer, "{}", visitor.fields.join(" "))?;
            write!(writer, "{RESET}")?;
        }

        for span in ctx
            .event_scope()
            .into_iter()
            .flat_map(|scope| scope.from_root())
        {
            write!(writer, " {GRAY}{}={}{RESET}", "span", span.name())?;
        }

        writeln!(writer)
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
    fields: Vec<String>,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        let rendered = format!("{:?}", value);

        if field.name() == "message" {
            self.message = Some(trim_debug_quotes(&rendered).to_string());
            return;
        }

        self.fields.push(format!("{}={}", field.name(), rendered));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
            return;
        }

        self.fields.push(format!("{}={}", field.name(), value));
    }
}

fn level_color(level: Level) -> &'static str {
    match level {
        Level::ERROR => ORANGE_DARK,
        Level::WARN => ORANGE,
        Level::INFO => ORANGE,
        Level::DEBUG => GRAY,
        Level::TRACE => GRAY,
    }
}

fn level_label(level: Level) -> &'static str {
    match level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARN",
        Level::INFO => "INFO",
        Level::DEBUG => "DEBUG",
        Level::TRACE => "TRACE",
    }
}

fn trim_debug_quotes(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|trimmed| trimmed.strip_suffix('"'))
        .unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_logs_use_orange() {
        assert_eq!(level_color(Level::INFO), ORANGE);
    }

    #[test]
    fn trims_message_debug_quotes() {
        assert_eq!(trim_debug_quotes("\"daemon starting\""), "daemon starting");
        assert_eq!(trim_debug_quotes("plain"), "plain");
    }
}
