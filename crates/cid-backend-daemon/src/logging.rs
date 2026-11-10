//! Tracing subscriber: JSON or pretty, selected by `CID_LOG_FORMAT`.
//!
//! `RUST_LOG` is the filter (`info` if unset). Core digest events use target
//! `cid_backend` and a stable `event` field.

use tracing_subscriber::EnvFilter;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

impl LogFormat {
    pub fn from_env() -> Self {
        Self::from_var(std::env::var("CID_LOG_FORMAT").ok())
    }

    pub fn from_var(raw: Option<String>) -> Self {
        match raw
            .as_deref()
            .map(|s| s.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("pretty") | Some("text") => LogFormat::Pretty,
            _ => LogFormat::Json,
        }
    }
}

/// Install the process-wide subscriber. Safe to call once from `main`.
/// Subsequent calls are no-ops (`try_init`).
pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_log::LogTracer::init();
    match LogFormat::from_env() {
        LogFormat::Json => {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .json()
                .try_init();
        }
        LogFormat::Pretty => {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .pretty()
                .try_init();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_format_defaults_to_json() {
        assert_eq!(LogFormat::from_var(None), LogFormat::Json);
        assert_eq!(LogFormat::from_var(Some("".into())), LogFormat::Json);
        assert_eq!(LogFormat::from_var(Some("json".into())), LogFormat::Json);
        assert_eq!(LogFormat::from_var(Some("JSON".into())), LogFormat::Json);
    }

    #[test]
    fn log_format_pretty_aliases() {
        assert_eq!(
            LogFormat::from_var(Some("pretty".into())),
            LogFormat::Pretty
        );
        assert_eq!(LogFormat::from_var(Some("text".into())), LogFormat::Pretty);
    }
}
