//! Configuration file parsing for `hotreload.yaml`.
//!
//! Supports both a single config mapping and a YAML sequence of configs.
//! Each config entry describes watch patterns, commands to run, notification
//! behaviour, and additional ignore globs.

use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::Deserialize;
use std::path::Path;
use tracing::warn;

/// A single configuration entry parsed from `hotreload.yaml`.
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    /// Human-readable name for this config (used in status/logging).
    pub name: String,
    /// Glob patterns that determine which changed files this config handles.
    pub watches: Vec<String>,
    /// Compiled glob set built from `watches` (populated by [`compile_watch_set`]).
    pub watch_set: Option<GlobSet>,
    /// Command template to execute when a matching file changes (may contain `{path}`).
    pub on_change: Option<String>,
    /// Alternative build command (used when `on_change` is absent).
    pub build: Option<String>,
    /// Notification mode: `"auto"`, `"reload"`, `"inject-css"`, or `"none"`.
    pub notify: NotifyMode,
    /// Additional exclude globs merged into the watcher's exclude set.
    pub ignore: Vec<String>,
}

/// Notification mode controlling how browser clients are informed of changes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum NotifyMode {
    /// Decide based on file extension (CSS → inject, otherwise reload).
    #[default]
    Auto,
    /// Always send a full-page reload.
    Reload,
    /// Always inject CSS content.
    InjectCss,
    /// Do not notify browser clients.
    None,
}

impl NotifyMode {
    fn from_str_lossy(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Self::Auto,
            "reload" => Self::Reload,
            "inject-css" | "inject_css" | "injectcss" => Self::InjectCss,
            "none" => Self::None,
            other => {
                warn!(value = other, "unknown notify mode, falling back to 'auto'");
                Self::Auto
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Raw serde model – mirrors the YAML schema before post-processing
// ---------------------------------------------------------------------------

/// Intermediate representation that serde deserializes directly from YAML.
#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(default = "default_name")]
    name: String,

    /// Accepts either a single glob string or a list of globs.
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    watch: Vec<String>,

    on_change: Option<String>,
    build: Option<String>,

    #[serde(default)]
    notify: Option<String>,

    /// Accepts either a single glob string or a list of globs.
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    ignore: Vec<String>,
}

fn default_name() -> String {
    "unnamed".to_string()
}

/// Custom deserializer that accepts either `"glob"` or `["glob1", "glob2"]`.
fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrVec {
        Single(String),
        Multiple(Vec<String>),
    }

    match StringOrVec::deserialize(deserializer)? {
        StringOrVec::Single(s) => Ok(vec![s]),
        StringOrVec::Multiple(v) => Ok(v),
    }
}

// ---------------------------------------------------------------------------
// Conversion from raw → domain model
// ---------------------------------------------------------------------------

impl From<RawConfig> for ConfigEntry {
    fn from(raw: RawConfig) -> Self {
        let watches: Vec<String> = raw.watch.iter().map(|p| normalize_pattern(p)).collect();
        let ignore: Vec<String> = raw.ignore.iter().map(|p| normalize_pattern(p)).collect();
        let notify = raw
            .notify
            .as_deref()
            .map(NotifyMode::from_str_lossy)
            .unwrap_or_default();

        Self {
            name: raw.name,
            watches,
            watch_set: None,
            on_change: raw.on_change,
            build: raw.build,
            notify,
            ignore,
        }
    }
}

impl ConfigEntry {
    /// Returns `true` if `path` matches any of this config's watch patterns.
    ///
    /// Returns `false` when no watch patterns are defined.
    pub fn matches(&self, path: &str) -> bool {
        match &self.watch_set {
            Some(gs) => gs.is_match(Path::new(path)),
            None => false,
        }
    }

    /// Returns the command to execute for a change, preferring `on_change` over `build`.
    pub fn command_for(&self, normalized_path: &str) -> Option<String> {
        self.on_change
            .as_deref()
            .or(self.build.as_deref())
            .map(|tpl| tpl.replace("{path}", normalized_path))
    }

    /// Compiles the `watches` globs into a `GlobSet` and stores it in `watch_set`.
    ///
    /// Invalid globs are logged and skipped.
    pub fn compile_watch_set(&mut self) {
        if self.watches.is_empty() {
            return;
        }
        let mut builder = GlobSetBuilder::new();
        for pat in &self.watches {
            match Glob::new(pat) {
                Ok(g) => {
                    builder.add(g);
                }
                Err(e) => {
                    warn!(pattern = %pat, error = %e, "skipping invalid watch glob");
                }
            }
        }
        match builder.build() {
            Ok(gs) => self.watch_set = Some(gs),
            Err(e) => warn!(error = %e, "failed to build watch glob set"),
        }
    }
}

// ---------------------------------------------------------------------------
// Public parsing API
// ---------------------------------------------------------------------------

/// Parse the contents of a `hotreload.yaml` file into a list of [`ConfigEntry`] values.
///
/// The YAML may be either:
/// - A single mapping (one config)
/// - A sequence of mappings (multiple configs)
///
/// Invalid entries are logged and skipped rather than failing the entire parse.
pub fn parse_configs(yaml: &str) -> Vec<ConfigEntry> {
    // Try as a sequence first, then as a single mapping.
    if let Ok(raw_list) = serde_yaml::from_str::<Vec<RawConfig>>(yaml) {
        return raw_list.into_iter().map(ConfigEntry::from).collect();
    }

    if let Ok(raw) = serde_yaml::from_str::<RawConfig>(yaml) {
        return vec![ConfigEntry::from(raw)];
    }

    warn!("failed to parse hotreload.yaml as YAML; no configs loaded");
    Vec::new()
}

/// Normalize a glob pattern by converting backslashes to forward slashes.
pub fn normalize_pattern(p: &str) -> String {
    p.replace('\\', "/")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_config() {
        let yaml = r#"
name: demo
watch: "**/*.txt"
on_change: "echo changed {path}"
notify: auto
"#;
        let configs = parse_configs(yaml);
        assert_eq!(configs.len(), 1);

        let c = &configs[0];
        assert_eq!(c.name, "demo");
        assert_eq!(c.watches, vec!["**/*.txt"]);
        assert_eq!(c.on_change.as_deref(), Some("echo changed {path}"));
        assert_eq!(c.notify, NotifyMode::Auto);
        assert!(c.ignore.is_empty());
    }

    #[test]
    fn parse_single_config_with_list_watch() {
        let yaml = r#"
name: yamldemo
watch:
  - "**/*.ts"
  - "**/*.tsx"
on_change: "npm run build -- {path}"
notify: reload
ignore:
  - "target/**"
"#;
        let configs = parse_configs(yaml);
        assert_eq!(configs.len(), 1);

        let c = &configs[0];
        assert_eq!(c.name, "yamldemo");
        assert_eq!(c.watches.len(), 2);
        assert_eq!(c.on_change.as_deref(), Some("npm run build -- {path}"));
        assert_eq!(c.notify, NotifyMode::Reload);
        assert_eq!(c.ignore, vec!["target/**"]);
    }

    #[test]
    fn parse_multiple_configs() {
        let yaml = r#"
- name: js
  watch:
    - "**/*.js"
  notify: reload

- name: css
  watch:
    - "**/*.css"
  notify: inject-css
"#;
        let configs = parse_configs(yaml);
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].name, "js");
        assert_eq!(configs[1].name, "css");
        assert_eq!(configs[0].notify, NotifyMode::Reload);
        assert_eq!(configs[1].notify, NotifyMode::InjectCss);
    }

    #[test]
    fn parse_unnamed_config() {
        let yaml = "watch: \"**/*.rs\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].name, "unnamed");
    }

    #[test]
    fn compile_and_match() {
        let yaml = "name: test\nwatch: \"src/**/*.rs\"\n";
        let mut configs = parse_configs(yaml);
        configs[0].compile_watch_set();
        assert!(configs[0].matches("src/main.rs"));
        assert!(configs[0].matches("src/lib/util.rs"));
        assert!(!configs[0].matches("tests/integration.rs"));
    }

    #[test]
    fn command_for_substitution() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"echo {path}\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(
            configs[0].command_for("src/main.rs"),
            Some("echo src/main.rs".to_string())
        );
    }

    #[test]
    fn command_for_falls_back_to_build() {
        let yaml = "name: t\nwatch: \"*\"\nbuild: \"make {path}\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(
            configs[0].command_for("foo.c"),
            Some("make foo.c".to_string())
        );
    }

    #[test]
    fn command_for_none_when_no_commands() {
        let yaml = "name: t\nwatch: \"*\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs[0].command_for("foo.c"), None);
    }

    #[test]
    fn normalize_pattern_backslashes() {
        assert_eq!(normalize_pattern("a\\b\\**"), "a/b/**");
        assert_eq!(normalize_pattern("already/fine"), "already/fine");
    }

    #[test]
    fn unknown_notify_mode_defaults_to_auto() {
        let mode = NotifyMode::from_str_lossy("bogus");
        assert_eq!(mode, NotifyMode::Auto);
    }

    #[test]
    fn notify_mode_case_insensitive() {
        assert_eq!(NotifyMode::from_str_lossy("RELOAD"), NotifyMode::Reload);
        assert_eq!(
            NotifyMode::from_str_lossy("Inject-CSS"),
            NotifyMode::InjectCss
        );
        assert_eq!(NotifyMode::from_str_lossy("None"), NotifyMode::None);
    }

    #[test]
    fn empty_yaml_returns_empty() {
        let configs = parse_configs("");
        // serde_yaml parses "" as Null, which won't match either Vec or RawConfig
        assert!(configs.is_empty());
    }

    #[test]
    fn invalid_yaml_returns_empty() {
        // Use YAML that parses but doesn't match our RawConfig schema
        let configs = parse_configs("42");
        assert!(configs.is_empty());
    }
}
