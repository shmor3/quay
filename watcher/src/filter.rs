//! Glob-based path filtering for the file watcher.
//!
//! Provides utilities to build include/exclude glob sets and decide whether a
//! given path should be processed or skipped.

use globset::{Glob, GlobSet, GlobSetBuilder};
use std::path::Path;
use tracing::warn;

/// Default directory and file patterns that are always excluded unless
/// explicitly overridden by the user.
pub const DEFAULT_EXCLUDES: &[&str] = &[
    "target/**",
    ".git/**",
    "node_modules/**",
    "**/*.tmp",
    "**/*.swp",
    "**/.DS_Store",
    "**/Thumbs.db",
    "**/*.lock",
    "**/hotreload.yaml",
];

/// Compiled include/exclude filter pair.
#[derive(Debug, Clone)]
pub struct PathFilter {
    include_set: Option<GlobSet>,
    exclude_set: Option<GlobSet>,
}

impl PathFilter {
    /// Build a new `PathFilter` from raw include and exclude pattern slices.
    ///
    /// Invalid globs are logged as warnings and skipped rather than causing a
    /// hard failure.
    pub fn new(includes: &[String], excludes: &[String]) -> Self {
        Self {
            include_set: build_glob_set(includes),
            exclude_set: build_glob_set(excludes),
        }
    }

    /// Convenience constructor that merges [`DEFAULT_EXCLUDES`] with any
    /// additional exclude patterns (e.g. from config `ignore` lists).
    pub fn with_defaults(extra_excludes: &[String]) -> Self {
        let mut excludes: Vec<String> = DEFAULT_EXCLUDES.iter().map(|s| (*s).to_string()).collect();
        for pat in extra_excludes {
            if !excludes.contains(pat) {
                excludes.push(pat.clone());
            }
        }
        Self::new(&[], &excludes)
    }

    /// Decide whether a given path should be handled.
    ///
    /// Rules:
    /// 1. If the path matches an **exclude** pattern → `false`.
    /// 2. If include patterns exist and the path matches none of them → `false`.
    /// 3. Otherwise → `true`.
    pub fn is_allowed(&self, path: &str) -> bool {
        let p = Path::new(path);

        if let Some(exc) = &self.exclude_set {
            if exc.is_match(p) {
                return false;
            }
        }

        if let Some(inc) = &self.include_set {
            return inc.is_match(p);
        }

        // No include set means "include everything that wasn't excluded".
        true
    }
}

/// Normalize a filesystem path string to use forward slashes so that glob
/// matching works consistently across platforms.
pub fn normalize_path(p: &str) -> String {
    p.replace('\\', "/")
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Compile a slice of glob pattern strings into a [`GlobSet`].
///
/// Returns `None` when the input is empty.  Invalid patterns are warned and
/// skipped.
fn build_glob_set(patterns: &[String]) -> Option<GlobSet> {
    if patterns.is_empty() {
        return None;
    }

    let mut builder = GlobSetBuilder::new();
    let mut valid_count = 0u32;

    for pat in patterns {
        match Glob::new(pat) {
            Ok(g) => {
                builder.add(g);
                valid_count += 1;
            }
            Err(e) => {
                warn!(pattern = %pat, error = %e, "skipping invalid glob pattern");
            }
        }
    }

    if valid_count == 0 {
        return None;
    }

    match builder.build() {
        Ok(gs) => Some(gs),
        Err(e) => {
            warn!(error = %e, "failed to build glob set");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_excludes_reject_common_dirs() {
        let filter = PathFilter::with_defaults(&[]);

        assert!(!filter.is_allowed("target/debug/watchd"));
        assert!(!filter.is_allowed(".git/HEAD"));
        assert!(!filter.is_allowed("node_modules/express/index.js"));
        assert!(!filter.is_allowed("some/path/file.tmp"));
        assert!(!filter.is_allowed("dir/.DS_Store"));
    }

    #[test]
    fn default_excludes_allow_normal_files() {
        let filter = PathFilter::with_defaults(&[]);

        assert!(filter.is_allowed("src/main.rs"));
        assert!(filter.is_allowed("index.html"));
        assert!(filter.is_allowed("styles/app.css"));
    }

    #[test]
    fn extra_excludes_merged() {
        let filter = PathFilter::with_defaults(&["build/**".to_string()]);

        assert!(!filter.is_allowed("build/output.js"));
        // Default excludes still work
        assert!(!filter.is_allowed("target/release/bin"));
    }

    #[test]
    fn include_set_restricts_matches() {
        let filter = PathFilter::new(&["src/**/*.rs".to_string()], &["target/**".to_string()]);

        assert!(filter.is_allowed("src/main.rs"));
        assert!(filter.is_allowed("src/lib/util.rs"));
        assert!(!filter.is_allowed("tests/integration.py")); // not included
        assert!(!filter.is_allowed("target/debug/watchd")); // excluded
    }

    #[test]
    fn empty_filter_allows_everything() {
        let filter = PathFilter::new(&[], &[]);
        assert!(filter.is_allowed("literally/anything.xyz"));
    }

    #[test]
    fn normalize_path_converts_backslashes() {
        assert_eq!(normalize_path(r"a\b\c.txt"), "a/b/c.txt");
        assert_eq!(normalize_path("already/fine"), "already/fine");
    }

    #[test]
    fn invalid_glob_is_skipped_gracefully() {
        // A glob with an unclosed bracket is invalid
        let filter = PathFilter::new(&[], &["[invalid".to_string()]);
        // Should still work, just with no exclude set
        assert!(filter.is_allowed("anything"));
    }

    #[test]
    fn exclude_takes_priority_over_include() {
        let filter = PathFilter::new(&["**/*.rs".to_string()], &["generated/**".to_string()]);

        assert!(filter.is_allowed("src/main.rs"));
        assert!(!filter.is_allowed("generated/code.rs")); // excluded wins
    }
}
