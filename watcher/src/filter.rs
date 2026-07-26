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
    "**/quay.yaml",
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
        let mut excludes: Vec<String> = DEFAULT_EXCLUDES.iter().map(ToString::to_string).collect();
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

/// Return `path` expressed relative to `root`, normalized to forward slashes.
///
/// `notify` delivers **absolute** paths, but include/exclude globs and config
/// `watch:` patterns are written relative to the watch root (e.g. `src/**/*.rs`,
/// `target/**`).  globset anchors non-`**/`-prefixed patterns at the start of
/// the path, so matching an absolute path against them never succeeds — callers
/// MUST relativize first.  If `path` is not under `root` (should not happen for
/// a recursive watch) the normalized absolute path is returned as a fallback.
pub fn relativize_to_root(path: &Path, root: &Path) -> String {
    match path.strip_prefix(root) {
        Ok(rel) => normalize_path(&rel.to_string_lossy()),
        Err(_) => normalize_path(&path.to_string_lossy()),
    }
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
    use std::path::Path as StdPath;

    #[test]
    fn relativize_strips_watch_root_prefix() {
        let root = StdPath::new("/home/dev/proj");
        assert_eq!(
            relativize_to_root(StdPath::new("/home/dev/proj/target/debug/foo"), root),
            "target/debug/foo"
        );
        assert_eq!(
            relativize_to_root(StdPath::new("/home/dev/proj/src/main.rs"), root),
            "src/main.rs"
        );
    }

    #[test]
    fn relativize_path_not_under_root_falls_back_to_normalized() {
        let root = StdPath::new("/home/dev/proj");
        assert_eq!(
            relativize_to_root(StdPath::new("/etc/passwd"), root),
            "/etc/passwd"
        );
    }

    #[test]
    fn relativize_root_itself_is_empty() {
        let root = StdPath::new("/home/dev/proj");
        assert_eq!(relativize_to_root(StdPath::new("/home/dev/proj"), root), "");
    }

    #[test]
    fn default_excludes_reject_relativized_absolute_target() {
        let root = StdPath::new("/home/dev/proj");
        let filter = PathFilter::with_defaults(&[]);
        let rel = relativize_to_root(StdPath::new("/home/dev/proj/target/debug/x.o"), root);
        assert!(!filter.is_allowed(&rel), "target/ artifact must be excluded");
        let rel_git = relativize_to_root(StdPath::new("/home/dev/proj/.git/index"), root);
        assert!(!filter.is_allowed(&rel_git), ".git must be excluded");
        let rel_src = relativize_to_root(StdPath::new("/home/dev/proj/src/main.rs"), root);
        assert!(filter.is_allowed(&rel_src), "src file must be allowed");
    }

    #[test]
    fn default_excludes_reject_common_dirs() {
        let filter = PathFilter::with_defaults(&[]);

        assert!(!filter.is_allowed("target/debug/quay"));
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
        assert!(!filter.is_allowed("target/debug/quay")); // excluded
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

    // -- normalize_path edge cases -----------------------------------------

    #[test]
    fn normalize_path_empty_string() {
        assert_eq!(normalize_path(""), "");
    }

    #[test]
    fn normalize_path_only_backslashes() {
        assert_eq!(normalize_path(r"\\\"), "///");
    }

    #[test]
    fn normalize_path_mixed_separators() {
        assert_eq!(normalize_path(r"a/b\c/d\e"), "a/b/c/d/e");
    }

    #[test]
    fn normalize_path_windows_absolute() {
        assert_eq!(
            normalize_path(r"C:\Users\dev\project\src\main.rs"),
            "C:/Users/dev/project/src/main.rs"
        );
    }

    #[test]
    fn normalize_path_already_forward_slashes() {
        let p = "some/deep/path/file.txt";
        assert_eq!(normalize_path(p), p);
    }

    #[test]
    fn normalize_path_single_file() {
        assert_eq!(normalize_path("file.rs"), "file.rs");
    }

    #[test]
    fn normalize_path_trailing_separator() {
        assert_eq!(normalize_path(r"dir\subdir\"), "dir/subdir/");
    }

    #[test]
    fn normalize_path_unicode() {
        assert_eq!(
            normalize_path(r"日本語\パス\ファイル.txt"),
            "日本語/パス/ファイル.txt"
        );
    }

    // -- PathFilter::new edge cases ----------------------------------------

    #[test]
    fn filter_with_only_includes() {
        let filter = PathFilter::new(&["**/*.rs".to_string(), "**/*.toml".to_string()], &[]);
        assert!(filter.is_allowed("src/main.rs"));
        assert!(filter.is_allowed("Cargo.toml"));
        assert!(!filter.is_allowed("README.md"));
    }

    #[test]
    fn filter_with_only_excludes() {
        let filter = PathFilter::new(&[], &["**/*.log".to_string(), "tmp/**".to_string()]);
        assert!(filter.is_allowed("src/main.rs"));
        assert!(!filter.is_allowed("debug.log"));
        assert!(!filter.is_allowed("tmp/cache.dat"));
    }

    #[test]
    fn filter_with_overlapping_include_exclude() {
        // Include *.rs but exclude test files — exclude should win.
        let filter = PathFilter::new(&["**/*.rs".to_string()], &["**/test_*.rs".to_string()]);
        assert!(filter.is_allowed("src/main.rs"));
        assert!(!filter.is_allowed("src/test_utils.rs"));
    }

    #[test]
    fn filter_duplicate_excludes_handled() {
        let filter = PathFilter::new(
            &[],
            &[
                "target/**".to_string(),
                "target/**".to_string(),
                "target/**".to_string(),
            ],
        );
        assert!(!filter.is_allowed("target/debug/bin"));
        assert!(filter.is_allowed("src/main.rs"));
    }

    #[test]
    fn filter_duplicate_includes_handled() {
        let filter = PathFilter::new(&["**/*.rs".to_string(), "**/*.rs".to_string()], &[]);
        assert!(filter.is_allowed("src/main.rs"));
        assert!(!filter.is_allowed("style.css"));
    }

    // -- PathFilter::with_defaults edge cases ------------------------------

    #[test]
    fn with_defaults_includes_all_default_excludes() {
        let filter = PathFilter::with_defaults(&[]);
        for pattern_example in &[
            "target/release/binary",
            ".git/objects/abc123",
            "node_modules/express/lib/index.js",
            "src/backup.tmp",
            "editor.swp",
            "folder/.DS_Store",
            "icons/Thumbs.db",
            "Cargo.lock",
            "quay.yaml",
        ] {
            assert!(
                !filter.is_allowed(pattern_example),
                "expected '{}' to be excluded by defaults",
                pattern_example
            );
        }
    }

    #[test]
    fn with_defaults_does_not_duplicate_extras() {
        // Adding a pattern that's already in DEFAULT_EXCLUDES should not cause issues.
        let filter = PathFilter::with_defaults(&["target/**".to_string()]);
        assert!(!filter.is_allowed("target/debug/x"));
        assert!(filter.is_allowed("src/lib.rs"));
    }

    #[test]
    fn with_defaults_extra_excludes_work() {
        let filter = PathFilter::with_defaults(&["dist/**".to_string(), "coverage/**".to_string()]);
        assert!(!filter.is_allowed("dist/bundle.js"));
        assert!(!filter.is_allowed("coverage/lcov.info"));
        // Default excludes still in effect.
        assert!(!filter.is_allowed("target/debug/quay"));
        // Normal files still allowed.
        assert!(filter.is_allowed("src/main.rs"));
    }

    #[test]
    fn with_defaults_empty_extra_excludes() {
        let filter = PathFilter::with_defaults(&[]);
        // Should behave identically to defaults.
        assert!(filter.is_allowed("src/main.rs"));
        assert!(!filter.is_allowed(".git/HEAD"));
    }

    // -- is_allowed edge cases ---------------------------------------------

    #[test]
    fn is_allowed_empty_path() {
        let filter = PathFilter::with_defaults(&[]);
        // Empty string should not panic; whether it's allowed depends on the
        // glob engine but it must not crash.
        let _ = filter.is_allowed("");
    }

    #[test]
    fn is_allowed_dot_path() {
        let filter = PathFilter::with_defaults(&[]);
        assert!(filter.is_allowed("."));
    }

    #[test]
    fn is_allowed_deeply_nested_path() {
        let filter = PathFilter::with_defaults(&[]);
        assert!(filter.is_allowed("a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/file.rs"));
    }

    #[test]
    fn is_allowed_path_with_spaces() {
        let filter = PathFilter::with_defaults(&[]);
        assert!(filter.is_allowed("path/with spaces/file name.txt"));
    }

    #[test]
    fn is_allowed_path_with_special_chars() {
        let filter = PathFilter::with_defaults(&[]);
        assert!(filter.is_allowed("path/to/file-name_v2.0 (copy).txt"));
    }

    #[test]
    fn is_allowed_hidden_files() {
        let filter = PathFilter::with_defaults(&[]);
        // Hidden files (except those in default excludes) should be allowed.
        assert!(filter.is_allowed(".env"));
        assert!(filter.is_allowed(".gitignore"));
        // But .git directory contents are excluded.
        assert!(!filter.is_allowed(".git/config"));
    }

    // -- build_glob_set internals ------------------------------------------

    #[test]
    fn build_glob_set_empty_returns_none() {
        let result = build_glob_set(&[]);
        assert!(result.is_none());
    }

    #[test]
    fn build_glob_set_single_valid() {
        let result = build_glob_set(&["**/*.rs".to_string()]);
        assert!(result.is_some());
        let gs = result.unwrap();
        assert!(gs.is_match(Path::new("src/main.rs")));
    }

    #[test]
    fn build_glob_set_all_invalid() {
        let result = build_glob_set(&["[bad".to_string(), "[also_bad".to_string()]);
        assert!(result.is_none());
    }

    #[test]
    fn build_glob_set_mixed_valid_and_invalid() {
        let result = build_glob_set(&[
            "[bad".to_string(),
            "**/*.rs".to_string(),
            "[also_bad".to_string(),
            "**/*.toml".to_string(),
        ]);
        assert!(result.is_some());
        let gs = result.unwrap();
        assert!(gs.is_match(Path::new("src/main.rs")));
        assert!(gs.is_match(Path::new("Cargo.toml")));
        assert!(!gs.is_match(Path::new("README.md")));
    }

    #[test]
    fn build_glob_set_many_patterns() {
        let patterns: Vec<String> = (0..50).map(|i| format!("**/*.ext{}", i)).collect();
        let result = build_glob_set(&patterns);
        assert!(result.is_some());
        let gs = result.unwrap();
        assert!(gs.is_match(Path::new("dir/file.ext0")));
        assert!(gs.is_match(Path::new("dir/file.ext49")));
        assert!(!gs.is_match(Path::new("dir/file.ext50")));
    }

    // -- DEFAULT_EXCLUDES constant -----------------------------------------

    #[test]
    #[allow(clippy::const_is_empty)]
    fn default_excludes_is_non_empty() {
        assert!(!DEFAULT_EXCLUDES.is_empty());
    }

    #[test]
    fn default_excludes_all_valid_globs() {
        // Every pattern in DEFAULT_EXCLUDES must be a valid glob.
        for pattern in DEFAULT_EXCLUDES {
            let result = Glob::new(pattern);
            assert!(
                result.is_ok(),
                "DEFAULT_EXCLUDES contains invalid glob: '{}': {:?}",
                pattern,
                result.err()
            );
        }
    }

    #[test]
    fn default_excludes_contains_expected_entries() {
        let excludes: Vec<&str> = DEFAULT_EXCLUDES.to_vec();
        assert!(excludes.contains(&"target/**"));
        assert!(excludes.contains(&".git/**"));
        assert!(excludes.contains(&"node_modules/**"));
        assert!(excludes.contains(&"**/quay.yaml"));
    }

    // -- PathFilter Clone and Debug ----------------------------------------

    #[test]
    fn path_filter_clone() {
        let filter = PathFilter::with_defaults(&["extra/**".to_string()]);
        let cloned = filter.clone();

        // Both should behave identically.
        assert!(cloned.is_allowed("src/main.rs"));
        assert!(!cloned.is_allowed("target/debug/x"));
        assert!(!cloned.is_allowed("extra/output.js"));
    }

    #[test]
    fn path_filter_debug() {
        let filter = PathFilter::with_defaults(&[]);
        let dbg = format!("{:?}", filter);
        assert!(dbg.contains("PathFilter"), "debug output: {dbg}");
    }

    // -- Precedence and ordering -------------------------------------------

    #[test]
    fn exclude_always_wins_over_include_regardless_of_order() {
        // The filter should check exclude first, then include.
        let filter = PathFilter::new(&["**/*".to_string()], &["secret/**".to_string()]);
        assert!(!filter.is_allowed("secret/key.pem"));
        assert!(filter.is_allowed("public/index.html"));
    }

    #[test]
    fn include_restricts_when_no_exclude_matches() {
        let filter = PathFilter::new(&["src/**".to_string()], &[]);
        assert!(filter.is_allowed("src/main.rs"));
        assert!(filter.is_allowed("src/lib/util.rs"));
        assert!(!filter.is_allowed("tests/test.rs"));
        assert!(!filter.is_allowed("README.md"));
    }

    // -- Real-world scenarios ----------------------------------------------

    #[test]
    fn rust_project_filter() {
        let filter = PathFilter::new(
            &["**/*.rs".to_string(), "**/*.toml".to_string()],
            &["target/**".to_string()],
        );
        assert!(filter.is_allowed("src/main.rs"));
        assert!(filter.is_allowed("Cargo.toml"));
        assert!(!filter.is_allowed("target/debug/quay"));
        assert!(!filter.is_allowed("target/release/quay"));
        assert!(!filter.is_allowed("README.md")); // not in include set
    }

    #[test]
    fn web_project_filter() {
        let filter = PathFilter::with_defaults(&["dist/**".to_string(), ".cache/**".to_string()]);
        assert!(filter.is_allowed("src/index.js"));
        assert!(filter.is_allowed("styles/app.css"));
        assert!(filter.is_allowed("public/index.html"));
        assert!(!filter.is_allowed("dist/bundle.js"));
        assert!(!filter.is_allowed(".cache/hash123"));
        assert!(!filter.is_allowed("node_modules/express/index.js"));
    }

    #[test]
    fn filter_with_extension_patterns() {
        let filter = PathFilter::new(
            &[],
            &[
                "**/*.tmp".to_string(),
                "**/*.bak".to_string(),
                "**/*.swp".to_string(),
                "**/*~".to_string(),
            ],
        );
        assert!(filter.is_allowed("src/main.rs"));
        assert!(!filter.is_allowed("src/main.rs.tmp"));
        assert!(!filter.is_allowed("notes.bak"));
        assert!(!filter.is_allowed(".main.rs.swp"));
    }
}
