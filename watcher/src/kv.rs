//! In-memory key-value store for file change diffs.
//!
//! When the watcher detects a file change, the KV store:
//! 1. Reads the new file content.
//! 2. Compares it against the previously stored snapshot (if any).
//! 3. Computes a unified diff using `+` / `-` line prefixes.
//! 4. Stores the diff entry and updates the snapshot.
//!
//! The store is bounded: each key retains at most `capacity` diff entries
//! (oldest evicted first), and the total number of tracked keys is capped at
//! `max_keys` (least-recently-inserted key evicted when full).
//!
//! A per-file size limit (`max_file_size`) prevents a single large file from
//! blowing up memory.  Files exceeding the limit are recorded with a
//! placeholder diff (`<file too large: ... bytes>`) and their content is **not**
//! stored as a snapshot, so subsequent changes to the same oversized file will
//! each produce the same placeholder rather than a real diff.
//!
//! ## Thread safety
//!
//! All public access goes through [`SharedDiffStore`] (`Arc<Mutex<DiffStore>>`).
//! The lock is held only for short in-memory operations so contention is
//! negligible in practice.

use similar::{ChangeTag, TextDiff};
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default maximum file size (in bytes) that the diff store will process.
/// Files larger than this are skipped with a placeholder message.
/// 512 KiB — generous for source files, safe for memory.
#[allow(dead_code)]
pub const DEFAULT_MAX_FILE_SIZE: usize = 512 * 1024;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A single recorded diff for a file change.
#[derive(Debug, Clone)]
pub struct DiffEntry {
    /// Normalised path that changed.
    pub path: String,
    /// Wall-clock time when the change was recorded.
    pub timestamp: SystemTime,
    /// Human-readable unified diff text using `+` / `-` prefixes.
    /// Empty when this is the first time a file is seen (no previous snapshot).
    pub diff: String,
    /// Size (bytes) of the previous snapshot (`0` if first observation).
    pub old_size: usize,
    /// Size (bytes) of the new content.
    pub new_size: usize,
    /// `true` when the file content could not be decoded as UTF-8 and was
    /// treated as a binary blob (diff text will say `<binary file changed>`).
    pub binary: bool,
    /// `true` when the file exceeded `max_file_size` and the diff is a
    /// placeholder rather than real content.
    pub truncated: bool,
}

/// Summary statistics for the store.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StoreSummary {
    /// Number of distinct file paths tracked.
    pub tracked_files: usize,
    /// Total number of diff entries across all paths.
    pub total_diffs: usize,
    /// Per-key capacity (max history depth).
    pub capacity: usize,
    /// Maximum number of keys the store will track.
    pub max_keys: usize,
    /// Maximum file size (bytes) the store will diff.
    pub max_file_size: usize,
}

/// Thread-safe handle to a [`DiffStore`].
pub type SharedDiffStore = Arc<Mutex<DiffStore>>;

// ---------------------------------------------------------------------------
// DiffStore
// ---------------------------------------------------------------------------

/// Bounded in-memory store of file diffs and content snapshots.
pub struct DiffStore {
    /// Per-path diff history.  Each key maps to a bounded ring of entries.
    diffs: HashMap<String, VecDeque<DiffEntry>>,
    /// Last-known file content per path (used to compute the next diff).
    snapshots: HashMap<String, Vec<u8>>,
    /// Insertion order of keys, used for LRU-style eviction when `max_keys`
    /// is exceeded.
    key_order: VecDeque<String>,
    /// Maximum number of diff entries retained per path.
    capacity: usize,
    /// Maximum number of distinct paths tracked.
    max_keys: usize,
    /// Maximum file size (in bytes) the store will accept for diffing.
    /// Files larger than this produce a placeholder entry and their content
    /// is **not** stored as a snapshot.
    max_file_size: usize,
}

impl std::fmt::Debug for DiffStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiffStore")
            .field("tracked_files", &self.diffs.len())
            .field("capacity", &self.capacity)
            .field("max_keys", &self.max_keys)
            .field("max_file_size", &self.max_file_size)
            .finish()
    }
}

#[allow(dead_code)]
impl DiffStore {
    // -- Construction ------------------------------------------------------

    /// Create a new empty store.
    ///
    /// * `capacity`      – max diff entries per path (clamped to `1` minimum).
    /// * `max_keys`      – max distinct paths tracked (clamped to `1` minimum).
    /// * `max_file_size` – max file size in bytes; files larger than this are
    ///   recorded with a placeholder diff and their snapshot is not stored.
    ///   Clamped to `1` minimum.
    pub fn new(capacity: usize, max_keys: usize, max_file_size: usize) -> Self {
        let capacity = capacity.max(1);
        let max_keys = max_keys.max(1);
        let max_file_size = max_file_size.max(1);
        Self {
            diffs: HashMap::new(),
            snapshots: HashMap::new(),
            key_order: VecDeque::new(),
            capacity,
            max_keys,
            max_file_size,
        }
    }

    /// Create a [`SharedDiffStore`] wrapped in `Arc<Mutex<…>>`.
    pub fn new_shared(capacity: usize, max_keys: usize, max_file_size: usize) -> SharedDiffStore {
        Arc::new(Mutex::new(Self::new(capacity, max_keys, max_file_size)))
    }

    // -- Recording changes -------------------------------------------------

    /// Record a file change by reading `new_content`, diffing against the
    /// stored snapshot, and persisting the result.
    ///
    /// If `new_content` is larger than `max_file_size` the entry is recorded
    /// with a placeholder diff and the snapshot is **not** updated (so the
    /// next change to the same path will also produce a placeholder until the
    /// file shrinks back below the limit).
    ///
    /// Returns `Some(DiffEntry)` for the newly inserted entry, or `None` if
    /// an internal error prevented recording.
    pub fn record_change(&mut self, path: &str, new_content: &[u8]) -> Option<DiffEntry> {
        let new_size = new_content.len();

        // --- Size guard ---------------------------------------------------
        if new_size > self.max_file_size {
            debug!(
                path = path,
                size = new_size,
                limit = self.max_file_size,
                "file exceeds max_file_size; recording placeholder diff"
            );
            let old_size = self.snapshots.get(path).map_or(0, Vec::len);
            let entry = DiffEntry {
                path: path.to_string(),
                timestamp: SystemTime::now(),
                diff: format!(
                    "<file too large: {} bytes, limit {} bytes>",
                    new_size, self.max_file_size
                ),
                old_size,
                new_size,
                binary: false,
                truncated: true,
            };
            // Do NOT update the snapshot — we don't want to store megabytes.
            // We still record the diff entry itself (it's small).
            self.insert_entry(path, entry.clone());
            return Some(entry);
        }

        let old_content = self.snapshots.get(path).cloned();
        let old_size = old_content.as_ref().map_or(0, Vec::len);

        let (diff_text, binary) = compute_diff(old_content.as_deref(), new_content);

        let entry = DiffEntry {
            path: path.to_string(),
            timestamp: SystemTime::now(),
            diff: diff_text,
            old_size,
            new_size,
            binary,
            truncated: false,
        };

        // Update the snapshot.
        self.snapshots
            .insert(path.to_string(), new_content.to_vec());

        self.insert_entry(path, entry.clone());

        Some(entry)
    }

    /// Convenience: record a change by reading the file at `file_path` from
    /// disk.  Returns `None` if the file could not be read (error is logged).
    ///
    /// The file size is checked **before** reading to avoid pulling a huge
    /// file into memory just to reject it.
    pub fn record_change_from_disk(
        &mut self,
        normalized: &str,
        file_path: &Path,
    ) -> Option<DiffEntry> {
        // Fast-path: check file size via metadata before reading.
        match file_path.metadata() {
            Ok(meta) => {
                let len = meta.len() as usize;
                if len > self.max_file_size {
                    debug!(
                        path = %file_path.display(),
                        size = len,
                        limit = self.max_file_size,
                        "file exceeds max_file_size (pre-read check); recording placeholder"
                    );
                    let old_size = self.snapshots.get(normalized).map_or(0, Vec::len);
                    let entry = DiffEntry {
                        path: normalized.to_string(),
                        timestamp: SystemTime::now(),
                        diff: format!(
                            "<file too large: {} bytes, limit {} bytes>",
                            len, self.max_file_size
                        ),
                        old_size,
                        new_size: len,
                        binary: false,
                        truncated: true,
                    };
                    self.insert_entry(normalized, entry.clone());
                    return Some(entry);
                }
            }
            Err(e) => {
                warn!(
                    path = %file_path.display(),
                    error = %e,
                    "failed to stat file for diff store; will attempt read anyway"
                );
                // Fall through and try reading — the read itself will
                // produce a better error if the file truly doesn't exist.
            }
        }

        match std::fs::read(file_path) {
            Ok(content) => self.record_change(normalized, &content),
            Err(e) => {
                warn!(
                    path = %file_path.display(),
                    error = %e,
                    "failed to read file for diff store"
                );
                None
            }
        }
    }

    /// Seed the snapshot for a path *without* creating a diff entry.
    ///
    /// Useful for setting the initial state of a file so that the first real
    /// change produces a meaningful diff rather than showing the entire file
    /// as added.
    ///
    /// Content larger than `max_file_size` is silently ignored.
    pub fn seed_snapshot(&mut self, path: &str, content: &[u8]) {
        if content.len() > self.max_file_size {
            debug!(
                path = path,
                size = content.len(),
                limit = self.max_file_size,
                "seed_snapshot skipped: content exceeds max_file_size"
            );
            return;
        }
        self.snapshots.insert(path.to_string(), content.to_vec());
    }

    // -- Querying ----------------------------------------------------------

    /// Get the latest diff entry for `path`, if any.
    pub fn get_latest(&self, path: &str) -> Option<&DiffEntry> {
        self.diffs.get(path).and_then(|ring| ring.back())
    }

    /// Get the full diff history for `path`.
    pub fn get_history(&self, path: &str) -> Vec<&DiffEntry> {
        self.diffs
            .get(path)
            .map(|ring| ring.iter().collect())
            .unwrap_or_default()
    }

    /// List all tracked paths (in insertion order).
    pub fn list_keys(&self) -> Vec<&str> {
        self.key_order.iter().map(String::as_str).collect()
    }

    /// Return a summary of the store's current state.
    pub fn summary(&self) -> StoreSummary {
        StoreSummary {
            tracked_files: self.diffs.len(),
            total_diffs: self.diffs.values().map(VecDeque::len).sum(),
            capacity: self.capacity,
            max_keys: self.max_keys,
            max_file_size: self.max_file_size,
        }
    }

    /// Clear all entries and snapshots.
    pub fn clear(&mut self) {
        self.diffs.clear();
        self.snapshots.clear();
        self.key_order.clear();
        debug!("diff store cleared");
    }

    /// Remove a single key (its diffs and snapshot).  Returns `true` if the
    /// key existed.
    pub fn remove(&mut self, path: &str) -> bool {
        let existed = self.diffs.remove(path).is_some();
        self.snapshots.remove(path);
        self.key_order.retain(|k| k != path);
        existed
    }

    // -- Internal helpers --------------------------------------------------

    /// Insert a [`DiffEntry`] into the store, handling key-limit eviction and
    /// per-key capacity eviction.
    fn insert_entry(&mut self, path: &str, entry: DiffEntry) {
        // Evict oldest key if we've hit the limit and this is a new key.
        if !self.diffs.contains_key(path) {
            while self.diffs.len() >= self.max_keys {
                if let Some(evicted) = self.key_order.pop_front() {
                    debug!(path = %evicted, "evicting oldest tracked file from diff store");
                    self.diffs.remove(&evicted);
                    self.snapshots.remove(&evicted);
                } else {
                    break;
                }
            }
            self.key_order.push_back(path.to_string());
        }

        // Insert the diff entry, evicting the oldest if over capacity.
        let ring = self
            .diffs
            .entry(path.to_string())
            .or_insert_with(|| VecDeque::with_capacity(self.capacity));
        if ring.len() >= self.capacity {
            ring.pop_front();
        }
        ring.push_back(entry);
    }
}

// ---------------------------------------------------------------------------
// Diff computation
// ---------------------------------------------------------------------------

/// Detect whether `data` looks like a binary blob (contains null bytes in the
/// first 8 KiB).
fn is_binary(data: &[u8]) -> bool {
    let check_len = data.len().min(8192);
    data[..check_len].contains(&0)
}

/// Compute a unified diff between `old` and `new` content.
///
/// Returns `(diff_text, is_binary)`.
///
/// * Lines present only in the old content are prefixed with `- `.
/// * Lines present only in the new content are prefixed with `+ `.
/// * Unchanged context lines are prefixed with `  ` (two spaces).
/// * Binary files produce the placeholder `<binary file changed>`.
/// * When there is no old content the entire new file is shown as additions.
fn compute_diff(old: Option<&[u8]>, new: &[u8]) -> (String, bool) {
    let new_is_binary = is_binary(new);
    let old_is_binary = old.map_or(false, is_binary);

    if new_is_binary || old_is_binary {
        return ("<binary file changed>".to_string(), true);
    }

    let old_text = old
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    let new_text = String::from_utf8_lossy(new).into_owned();

    if old_text == new_text {
        return (String::new(), false);
    }

    let diff = TextDiff::from_lines(&old_text, &new_text);
    let mut output = String::new();

    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "- ",
            ChangeTag::Insert => "+ ",
            ChangeTag::Equal => "  ",
        };
        output.push_str(sign);
        let value = change.value();
        output.push_str(value);
        // Ensure every diff line ends with a newline so output is tidy.
        if !value.ends_with('\n') {
            output.push('\n');
        }
    }

    (output, false)
}

// ---------------------------------------------------------------------------
// JSON serialisation helpers (used by control socket / WS broadcast)
// ---------------------------------------------------------------------------

impl DiffEntry {
    /// Serialise this entry to a [`serde_json::Value`].
    pub fn to_json(&self) -> serde_json::Value {
        let ts = self
            .timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        serde_json::json!({
            "path": self.path,
            "timestamp": ts,
            "diff": self.diff,
            "old_size": self.old_size,
            "new_size": self.new_size,
            "binary": self.binary,
            "truncated": self.truncated,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Helper ------------------------------------------------------------

    fn make_store(capacity: usize, max_keys: usize) -> DiffStore {
        DiffStore::new(capacity, max_keys, DEFAULT_MAX_FILE_SIZE)
    }

    fn make_store_with_limit(capacity: usize, max_keys: usize, max_file_size: usize) -> DiffStore {
        DiffStore::new(capacity, max_keys, max_file_size)
    }

    // -- DiffStore::new ----------------------------------------------------

    #[test]
    fn new_clamps_capacity_to_one() {
        let store = make_store(0, 10);
        assert_eq!(store.capacity, 1);
    }

    #[test]
    fn new_clamps_max_keys_to_one() {
        let store = make_store(10, 0);
        assert_eq!(store.max_keys, 1);
    }

    #[test]
    fn new_clamps_max_file_size_to_one() {
        let store = make_store_with_limit(5, 10, 0);
        assert_eq!(store.max_file_size, 1);
    }

    #[test]
    fn new_starts_empty() {
        let store = make_store(5, 10);
        assert_eq!(store.diffs.len(), 0);
        assert_eq!(store.snapshots.len(), 0);
        assert!(store.list_keys().is_empty());
    }

    #[test]
    fn new_stores_max_file_size() {
        let store = make_store_with_limit(5, 10, 1024);
        assert_eq!(store.max_file_size, 1024);
    }

    // -- record_change -----------------------------------------------------

    #[test]
    fn first_change_records_entire_file_as_additions() {
        let mut store = make_store(5, 10);
        let content = b"line1\nline2\n";
        let entry = store.record_change("a.txt", content).unwrap();

        assert_eq!(entry.path, "a.txt");
        assert_eq!(entry.old_size, 0);
        assert_eq!(entry.new_size, 12);
        assert!(!entry.binary);
        assert!(!entry.truncated);
        assert!(entry.diff.contains("+ line1\n"));
        assert!(entry.diff.contains("+ line2\n"));
    }

    #[test]
    fn second_change_shows_diff() {
        let mut store = make_store(5, 10);
        store.record_change("a.txt", b"alpha\nbeta\n");
        let entry = store.record_change("a.txt", b"alpha\ngamma\n").unwrap();

        assert!(entry.diff.contains("- beta\n"), "diff: {}", entry.diff);
        assert!(entry.diff.contains("+ gamma\n"), "diff: {}", entry.diff);
        assert!(entry.diff.contains("  alpha\n"), "diff: {}", entry.diff);
        assert!(!entry.truncated);
    }

    #[test]
    fn unchanged_content_produces_empty_diff() {
        let mut store = make_store(5, 10);
        store.record_change("a.txt", b"hello\n");
        let entry = store.record_change("a.txt", b"hello\n").unwrap();
        assert!(entry.diff.is_empty());
        assert!(!entry.truncated);
    }

    #[test]
    fn binary_content_detected() {
        let mut store = make_store(5, 10);
        let binary = b"hello\x00world";
        let entry = store.record_change("img.png", binary).unwrap();
        assert!(entry.binary);
        assert_eq!(entry.diff, "<binary file changed>");
        assert!(!entry.truncated);
    }

    #[test]
    fn binary_old_content_detected() {
        let mut store = make_store(5, 10);
        store.record_change("file.bin", b"\x00\x01\x02");
        let entry = store.record_change("file.bin", b"now text\n").unwrap();
        assert!(entry.binary);
        assert_eq!(entry.diff, "<binary file changed>");
    }

    #[test]
    fn record_change_updates_snapshot() {
        let mut store = make_store(5, 10);
        store.record_change("f.txt", b"v1");
        assert_eq!(store.snapshots.get("f.txt").unwrap(), b"v1");

        store.record_change("f.txt", b"v2");
        assert_eq!(store.snapshots.get("f.txt").unwrap(), b"v2");
    }

    // -- max_file_size enforcement -----------------------------------------

    #[test]
    fn record_change_rejects_oversized_content() {
        let mut store = make_store_with_limit(5, 10, 16);
        let big = vec![b'X'; 17];
        let entry = store.record_change("big.txt", &big).unwrap();

        assert!(entry.truncated);
        assert!(entry.diff.contains("<file too large:"));
        assert!(entry.diff.contains("17 bytes"));
        assert!(entry.diff.contains("limit 16 bytes"));
        assert_eq!(entry.new_size, 17);
    }

    #[test]
    fn oversized_file_does_not_store_snapshot() {
        let mut store = make_store_with_limit(5, 10, 16);
        let big = vec![b'X'; 20];
        store.record_change("big.txt", &big);

        assert!(!store.snapshots.contains_key("big.txt"));
    }

    #[test]
    fn oversized_file_still_creates_diff_entry() {
        let mut store = make_store_with_limit(5, 10, 10);
        let big = vec![b'A'; 11];
        store.record_change("big.txt", &big);

        assert!(store.get_latest("big.txt").is_some());
        let entry = store.get_latest("big.txt").unwrap();
        assert!(entry.truncated);
    }

    #[test]
    fn file_at_exact_limit_is_accepted() {
        let mut store = make_store_with_limit(5, 10, 10);
        let content = vec![b'A'; 10];
        let entry = store.record_change("exact.txt", &content).unwrap();

        assert!(!entry.truncated);
        assert!(store.snapshots.contains_key("exact.txt"));
    }

    #[test]
    fn file_one_byte_over_limit_is_rejected() {
        let mut store = make_store_with_limit(5, 10, 10);
        let content = vec![b'A'; 11];
        let entry = store.record_change("over.txt", &content).unwrap();

        assert!(entry.truncated);
        assert!(!store.snapshots.contains_key("over.txt"));
    }

    #[test]
    fn small_file_after_oversized_produces_full_additions() {
        let mut store = make_store_with_limit(5, 10, 20);
        // First change: too big, no snapshot stored.
        let big = vec![b'X'; 30];
        store.record_change("f.txt", &big);

        // Second change: fits, but no prior snapshot → full additions.
        let entry = store.record_change("f.txt", b"hello\n").unwrap();
        assert!(!entry.truncated);
        assert_eq!(entry.old_size, 0); // no snapshot was kept
        assert!(entry.diff.contains("+ hello\n"));
    }

    #[test]
    fn oversized_after_normal_reports_old_size_from_snapshot() {
        let mut store = make_store_with_limit(5, 10, 20);
        store.record_change("f.txt", b"small\n");
        assert_eq!(store.snapshots.get("f.txt").unwrap().len(), 6);

        let big = vec![b'X'; 25];
        let entry = store.record_change("f.txt", &big).unwrap();
        assert!(entry.truncated);
        assert_eq!(entry.old_size, 6); // from the stored snapshot
        assert_eq!(entry.new_size, 25);
        // Snapshot should NOT be updated to the oversized content.
        assert_eq!(store.snapshots.get("f.txt").unwrap(), b"small\n");
    }

    #[test]
    fn repeated_oversized_changes_all_produce_placeholders() {
        let mut store = make_store_with_limit(3, 10, 10);
        for i in 0..5 {
            let big = vec![b'A' + (i as u8); 15];
            let entry = store.record_change("f.txt", &big).unwrap();
            assert!(entry.truncated);
        }
        // Capacity is 3, so history should have at most 3 entries.
        assert_eq!(store.get_history("f.txt").len(), 3);
    }

    #[test]
    fn seed_snapshot_rejects_oversized_content() {
        let mut store = make_store_with_limit(5, 10, 10);
        let big = vec![b'Z'; 20];
        store.seed_snapshot("f.txt", &big);

        assert!(!store.snapshots.contains_key("f.txt"));
    }

    #[test]
    fn seed_snapshot_accepts_content_at_limit() {
        let mut store = make_store_with_limit(5, 10, 10);
        let content = vec![b'Z'; 10];
        store.seed_snapshot("f.txt", &content);

        assert!(store.snapshots.contains_key("f.txt"));
    }

    #[test]
    fn summary_includes_max_file_size() {
        let store = make_store_with_limit(5, 10, 4096);
        let s = store.summary();
        assert_eq!(s.max_file_size, 4096);
    }

    // -- Capacity / eviction -----------------------------------------------

    #[test]
    fn oldest_diff_evicted_when_over_capacity() {
        let mut store = make_store(2, 10);
        store.record_change("f.txt", b"v1\n");
        store.record_change("f.txt", b"v2\n");
        store.record_change("f.txt", b"v3\n");

        let history = store.get_history("f.txt");
        // Capacity is 2 so only the last two entries survive.
        assert_eq!(history.len(), 2);
        // Second entry was v1→v2, third was v2→v3.  First (initial) was evicted.
        assert!(history[1].diff.contains("+ v3\n"));
    }

    #[test]
    fn oldest_key_evicted_when_over_max_keys() {
        let mut store = make_store(5, 2);
        store.record_change("a.txt", b"a");
        store.record_change("b.txt", b"b");
        // This should evict "a.txt".
        store.record_change("c.txt", b"c");

        assert!(store.get_latest("a.txt").is_none());
        assert!(store.get_latest("b.txt").is_some());
        assert!(store.get_latest("c.txt").is_some());
        assert_eq!(store.list_keys().len(), 2);
    }

    #[test]
    fn existing_key_does_not_trigger_eviction() {
        let mut store = make_store(5, 2);
        store.record_change("a.txt", b"a");
        store.record_change("b.txt", b"b");
        // Updating an existing key should NOT evict anything.
        store.record_change("a.txt", b"a2");

        assert!(store.get_latest("a.txt").is_some());
        assert!(store.get_latest("b.txt").is_some());
        assert_eq!(store.list_keys().len(), 2);
    }

    // -- Querying ----------------------------------------------------------

    #[test]
    fn get_latest_returns_none_for_unknown() {
        let store = make_store(5, 10);
        assert!(store.get_latest("nope").is_none());
    }

    #[test]
    fn get_latest_returns_most_recent() {
        let mut store = make_store(5, 10);
        store.record_change("f.txt", b"first\n");
        store.record_change("f.txt", b"second\n");

        let latest = store.get_latest("f.txt").unwrap();
        assert!(latest.diff.contains("+ second\n"));
    }

    #[test]
    fn get_history_empty_for_unknown() {
        let store = make_store(5, 10);
        assert!(store.get_history("nope").is_empty());
    }

    #[test]
    fn get_history_returns_all_entries() {
        let mut store = make_store(10, 10);
        for i in 0..5 {
            store.record_change("f.txt", format!("v{i}\n").as_bytes());
        }
        assert_eq!(store.get_history("f.txt").len(), 5);
    }

    #[test]
    fn list_keys_preserves_insertion_order() {
        let mut store = make_store(5, 10);
        store.record_change("c.txt", b"c");
        store.record_change("a.txt", b"a");
        store.record_change("b.txt", b"b");

        assert_eq!(store.list_keys(), vec!["c.txt", "a.txt", "b.txt"]);
    }

    // -- summary -----------------------------------------------------------

    #[test]
    fn summary_reflects_state() {
        let mut store = make_store(5, 100);
        store.record_change("x.txt", b"x1\n");
        store.record_change("x.txt", b"x2\n");
        store.record_change("y.txt", b"y1\n");

        let s = store.summary();
        assert_eq!(s.tracked_files, 2);
        assert_eq!(s.total_diffs, 3);
        assert_eq!(s.capacity, 5);
        assert_eq!(s.max_keys, 100);
    }

    #[test]
    fn summary_on_empty_store() {
        let store = make_store(3, 7);
        let s = store.summary();
        assert_eq!(s.tracked_files, 0);
        assert_eq!(s.total_diffs, 0);
        assert_eq!(s.capacity, 3);
        assert_eq!(s.max_keys, 7);
    }

    // -- clear / remove ----------------------------------------------------

    #[test]
    fn clear_removes_everything() {
        let mut store = make_store(5, 10);
        store.record_change("a.txt", b"a");
        store.record_change("b.txt", b"b");
        store.clear();

        assert!(store.list_keys().is_empty());
        assert!(store.diffs.is_empty());
        assert!(store.snapshots.is_empty());
    }

    #[test]
    fn remove_deletes_single_key() {
        let mut store = make_store(5, 10);
        store.record_change("a.txt", b"a");
        store.record_change("b.txt", b"b");

        assert!(store.remove("a.txt"));
        assert!(store.get_latest("a.txt").is_none());
        assert!(store.get_latest("b.txt").is_some());
        assert_eq!(store.list_keys(), vec!["b.txt"]);
    }

    #[test]
    fn remove_returns_false_for_unknown() {
        let mut store = make_store(5, 10);
        assert!(!store.remove("nope"));
    }

    // -- seed_snapshot -----------------------------------------------------

    #[test]
    fn seed_snapshot_makes_first_diff_meaningful() {
        let mut store = make_store(5, 10);
        store.seed_snapshot("f.txt", b"old line\n");
        let entry = store.record_change("f.txt", b"new line\n").unwrap();

        assert!(entry.diff.contains("- old line\n"), "diff: {}", entry.diff);
        assert!(entry.diff.contains("+ new line\n"), "diff: {}", entry.diff);
        assert_eq!(entry.old_size, 9);
    }

    #[test]
    fn seed_snapshot_does_not_create_diff_entry() {
        let mut store = make_store(5, 10);
        store.seed_snapshot("f.txt", b"content");

        assert!(store.get_latest("f.txt").is_none());
        assert!(store.list_keys().is_empty());
        // But snapshot IS stored.
        assert_eq!(store.snapshots.get("f.txt").unwrap(), b"content");
    }

    // -- compute_diff unit tests -------------------------------------------

    #[test]
    fn compute_diff_no_old_content() {
        let (diff, binary) = compute_diff(None, b"hello\n");
        assert!(!binary);
        assert!(diff.contains("+ hello\n"));
    }

    #[test]
    fn compute_diff_identical_content() {
        let (diff, binary) = compute_diff(Some(b"same\n"), b"same\n");
        assert!(!binary);
        assert!(diff.is_empty());
    }

    #[test]
    fn compute_diff_addition_and_removal() {
        let old = b"aaa\nbbb\nccc\n";
        let new = b"aaa\nBBB\nccc\n";
        let (diff, binary) = compute_diff(Some(old), new);
        assert!(!binary);
        assert!(diff.contains("- bbb\n"), "diff: {diff}");
        assert!(diff.contains("+ BBB\n"), "diff: {diff}");
        assert!(diff.contains("  aaa\n"), "diff: {diff}");
        assert!(diff.contains("  ccc\n"), "diff: {diff}");
    }

    #[test]
    fn compute_diff_binary_new() {
        let (diff, binary) = compute_diff(Some(b"text"), b"\x00bin");
        assert!(binary);
        assert_eq!(diff, "<binary file changed>");
    }

    #[test]
    fn compute_diff_binary_old() {
        let (diff, binary) = compute_diff(Some(b"\x00bin"), b"text");
        assert!(binary);
        assert_eq!(diff, "<binary file changed>");
    }

    #[test]
    fn compute_diff_both_binary() {
        let (diff, binary) = compute_diff(Some(b"\x00old"), b"\x00new");
        assert!(binary);
        assert_eq!(diff, "<binary file changed>");
    }

    #[test]
    fn compute_diff_empty_to_content() {
        let (diff, binary) = compute_diff(Some(b""), b"new\n");
        assert!(!binary);
        assert!(diff.contains("+ new\n"));
    }

    #[test]
    fn compute_diff_content_to_empty() {
        let (diff, binary) = compute_diff(Some(b"old\n"), b"");
        assert!(!binary);
        assert!(diff.contains("- old\n"));
    }

    #[test]
    fn compute_diff_multiline_additions() {
        let old = b"line1\n";
        let new = b"line1\nline2\nline3\n";
        let (diff, binary) = compute_diff(Some(old), new);
        assert!(!binary);
        assert!(diff.contains("  line1\n"), "diff: {diff}");
        assert!(diff.contains("+ line2\n"), "diff: {diff}");
        assert!(diff.contains("+ line3\n"), "diff: {diff}");
    }

    #[test]
    fn compute_diff_multiline_removals() {
        let old = b"line1\nline2\nline3\n";
        let new = b"line1\n";
        let (diff, binary) = compute_diff(Some(old), new);
        assert!(!binary);
        assert!(diff.contains("  line1\n"), "diff: {diff}");
        assert!(diff.contains("- line2\n"), "diff: {diff}");
        assert!(diff.contains("- line3\n"), "diff: {diff}");
    }

    #[test]
    fn compute_diff_no_trailing_newline_handled() {
        let old = b"no newline";
        let new = b"no newline here either";
        let (diff, _) = compute_diff(Some(old), new);
        // Should still produce valid output with newlines appended.
        assert!(diff.contains("- no newline\n"), "diff: {diff}");
        assert!(diff.contains("+ no newline here either\n"), "diff: {diff}");
    }

    // -- is_binary ---------------------------------------------------------

    #[test]
    fn is_binary_with_null_byte() {
        assert!(is_binary(b"hello\x00world"));
    }

    #[test]
    fn is_binary_pure_text() {
        assert!(!is_binary(b"hello world\n"));
    }

    #[test]
    fn is_binary_empty() {
        assert!(!is_binary(b""));
    }

    #[test]
    fn is_binary_null_at_start() {
        assert!(is_binary(b"\x00rest of file"));
    }

    #[test]
    fn is_binary_large_text_no_null() {
        let data = vec![b'A'; 16384];
        assert!(!is_binary(&data));
    }

    #[test]
    fn is_binary_null_past_8k_not_detected() {
        let mut data = vec![b'A'; 9000];
        data[8500] = 0;
        // Only first 8192 bytes checked, so null at 8500 is not seen.
        assert!(!is_binary(&data));
    }

    #[test]
    fn is_binary_null_within_8k_detected() {
        let mut data = vec![b'A'; 9000];
        data[4000] = 0;
        assert!(is_binary(&data));
    }

    // -- DiffEntry::to_json ------------------------------------------------

    #[test]
    fn diff_entry_to_json_has_all_fields() {
        let entry = DiffEntry {
            path: "src/main.rs".to_string(),
            timestamp: SystemTime::UNIX_EPOCH,
            diff: "+ added\n".to_string(),
            old_size: 10,
            new_size: 20,
            binary: false,
            truncated: false,
        };
        let json = entry.to_json();
        assert_eq!(json["path"], "src/main.rs");
        assert_eq!(json["timestamp"], 0);
        assert_eq!(json["diff"], "+ added\n");
        assert_eq!(json["old_size"], 10);
        assert_eq!(json["new_size"], 20);
        assert_eq!(json["binary"], false);
        assert_eq!(json["truncated"], false);
    }

    #[test]
    fn diff_entry_to_json_binary() {
        let entry = DiffEntry {
            path: "image.png".to_string(),
            timestamp: SystemTime::now(),
            diff: "<binary file changed>".to_string(),
            old_size: 100,
            new_size: 200,
            binary: true,
            truncated: false,
        };
        let json = entry.to_json();
        assert_eq!(json["binary"], true);
        assert_eq!(json["diff"], "<binary file changed>");
        assert_eq!(json["truncated"], false);
    }

    #[test]
    fn diff_entry_to_json_truncated() {
        let entry = DiffEntry {
            path: "huge.log".to_string(),
            timestamp: SystemTime::now(),
            diff: "<file too large: 999999 bytes, limit 512 bytes>".to_string(),
            old_size: 0,
            new_size: 999999,
            binary: false,
            truncated: true,
        };
        let json = entry.to_json();
        assert_eq!(json["truncated"], true);
        assert!(json["diff"].as_str().unwrap().contains("too large"));
    }

    // -- SharedDiffStore ---------------------------------------------------

    #[test]
    fn new_shared_creates_arc_mutex() {
        let shared = DiffStore::new_shared(5, 10, DEFAULT_MAX_FILE_SIZE);
        let guard = shared.lock().unwrap();
        assert_eq!(guard.capacity, 5);
        assert_eq!(guard.max_keys, 10);
        assert_eq!(guard.max_file_size, DEFAULT_MAX_FILE_SIZE);
    }

    #[test]
    fn shared_store_usable_across_scopes() {
        let shared = DiffStore::new_shared(5, 10, DEFAULT_MAX_FILE_SIZE);
        {
            let mut guard = shared.lock().unwrap();
            guard.record_change("x.txt", b"hello\n");
        }
        {
            let guard = shared.lock().unwrap();
            assert!(guard.get_latest("x.txt").is_some());
        }
    }

    // -- Debug impl --------------------------------------------------------

    #[test]
    fn debug_impl_works() {
        let store = make_store(5, 10);
        let dbg = format!("{:?}", store);
        assert!(dbg.contains("DiffStore"));
        assert!(dbg.contains("tracked_files"));
        assert!(dbg.contains("max_file_size"));
    }

    // -- StoreSummary serialisation ----------------------------------------

    #[test]
    fn store_summary_serialises_to_json() {
        let summary = StoreSummary {
            tracked_files: 3,
            total_diffs: 7,
            capacity: 10,
            max_keys: 50,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
        };
        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(json["tracked_files"], 3);
        assert_eq!(json["total_diffs"], 7);
        assert_eq!(json["capacity"], 10);
        assert_eq!(json["max_keys"], 50);
        assert_eq!(json["max_file_size"], DEFAULT_MAX_FILE_SIZE);
    }

    // -- Edge cases --------------------------------------------------------

    #[test]
    fn record_change_empty_path() {
        let mut store = make_store(5, 10);
        let entry = store.record_change("", b"content\n").unwrap();
        assert_eq!(entry.path, "");
        assert!(store.get_latest("").is_some());
    }

    #[test]
    fn record_change_empty_content() {
        let mut store = make_store(5, 10);
        let entry = store.record_change("f.txt", b"").unwrap();
        assert!(entry.diff.is_empty());
        assert_eq!(entry.new_size, 0);
        assert!(!entry.truncated);
    }

    #[test]
    fn record_change_very_large_content_within_limit() {
        let mut store = make_store_with_limit(2, 10, 2_000_000);
        let big = vec![b'X'; 1_000_000];
        let entry = store.record_change("big.txt", &big).unwrap();
        assert_eq!(entry.new_size, 1_000_000);
        assert!(!entry.truncated);
    }

    #[test]
    fn capacity_one_keeps_only_latest() {
        let mut store = make_store(1, 10);
        store.record_change("f.txt", b"v1\n");
        store.record_change("f.txt", b"v2\n");
        store.record_change("f.txt", b"v3\n");

        let history = store.get_history("f.txt");
        assert_eq!(history.len(), 1);
        assert!(history[0].diff.contains("+ v3\n") || history[0].diff.contains("- v2\n"));
    }

    #[test]
    fn max_keys_one_keeps_only_latest_file() {
        let mut store = make_store(5, 1);
        store.record_change("a.txt", b"a");
        store.record_change("b.txt", b"b");

        assert!(store.get_latest("a.txt").is_none());
        assert!(store.get_latest("b.txt").is_some());
        assert_eq!(store.list_keys().len(), 1);
    }

    #[test]
    fn eviction_cleans_up_snapshots() {
        let mut store = make_store(5, 1);
        store.record_change("a.txt", b"a");
        assert!(store.snapshots.contains_key("a.txt"));

        store.record_change("b.txt", b"b");
        assert!(!store.snapshots.contains_key("a.txt"));
        assert!(store.snapshots.contains_key("b.txt"));
    }

    #[test]
    fn multiple_keys_interleaved_changes() {
        let mut store = make_store(3, 10);
        store.record_change("a.txt", b"a1\n");
        store.record_change("b.txt", b"b1\n");
        store.record_change("a.txt", b"a2\n");
        store.record_change("b.txt", b"b2\n");

        assert_eq!(store.get_history("a.txt").len(), 2);
        assert_eq!(store.get_history("b.txt").len(), 2);
    }

    #[test]
    fn remove_after_clear_is_safe() {
        let mut store = make_store(5, 10);
        store.record_change("a.txt", b"a");
        store.clear();
        assert!(!store.remove("a.txt"));
    }

    #[test]
    fn record_after_clear_works() {
        let mut store = make_store(5, 10);
        store.record_change("a.txt", b"v1\n");
        store.clear();
        let entry = store.record_change("a.txt", b"v2\n").unwrap();
        // No previous snapshot after clear, so entire file is additions.
        assert!(entry.diff.contains("+ v2\n"));
        assert_eq!(entry.old_size, 0);
    }

    #[test]
    fn record_after_remove_loses_snapshot() {
        let mut store = make_store(5, 10);
        store.record_change("a.txt", b"v1\n");
        store.remove("a.txt");
        let entry = store.record_change("a.txt", b"v2\n").unwrap();
        // Snapshot was removed, so old_size is 0 and diff shows full additions.
        assert_eq!(entry.old_size, 0);
        assert!(entry.diff.contains("+ v2\n"));
    }

    // -- DiffEntry Clone ---------------------------------------------------

    #[test]
    fn diff_entry_clone() {
        let entry = DiffEntry {
            path: "p.txt".to_string(),
            timestamp: SystemTime::now(),
            diff: "+ line\n".to_string(),
            old_size: 5,
            new_size: 10,
            binary: false,
            truncated: false,
        };
        let cloned = entry.clone();
        assert_eq!(cloned.path, entry.path);
        assert_eq!(cloned.diff, entry.diff);
        assert_eq!(cloned.old_size, entry.old_size);
        assert_eq!(cloned.new_size, entry.new_size);
        assert_eq!(cloned.binary, entry.binary);
        assert_eq!(cloned.truncated, entry.truncated);
    }

    // -- record_change_from_disk -------------------------------------------

    #[test]
    fn record_change_from_disk_with_real_file() {
        let dir = std::env::temp_dir().join("kv_test_disk");
        let _ = std::fs::create_dir_all(&dir);
        let file = dir.join("test.txt");
        std::fs::write(&file, b"hello\n").unwrap();

        let mut store = make_store(5, 10);
        let entry = store.record_change_from_disk("test.txt", &file).unwrap();
        assert_eq!(entry.new_size, 6);
        assert!(entry.diff.contains("+ hello\n"));
        assert!(!entry.truncated);

        // Clean up.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn record_change_from_disk_missing_file() {
        let mut store = make_store(5, 10);
        let result = store.record_change_from_disk("nope.txt", Path::new("/no/such/file.txt"));
        assert!(result.is_none());
    }

    #[test]
    fn record_change_from_disk_oversized_file() {
        let dir = std::env::temp_dir().join("kv_test_disk_big");
        let _ = std::fs::create_dir_all(&dir);
        let file = dir.join("big.txt");
        let big = vec![b'Y'; 100];
        std::fs::write(&file, &big).unwrap();

        let mut store = make_store_with_limit(5, 10, 50);
        let entry = store.record_change_from_disk("big.txt", &file).unwrap();
        assert!(entry.truncated);
        assert!(entry.diff.contains("<file too large:"));
        // Snapshot should NOT be stored.
        assert!(!store.snapshots.contains_key("big.txt"));

        // Clean up.
        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Diff output format verification -----------------------------------

    #[test]
    fn diff_lines_use_correct_prefixes() {
        let mut store = make_store(5, 10);
        store.record_change("f.txt", b"keep\nold\n");
        let entry = store.record_change("f.txt", b"keep\nnew\n").unwrap();

        for line in entry.diff.lines() {
            assert!(
                line.starts_with("+ ") || line.starts_with("- ") || line.starts_with("  "),
                "unexpected line prefix: {:?}",
                line
            );
        }
    }

    #[test]
    fn diff_context_lines_have_two_space_prefix() {
        let mut store = make_store(5, 10);
        store.record_change("f.txt", b"ctx\nold\n");
        let entry = store.record_change("f.txt", b"ctx\nnew\n").unwrap();

        let context_lines: Vec<_> = entry.diff.lines().filter(|l| l.starts_with("  ")).collect();
        assert!(!context_lines.is_empty(), "should have context lines");
        assert!(context_lines[0].contains("ctx"));
    }

    // -- DEFAULT_MAX_FILE_SIZE constant ------------------------------------

    #[test]
    fn default_max_file_size_is_512k() {
        assert_eq!(DEFAULT_MAX_FILE_SIZE, 512 * 1024);
    }
}
