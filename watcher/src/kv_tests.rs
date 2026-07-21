
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

    // -- too_large_placeholder helper --------------------------------------

    #[test]
    fn too_large_placeholder_format() {
        let msg = DiffStore::too_large_placeholder(1024, 512);
        assert_eq!(msg, "<file too large: 1024 bytes, limit 512 bytes>");
    }

    #[test]
    fn too_large_placeholder_zero_values() {
        let msg = DiffStore::too_large_placeholder(0, 0);
        assert_eq!(msg, "<file too large: 0 bytes, limit 0 bytes>");
    }

    #[test]
    fn too_large_placeholder_large_values() {
        let msg = DiffStore::too_large_placeholder(10_000_000, 524_288);
        assert!(msg.contains("10000000 bytes"));
        assert!(msg.contains("limit 524288 bytes"));
    }

    #[test]
    fn too_large_placeholder_matches_record_change_output() {
        // Verify the helper produces the same string that record_change
        // embeds in DiffEntry when content exceeds the limit.
        let mut store = make_store_with_limit(5, 10, 16);
        let big = vec![b'X'; 20];
        let entry = store.record_change("f.txt", &big).unwrap();
        assert_eq!(entry.diff, DiffStore::too_large_placeholder(20, 16));
    }

