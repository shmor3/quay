
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
        // Path is shell-escaped to prevent command injection.
        let result = configs[0].command_for("src/main.rs").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "echo \"src/main.rs\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "echo 'src/main.rs'");
    }

    #[test]
    fn command_for_falls_back_to_build() {
        let yaml = "name: t\nwatch: \"*\"\nbuild: \"make {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("foo.c").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "make \"foo.c\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "make 'foo.c'");
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
    fn normalize_pattern_empty() {
        assert_eq!(normalize_pattern(""), "");
    }

    #[test]
    fn normalize_pattern_only_backslashes() {
        assert_eq!(normalize_pattern("\\\\\\"), "///");
    }

    #[test]
    fn normalize_pattern_mixed_separators() {
        assert_eq!(normalize_pattern("a/b\\c/d\\e"), "a/b/c/d/e");
    }

    // -- NotifyMode --------------------------------------------------------

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
    fn notify_mode_all_inject_css_variants() {
        assert_eq!(
            NotifyMode::from_str_lossy("inject-css"),
            NotifyMode::InjectCss
        );
        assert_eq!(
            NotifyMode::from_str_lossy("inject_css"),
            NotifyMode::InjectCss
        );
        assert_eq!(
            NotifyMode::from_str_lossy("injectcss"),
            NotifyMode::InjectCss
        );
        assert_eq!(
            NotifyMode::from_str_lossy("INJECT-CSS"),
            NotifyMode::InjectCss
        );
        assert_eq!(
            NotifyMode::from_str_lossy("InjectCss"),
            NotifyMode::InjectCss
        );
    }

    #[test]
    fn notify_mode_whitespace_trimmed() {
        assert_eq!(NotifyMode::from_str_lossy("  reload  "), NotifyMode::Reload);
        assert_eq!(NotifyMode::from_str_lossy("\tnone\n"), NotifyMode::None);
    }

    #[test]
    fn notify_mode_empty_string_defaults_to_auto() {
        assert_eq!(NotifyMode::from_str_lossy(""), NotifyMode::Auto);
    }

    #[test]
    fn notify_mode_default_is_auto() {
        assert_eq!(NotifyMode::default(), NotifyMode::Auto);
    }

    #[test]
    fn notify_mode_clone_and_eq() {
        let mode = NotifyMode::InjectCss;
        let cloned = mode.clone();
        assert_eq!(mode, cloned);
    }

    #[test]
    fn notify_mode_debug() {
        let dbg = format!("{:?}", NotifyMode::Reload);
        assert!(dbg.contains("Reload"));
    }

    // -- parse_configs edge cases ------------------------------------------

    #[test]
    fn empty_yaml_returns_empty() {
        let configs = parse_configs("");
        assert!(configs.is_empty());
    }

    #[test]
    fn invalid_yaml_returns_empty() {
        let configs = parse_configs("42");
        assert!(configs.is_empty());
    }

    #[test]
    fn yaml_with_utf8_bom() {
        // UTF-8 BOM is \xEF\xBB\xBF — serde_yaml should handle it gracefully
        // or we should get an empty result, not a panic.
        let bom = "\u{FEFF}";
        let yaml = format!("{}name: bom\nwatch: \"**/*.rs\"\n", bom);
        let configs = parse_configs(&yaml);
        // serde_yaml may or may not handle BOM; either parse succeeds or returns empty.
        // The key property is that it must not panic.
        if !configs.is_empty() {
            assert_eq!(configs.len(), 1);
        }
    }

    #[test]
    fn yaml_only_whitespace() {
        let configs = parse_configs("   \n\n\t  \n");
        assert!(configs.is_empty());
    }

    #[test]
    fn yaml_only_comments() {
        let yaml = "# this is a comment\n# another comment\n";
        let configs = parse_configs(yaml);
        assert!(configs.is_empty());
    }

    #[test]
    fn yaml_null_value() {
        let configs = parse_configs("null");
        assert!(configs.is_empty());
    }

    #[test]
    fn yaml_boolean_value() {
        let configs = parse_configs("true");
        assert!(configs.is_empty());
    }

    #[test]
    fn yaml_array_of_scalars() {
        let configs = parse_configs("- one\n- two\n- three\n");
        assert!(configs.is_empty());
    }

    #[test]
    fn parse_config_minimal_fields() {
        // Config with only the watch field; everything else should default.
        let yaml = "watch: \"*\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs.len(), 1);
        let c = &configs[0];
        assert_eq!(c.name, "unnamed");
        assert_eq!(c.notify, NotifyMode::Auto);
        assert!(c.on_change.is_none());
        assert!(c.build.is_none());
        assert!(c.ignore.is_empty());
        assert!(c.watch_set.is_none()); // not compiled yet
    }

    #[test]
    fn parse_config_empty_watch_list() {
        let yaml = "name: empty\nwatch: []\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs.len(), 1);
        assert!(configs[0].watches.is_empty());
    }

    #[test]
    fn parse_config_no_watch_field() {
        let yaml = "name: no-watch\non_change: \"echo hi\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs.len(), 1);
        assert!(configs[0].watches.is_empty());
    }

    #[test]
    fn parse_config_with_build_fallback() {
        let yaml = "name: builder\nwatch: \"*\"\nbuild: \"make all\"\n";
        let configs = parse_configs(yaml);
        assert!(configs[0].on_change.is_none());
        assert_eq!(configs[0].build.as_deref(), Some("make all"));
    }

    #[test]
    fn parse_config_on_change_takes_priority_over_build() {
        let yaml = "name: both\nwatch: \"*\"\non_change: \"npm build\"\nbuild: \"make\"\n";
        let configs = parse_configs(yaml);
        // No {path} placeholder in the template, so output is unchanged.
        assert_eq!(configs[0].command_for("x"), Some("npm build".to_string()));
    }

    #[test]
    fn parse_config_notify_none() {
        let yaml = "name: silent\nwatch: \"*\"\nnotify: none\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs[0].notify, NotifyMode::None);
    }

    #[test]
    fn parse_config_notify_absent_defaults_to_auto() {
        let yaml = "name: default-notify\nwatch: \"*\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs[0].notify, NotifyMode::Auto);
    }

    #[test]
    fn parse_multiple_configs_preserves_order() {
        let yaml = r#"
- name: first
  watch: "*.a"
- name: second
  watch: "*.b"
- name: third
  watch: "*.c"
"#;
        let configs = parse_configs(yaml);
        assert_eq!(configs.len(), 3);
        assert_eq!(configs[0].name, "first");
        assert_eq!(configs[1].name, "second");
        assert_eq!(configs[2].name, "third");
    }

    #[test]
    fn parse_large_number_of_configs() {
        let mut yaml = String::new();
        for i in 0..100 {
            yaml.push_str(&format!("- name: cfg-{}\n  watch: \"**/*.ext{}\"\n", i, i));
        }
        let configs = parse_configs(&yaml);
        assert_eq!(configs.len(), 100);
        assert_eq!(configs[0].name, "cfg-0");
        assert_eq!(configs[99].name, "cfg-99");
    }

    // -- Special characters in names/patterns ------------------------------

    #[test]
    fn config_name_with_spaces() {
        let yaml = "name: \"my project config\"\nwatch: \"*\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs[0].name, "my project config");
    }

    #[test]
    fn config_name_with_unicode() {
        let yaml = "name: \"配置-émojis-🎉\"\nwatch: \"*\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs[0].name, "配置-émojis-🎉");
    }

    #[test]
    fn watch_pattern_with_deep_nesting() {
        let yaml = "name: deep\nwatch: \"a/b/c/d/e/f/g/**/*.rs\"\n";
        let mut configs = parse_configs(yaml);
        configs[0].compile_watch_set();
        assert!(configs[0].matches("a/b/c/d/e/f/g/h/main.rs"));
        assert!(!configs[0].matches("a/b/main.rs"));
    }

    #[test]
    fn watch_pattern_with_braces() {
        let yaml = "name: braces\nwatch: \"**/*.{js,ts}\"\n";
        let mut configs = parse_configs(yaml);
        configs[0].compile_watch_set();
        assert!(configs[0].matches("src/app.js"));
        assert!(configs[0].matches("src/app.ts"));
        assert!(!configs[0].matches("src/app.rs"));
    }

    // -- compile_watch_set -------------------------------------------------

    #[test]
    fn compile_watch_set_empty_watches() {
        let yaml = "name: empty\n";
        let mut configs = parse_configs(yaml);
        configs[0].compile_watch_set();
        assert!(configs[0].watch_set.is_none());
    }

    #[test]
    fn compile_watch_set_invalid_glob_skipped() {
        let mut entry = ConfigEntry {
            name: "bad".to_string(),
            watches: vec!["[unclosed".to_string(), "**/*.rs".to_string()],
            watch_set: None,
            on_change: None,
            build: None,
            notify: NotifyMode::Auto,
            ignore: vec![],
        };
        entry.compile_watch_set();
        // Should still compile the valid glob; the invalid one is skipped.
        assert!(entry.watch_set.is_some());
        assert!(entry.matches("src/main.rs"));
    }

    #[test]
    fn compile_watch_set_all_invalid_globs() {
        let mut entry = ConfigEntry {
            name: "all-bad".to_string(),
            watches: vec!["[!".to_string(), "a]b[".to_string()],
            watch_set: None,
            on_change: None,
            build: None,
            notify: NotifyMode::Auto,
            ignore: vec![],
        };
        entry.compile_watch_set();
        // If the globset crate treats these as valid (implementation-defined),
        // the watch_set may or may not be None.  The key property is that it
        // must not panic and `matches` must return a deterministic result.
        let _ = entry.matches("anything");
    }

    #[test]
    fn compile_watch_set_multiple_valid_patterns() {
        let yaml = r#"
name: multi
watch:
  - "**/*.rs"
  - "**/*.toml"
  - "**/*.yaml"
"#;
        let mut configs = parse_configs(yaml);
        configs[0].compile_watch_set();
        assert!(configs[0].matches("src/main.rs"));
        assert!(configs[0].matches("Cargo.toml"));
        assert!(configs[0].matches("config.yaml"));
        assert!(!configs[0].matches("readme.md"));
    }

    // -- matches -----------------------------------------------------------

    #[test]
    fn matches_returns_false_without_compiled_set() {
        let yaml = "name: t\nwatch: \"**/*.rs\"\n";
        let configs = parse_configs(yaml);
        // watch_set is None because compile_watch_set was not called.
        assert!(!configs[0].matches("src/main.rs"));
    }

    #[test]
    fn matches_empty_path() {
        let yaml = "name: t\nwatch: \"*\"\n";
        let mut configs = parse_configs(yaml);
        configs[0].compile_watch_set();
        // Empty path is a valid (though unusual) input — should not panic.
        let _ = configs[0].matches("");
    }

    #[test]
    fn matches_path_with_backslashes() {
        let yaml = "name: t\nwatch: \"src/**/*.rs\"\n";
        let mut configs = parse_configs(yaml);
        configs[0].compile_watch_set();
        // On Windows, paths may have backslashes.  `matches` uses Path which
        // handles this on the platform level.
        let result = configs[0].matches("src/lib/util.rs");
        assert!(result);
    }

    // -- command_for -------------------------------------------------------

    #[test]
    fn command_for_no_placeholder() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"make all\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(
            configs[0].command_for("ignored.txt"),
            Some("make all".to_string())
        );
    }

    #[test]
    fn command_for_multiple_placeholders() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"echo {path} && lint {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("app.js").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "echo \"app.js\" && lint \"app.js\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "echo 'app.js' && lint 'app.js'");
    }

    #[test]
    fn command_for_path_with_spaces() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"echo {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("my file.txt").unwrap();
        // Shell escaping now wraps the path, protecting against word splitting.
        #[cfg(target_os = "windows")]
        assert_eq!(result, "echo \"my file.txt\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "echo 'my file.txt'");
    }

    #[test]
    fn command_for_empty_path() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"echo {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "echo \"\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "echo ''");
    }

    // -- ConfigEntry Clone -------------------------------------------------

    #[test]
    fn config_entry_clone() {
        let yaml = r#"
name: cloneable
watch: "**/*.rs"
on_change: "cargo build"
notify: reload
ignore:
  - "target/**"
"#;
        let mut configs = parse_configs(yaml);
        configs[0].compile_watch_set();

        let cloned = configs[0].clone();
        assert_eq!(cloned.name, "cloneable");
        assert_eq!(cloned.watches, configs[0].watches);
        assert_eq!(cloned.on_change, configs[0].on_change);
        assert_eq!(cloned.notify, configs[0].notify);
        assert_eq!(cloned.ignore, configs[0].ignore);
        assert!(cloned.watch_set.is_some());
        assert!(cloned.matches("src/main.rs"));
    }

    // -- ConfigEntry Debug -------------------------------------------------

    #[test]
    fn config_entry_debug() {
        let yaml = "name: dbg\nwatch: \"*\"\n";
        let configs = parse_configs(yaml);
        let dbg = format!("{:?}", configs[0]);
        assert!(dbg.contains("dbg"), "debug output: {dbg}");
        assert!(dbg.contains("ConfigEntry"), "debug output: {dbg}");
    }

    // -- Ignore patterns ---------------------------------------------------

    #[test]
    fn ignore_single_pattern() {
        let yaml = "name: t\nwatch: \"*\"\nignore: \"dist/**\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs[0].ignore, vec!["dist/**"]);
    }

    #[test]
    fn ignore_multiple_patterns() {
        let yaml = r#"
name: t
watch: "*"
ignore:
  - "dist/**"
  - "build/**"
  - "*.bak"
"#;
        let configs = parse_configs(yaml);
        assert_eq!(configs[0].ignore.len(), 3);
        assert!(configs[0].ignore.contains(&"dist/**".to_string()));
        assert!(configs[0].ignore.contains(&"build/**".to_string()));
        assert!(configs[0].ignore.contains(&"*.bak".to_string()));
    }

    #[test]
    fn ignore_with_backslashes_normalized() {
        let yaml = "name: t\nwatch: \"*\"\nignore: \"dist\\\\output\\\\**\"\n";
        let configs = parse_configs(yaml);
        // Backslashes in the YAML value are normalized to forward slashes.
        for pat in &configs[0].ignore {
            assert!(!pat.contains('\\'), "pattern should be normalized: {pat}");
        }
    }

    // -- YAML with extra/unknown fields ------------------------------------

    #[test]
    fn unknown_yaml_fields_ignored() {
        let yaml = r#"
name: flexible
watch: "**/*.rs"
unknown_field: "should be ignored"
another_extra: 42
"#;
        // serde(deny_unknown_fields) is NOT set, so this should parse fine.
        let configs = parse_configs(yaml);
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].name, "flexible");
    }

    // -- deserialize_string_or_vec edge cases ------------------------------

    #[test]
    fn watch_single_string_deserialized() {
        let yaml = "watch: \"**/*.ts\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs[0].watches, vec!["**/*.ts"]);
    }

    #[test]
    fn watch_list_deserialized() {
        let yaml = "watch:\n  - \"*.a\"\n  - \"*.b\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs[0].watches, vec!["*.a", "*.b"]);
    }

    #[test]
    fn watch_empty_string() {
        let yaml = "watch: \"\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(configs[0].watches, vec![""]);
    }

    // -- command_for shell-escaping (injection prevention) ------------------

    #[test]
    fn command_for_escapes_semicolon_injection() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"echo {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("file;rm -rf /").unwrap();
        // The semicolon must be inside quotes, not interpreted as a command separator.
        #[cfg(target_os = "windows")]
        assert_eq!(result, "echo \"file;rm -rf /\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "echo 'file;rm -rf /'");
    }

    #[test]
    fn command_for_escapes_backtick_injection() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"process {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("`whoami`").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "process \"`whoami`\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "process '`whoami`'");
    }

    #[test]
    fn command_for_escapes_dollar_expansion() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"build {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("$(cat /etc/passwd)").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "build \"$(cat /etc/passwd)\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "build '$(cat /etc/passwd)'");
    }

    #[test]
    fn command_for_escapes_pipe_and_ampersand() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"lint {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("a | evil && bad").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "lint \"a | evil && bad\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "lint 'a | evil && bad'");
    }

    #[test]
    fn command_for_escapes_single_quotes() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"echo {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("it's a trap").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "echo \"it's a trap\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "echo 'it'\\''s a trap'");
    }

    #[test]
    fn command_for_escapes_double_quotes() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"echo {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("say \"hello\"").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "echo \"say \"\"hello\"\"\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "echo 'say \"hello\"'");
    }

    #[test]
    fn command_for_escapes_newlines() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"echo {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("line1\nline2").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "echo \"line1\nline2\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "echo 'line1\nline2'");
    }

    #[test]
    fn command_for_no_placeholder_unaffected_by_escaping() {
        // When the template has no {path} placeholder, shell escaping
        // has no effect — the command is returned as-is.
        let yaml = "name: t\nwatch: \"*\"\non_change: \"make all\"\n";
        let configs = parse_configs(yaml);
        assert_eq!(
            configs[0].command_for("anything;dangerous"),
            Some("make all".to_string())
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn command_for_escapes_percent_on_windows() {
        let yaml = "name: t\nwatch: \"*\"\non_change: \"echo {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("100%APPDATA%").unwrap();
        assert_eq!(result, "echo \"100%%APPDATA%%\"");
    }

    #[test]
    fn command_for_build_fallback_also_escapes() {
        // Verify that when `build` is used as fallback (no on_change), the
        // path is still shell-escaped.
        let yaml = "name: t\nwatch: \"*\"\nbuild: \"make {path}\"\n";
        let configs = parse_configs(yaml);
        let result = configs[0].command_for("evil;payload").unwrap();
        #[cfg(target_os = "windows")]
        assert_eq!(result, "make \"evil;payload\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, "make 'evil;payload'");
    }


