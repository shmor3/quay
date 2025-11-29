use base64::Engine;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use globset::{Glob, GlobSet, GlobSetBuilder};
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher};
use serde_json;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::channel;
use std::time::Duration;
use std::fs;
use std::env;
use std::sync::OnceLock;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

// (No scripting helpers when using YAML adapter configs)

/// Simple hotreload watcher: language-agnostic incremental builds and websocket notifications
#[derive(Parser, Debug)]
#[command(name = "hotreload-watcher")]
struct Args {
    /// Command template to run on changes. Use {path} to substitute the changed file path.
    #[arg(default_value = "echo files changed")]
    cmd_template: String,

    /// Patterns file path. Lines starting with '+' are includes, '-' are excludes. Default: .hotreloadignore in repo root
    #[arg(long = "patterns-file")]
    patterns_file: Option<PathBuf>,

    /// Debounce in milliseconds
    #[arg(long = "debounce-ms", default_value_t = 200)]
    debounce_ms: u64,

    /// Websocket port
    #[arg(long = "port", default_value_t = 3012)]
    port: u16,

    /// Do not run the command on startup
    #[arg(long = "no-run-on-start")]
    no_run_on_start: bool,

    /// Directory to load adapters from. If not provided, watcher will search common locations.
    #[arg(long = "adapters-dir")]
    adapters_dir: Option<PathBuf>,
}

fn run_command_blocking(cmd: &str) {
    #[cfg(target_os = "windows")]
    let mut c = Command::new("cmd");
    #[cfg(target_os = "windows")]
    let c = c.args(["/C", cmd]).spawn();

    #[cfg(not(target_os = "windows"))]
    let mut c = Command::new("sh");
    #[cfg(not(target_os = "windows"))]
    let c = c.args(["-c", cmd]).spawn();

    match c {
        Ok(mut child) => {
            let _ = child.wait();
        }
        Err(e) => eprintln!("Failed to spawn command '{}': {}", cmd, e),
    }
}

/// Read patterns file and separate into include and exclude lists.
/// Lines starting with '+' are includes, '-' are excludes. Lines starting with '#' are comments.
fn read_patterns(path: &Path) -> Result<(Vec<String>, Vec<String>), std::io::Error> {
    let s = std::fs::read_to_string(path)?;
    let mut inc = Vec::new();
    let mut exc = Vec::new();
    for raw in s.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix('+') {
            inc.push(normalize_pattern(rest.trim()));
            continue;
        }
        if let Some(rest) = line.strip_prefix('-') {
            exc.push(normalize_pattern(rest.trim()));
            continue;
        }
        // default to include if no prefix
        inc.push(normalize_pattern(line));
    }
    Ok((inc, exc))
}

/// Build GlobSet objects from pattern lists. Returns (include_set, exclude_set) where each is Option<GlobSet>.
fn build_globsets(inc: &Vec<String>, exc: &Vec<String>) -> (Option<GlobSet>, Option<GlobSet>) {
    let mut inc_set: Option<GlobSet> = None;
    let mut exc_set: Option<GlobSet> = None;

    if !inc.is_empty() {
        let mut b = GlobSetBuilder::new();
        for p in inc {
            if let Ok(g) = Glob::new(p) {
                b.add(g);
            } else {
                eprintln!("hotreload-watcher: invalid include glob: {}", p);
            }
        }
        if let Ok(gs) = b.build() {
            inc_set = Some(gs);
        }
    }

    if !exc.is_empty() {
        let mut b = GlobSetBuilder::new();
        for p in exc {
            if let Ok(g) = Glob::new(p) {
                b.add(g);
            } else {
                eprintln!("hotreload-watcher: invalid exclude glob: {}", p);
            }
        }
        if let Ok(gs) = b.build() {
            exc_set = Some(gs);
        }
    }

    (inc_set, exc_set)
}

/// Decide whether a given path should be handled according to include/exclude sets.
/// - If include_set is Some, the path must match at least one include pattern.
/// - If exclude_set matches the path, it is excluded.
fn match_patterns(
    path: &str,
    include_set: Option<&GlobSet>,
    exclude_set: Option<&GlobSet>,
) -> bool {
    let p = Path::new(path);
    if let Some(exc) = exclude_set {
        if exc.is_match(p) {
            return false;
        }
    }
    if let Some(inc) = include_set {
        return inc.is_match(p);
    }
    // no include specified => default include
    true
}

/// Normalize a filesystem path string to use forward slashes
fn normalize_path(p: &str) -> String {
    p.replace('\\', "/")
}

/// Normalize a pattern (convert backslashes to forward slashes)
fn normalize_pattern(p: &str) -> String {
    p.replace('\\', "/")
}

/// Default exclude patterns to ignore common directories
fn default_exclude_patterns() -> Vec<String> {
    vec![
        "target/**".to_string(),
        ".git/**".to_string(),
        "node_modules/**".to_string(),
        "**/*.tmp".to_string(),
        "**/*.swp".to_string(),
        "**/.DS_Store".to_string(),
    ]
}

/// Adapter definition (parsed from pseudo-language files in adapters/)
#[derive(Debug, Clone)]
struct Adapter {
    name: String,
    watches: Vec<String>,   // glob patterns
    watch_set: Option<GlobSet>,
    on_change: Option<String>,
    build: Option<String>,
    notify: Option<String>, // 'auto', 'reload', 'inject-css', 'none'
    // scripting removed: adapters are pure YAML configs
}

impl Adapter {
    fn matches(&self, path: &str) -> bool {
        if let Some(gs) = &self.watch_set {
            return gs.is_match(Path::new(path));
        }
        // if no watch patterns defined, do not match
        false
    }
}

/// Load adapter files from `watcher/adapters` directory (relative to CWD)
fn load_adapters(dir: &Path) -> Vec<Adapter> {
    let mut adapters = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_file() {
                if let Ok(text) = fs::read_to_string(&p) {
                    if let Ok(mut adapter) = parse_adapter(&text) {
                        // compile watch globs
                        if !adapter.watches.is_empty() {
                            let mut b = GlobSetBuilder::new();
                            for pat in &adapter.watches {
                                if let Ok(g) = Glob::new(pat) {
                                    b.add(g);
                                }
                            }
                            if let Ok(gs) = b.build() {
                                adapter.watch_set = Some(gs);
                            }
                        }
                        adapters.push(adapter);
                    }
                }
            }
        }
    }
    adapters
}

/// Find adapters directory using optional preferred path or a set of common locations.
fn find_adapters_dir(preferred: Option<PathBuf>) -> PathBuf {
    if let Some(p) = preferred {
        if p.exists() {
            return p;
        }
    }

    // Common candidate locations relative to current working dir
    let candidates = vec![
        PathBuf::from("watcher/adapters"),
        PathBuf::from("adapters"),
        PathBuf::from("../watcher/adapters"),
        PathBuf::from("./watcher/adapters"),
    ];

    for c in candidates {
        if c.exists() {
            return c;
        }
    }

    // fallback: use watcher/adapters even if it doesn't exist
    PathBuf::from("watcher/adapters")
}

/// Parse adapter pseudo-language (simple key: value per line)
fn parse_adapter(s: &str) -> Result<Adapter, &'static str> {
    // New adapter format: YAML configuration file. Try parsing the entire file as YAML.
    if let Ok(val) = serde_yaml::from_str::<serde_json::Value>(s) {
        let name = val.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()).unwrap_or_else(|| "unnamed".to_string());
        let mut watches: Vec<String> = Vec::new();
        if let Some(wv) = val.get("watch") {
            if wv.is_array() {
                for item in wv.as_array().unwrap() {
                    if let Some(pat) = item.as_str() {
                        watches.push(normalize_pattern(pat));
                    }
                }
            } else if let Some(pat) = wv.as_str() {
                watches.push(normalize_pattern(pat));
            }
        }
        let on_change = val.get("on_change").and_then(|v| v.as_str()).map(|s| s.to_string());
        let build = val.get("build").and_then(|v| v.as_str()).map(|s| s.to_string());
        let notify = val.get("notify").and_then(|v| v.as_str()).map(|s| s.to_string());
        return Ok(Adapter { name, watches, watch_set: None, on_change, build, notify });
    }

    // Fallback: legacy key:value lines (keep compatibility)
    let mut name = None;
    let mut watches = Vec::new();
    let mut on_change = None;
    let mut build = None;
    let mut notify = None;

    for raw in s.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim();
            let val = v.trim();
            match key {
                "name" => name = Some(val.to_string()),
                "watch" => watches.push(normalize_pattern(val)),
                "on_change" => on_change = Some(val.to_string()),
                "build" => build = Some(val.to_string()),
                "notify" => notify = Some(val.to_string()),
                _ => {}
            }
        }
    }
    let name = name.unwrap_or_else(|| "unnamed".to_string());
    Ok(Adapter { name, watches, watch_set: None, on_change, build, notify })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let cmd_template = args.cmd_template.clone();
    println!(
        "hotreload-watcher: running command on changes: {}",
        cmd_template
    );

    // Broadcast channel for websocket messages
    let (btx, _brx) = broadcast::channel::<String>(128);

    // Load include/exclude patterns
    let patterns_path = args
        .patterns_file
        .clone()
        .unwrap_or_else(|| PathBuf::from(".hotreloadignore"));

    let (include_set, exclude_set) = match read_patterns(&patterns_path) {
        Ok((inc, exc)) => {
            // Merge with sensible defaults for excludes
            let mut exc = exc;
            for d in default_exclude_patterns() {
                if !exc.contains(&d) {
                    exc.push(d);
                }
            }
            build_globsets(&inc, &exc)
        }
        Err(_) => {
            // No user patterns; use only defaults for exclude
            let inc: Vec<String> = Vec::new();
            let exc = default_exclude_patterns();
            build_globsets(&inc, &exc)
        }
    };

    // Start websocket server
    let ws_addr = format!("127.0.0.1:{}", args.port);
    let listener = TcpListener::bind(&ws_addr).await?;
    println!(
        "hotreload-watcher: websocket server listening on ws://{}",
        ws_addr
    );

    // Spawn task to accept websocket clients
    let btx_accept = btx.clone();
    // spawn a ctrl-c handler to exit gracefully
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            eprintln!("hotreload-watcher: failed to listen for ctrl-c: {}", e);
        }
        println!("hotreload-watcher: received ctrl-c, shutting down");
        // exit process; this will terminate threads
        std::process::exit(0);
    });

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let peer_btx = btx_accept.clone();
                    tokio::spawn(async move {
                        match tokio_tungstenite::accept_async(stream).await {
                            Ok(ws_stream) => {
                                println!("hotreload-watcher: ws client connected: {}", addr);
                                let (mut ws_tx, mut ws_rx) = ws_stream.split();
                                let mut rx = peer_btx.subscribe();

                                // task to forward broadcast -> ws
                                let send_task = tokio::spawn(async move {
                                    while let Ok(msg) = rx.recv().await {
                                        if ws_tx.send(Message::Text(msg)).await.is_err() {
                                            break;
                                        }
                                    }
                                });

                                // read incoming (and drop) to detect disconnects
                                while let Some(Ok(_msg)) = ws_rx.next().await {
                                    // ignore client messages
                                }

                                // client disconnected; cancel send_task
                                send_task.abort();
                                println!("hotreload-watcher: ws client disconnected: {}", addr);
                            }
                            Err(e) => eprintln!("ws accept error: {}", e),
                        }
                    });
                }
                Err(e) => {
                    eprintln!("listener accept error: {}", e);
                    break;
                }
            }
        }
    });

    // Create a std mpsc channel and a notify watcher that sends events into it
    let (tx, rx) = channel::<NotifyResult<Event>>();
    let mut watcher: RecommendedWatcher = RecommendedWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        Config::default(),
    )?;

    watcher.watch(Path::new("."), RecursiveMode::Recursive)?;

    // Optionally run the command once at startup (unless disabled)
    if !args.no_run_on_start {
        let cmd_clone = cmd_template.clone();
        std::thread::spawn(move || run_command_blocking(&cmd_clone));
    }

    // Process incoming notify events in a blocking thread; this thread will perform
    // small per-file actions and broadcast messages to websocket clients.
    let btx_worker = btx.clone();
    let cmd_worker = cmd_template.clone();
    let include_set = include_set;
    let exclude_set = exclude_set;
    let debounce_ms = args.debounce_ms;
        // determine adapters directory (allow override via CLI)
        let adapters_dir = find_adapters_dir(args.adapters_dir.clone());
        let adapters = load_adapters(&adapters_dir);
        println!(
            "hotreload-watcher: loaded {} adapters from {}",
            adapters.len(),
            adapters_dir.display()
        );

        // spawn worker thread
        std::thread::spawn(move || {
        // no embedded scripting engine; adapters are configured via YAML files
        // simple debounce map: remember last event time per path
        use std::collections::HashMap;
        use std::time::Instant;
        let mut last_seen: HashMap<String, Instant> = HashMap::new();
        let debounce = Duration::from_millis(debounce_ms);

        while let Ok(res) = rx.recv() {
            match res {
                Ok(event) => {
                    let paths = event.paths;
                    for path in paths {
                        let pstr = path.to_string_lossy().to_string();
                        let now = Instant::now();
                        let do_handle = match last_seen.get(&pstr) {
                            Some(t) => now.duration_since(*t) > debounce,
                            None => true,
                        };
                        last_seen.insert(pstr.clone(), now);

                        if !do_handle {
                            continue;
                        }

                        // normalize path for matching (convert backslashes to forward slashes)
                        let pnorm = normalize_path(&pstr);

                        // pattern filtering: first check include/exclude rules
                        if !match_patterns(&pnorm, include_set.as_ref(), exclude_set.as_ref()) {
                            // skipped by patterns
                            println!("hotreload-watcher: skipped by patterns: {}", pstr);
                            continue;
                        }

                        // Adapter handling: if an adapter matches this path, run adapter logic
                        let mut handled_by_adapter = false;
                        for adapter in &adapters {
                            if adapter.matches(&pnorm) {
                                handled_by_adapter = true;
                                println!("hotreload-watcher: adapter '{}' matched {}", adapter.name, pnorm);
                                if let Some(cmd_tpl) = &adapter.on_change {
                                    let cmd = cmd_tpl.replace("{path}", &pnorm);
                                    println!("hotreload-watcher: adapter '{}' running: {}", adapter.name, cmd);
                                    run_command_blocking(&cmd);
                                } else if let Some(build_cmd) = &adapter.build {
                                    let cmd = build_cmd.replace("{path}", &pnorm);
                                    println!("hotreload-watcher: adapter '{}' building: {}", adapter.name, cmd);
                                    run_command_blocking(&cmd);
                                }

                                // determine notification behavior
                                let notify_mode = adapter.notify.as_deref().unwrap_or("auto");
                                match notify_mode {
                                    "inject-css" => {
                                        if let Ok(content) = std::fs::read(&path) {
                                            let encoded = base64::engine::general_purpose::STANDARD.encode(&content);
                                            let msg = serde_json::json!({"type":"inject-css","path":pnorm,"content":encoded}).to_string();
                                            let _ = btx_worker.send(msg);
                                            println!("hotreload-watcher: adapter '{}' broadcast inject-css for {}", adapter.name, pnorm);
                                        }
                                    }
                                    "reload" => {
                                        let msg = serde_json::json!({"type":"reload"}).to_string();
                                        let _ = btx_worker.send(msg);
                                        println!("hotreload-watcher: adapter '{}' broadcast reload for {}", adapter.name, pnorm);
                                    }
                                    "auto" | _ => {
                                        // auto: reuse default extension-based behavior
                                        if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
                                            if ext.eq_ignore_ascii_case("css") {
                                                if let Ok(content) = std::fs::read(&path) {
                                                    let encoded = base64::engine::general_purpose::STANDARD.encode(&content);
                                                    let msg = serde_json::json!({"type":"inject-css","path":pnorm,"content":encoded}).to_string();
                                                    let _ = btx_worker.send(msg);
                                                    println!("hotreload-watcher: adapter '{}' broadcast inject-css for {}", adapter.name, pnorm);
                                                }
                                            } else {
                                                let msg = serde_json::json!({"type":"reload"}).to_string();
                                                let _ = btx_worker.send(msg);
                                                println!("hotreload-watcher: adapter '{}' broadcast reload for {}", adapter.name, pnorm);
                                            }
                                        } else {
                                            let msg = serde_json::json!({"type":"reload"}).to_string();
                                            let _ = btx_worker.send(msg);
                                            println!("hotreload-watcher: adapter '{}' broadcast reload for {}", adapter.name, pnorm);
                                        }
                                    }
                                }
                                // continue to next adapter (multiple adapters may run)
                            }
                        }

                        if handled_by_adapter {
                            continue;
                        }

                        // decide action by extension (fallback behavior)
                        if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
                            if ext.eq_ignore_ascii_case("css") {
                                if let Ok(content) = std::fs::read(&path) {
                                    // base64-encode content bytes to safely transport it
                                    let encoded = base64::engine::general_purpose::STANDARD
                                        .encode(&content);
                                    let msg = serde_json::json!({
                                        "type": "inject-css",
                                        "path": pnorm,
                                        "content": encoded,
                                    })
                                    .to_string();

                                    let _ = btx_worker.send(msg);
                                    println!("hotreload-watcher: broadcast inject-css for {}", pnorm);
                                    continue;
                                }
                            }

                            if ext.eq_ignore_ascii_case("html") {
                                let msg = serde_json::json!({ "type": "reload" }).to_string();
                                let _ = btx_worker.send(msg);
                                println!("hotreload-watcher: broadcast reload for {}", pnorm);
                                continue;
                            }
                        }

                        // fallback: run the configured command template for this path
                        // replace {path} token in the template
                        let cmd = cmd_worker.replace("{path}", &pnorm);
                        println!("hotreload-watcher: running: {}", cmd);
                        run_command_blocking(&cmd);

                        // notify clients to reload after command
                        let msg = serde_json::json!({ "type": "reload" }).to_string();
                        let _ = btx_worker.send(msg);
                    }
                }
                Err(e) => eprintln!("watch error recv: {:?}", e),
            }
        }
    });

    // Keep the main task alive; websocket accept loop and worker threads do the work.
    // Await on a never-ending future.
    futures::future::pending::<()>().await;

    // unreachable
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path_and_pattern() {
        assert_eq!(normalize_path("a\\b\\c.txt"), "a/b/c.txt");
        assert_eq!(normalize_pattern("a\\b\\**"), "a/b/**");
    }

    #[test]
    fn test_default_excludes() {
        let defs = default_exclude_patterns();
        assert!(defs.iter().any(|p| p.contains("target")));
    }

    #[test]
    fn test_match_patterns_defaults() {
        let (inc, exc) = (Vec::<String>::new(), default_exclude_patterns());
        let (inc_set, exc_set) = build_globsets(&inc, &exc);
        // path in target should be excluded
        assert!(!match_patterns(
            "target/foo.rs",
            inc_set.as_ref(),
            exc_set.as_ref()
        ));
        // path not excluded should be included
        assert!(match_patterns(
            "src/main.rs",
            inc_set.as_ref(),
            exc_set.as_ref()
        ));
    }

    #[test]
    fn test_parse_adapter() {
        let s = "name: demo\nwatch: **/*.txt\non_change: echo changed {path}\nnotify: auto";
        let a = parse_adapter(s).expect("parse");
        assert_eq!(a.name, "demo");
        assert_eq!(a.watches.len(), 1);
        assert_eq!(a.on_change.as_deref(), Some("echo changed {path}"));
        assert_eq!(a.notify.as_deref(), Some("auto"));
    }

    #[test]
    fn test_parse_adapter_json_block() {
        let s = "name: yamldemo\nwatch:\n  - \"**/*.txt\"\non_change: \"echo yaml {path}\"\nnotify: reload";
        let a = parse_adapter(s).expect("parse yaml");
        assert_eq!(a.name, "yamldemo");
        assert_eq!(a.watches.len(), 1);
        assert_eq!(a.on_change.as_deref(), Some("echo yaml {path}"));
        assert_eq!(a.notify.as_deref(), Some("reload"));
    }
}
