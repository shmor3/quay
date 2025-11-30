use base64::Engine;
use clap::{Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use globset::{Glob, GlobSet, GlobSetBuilder};
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher};
use serde_json;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::fs;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt, BufReader};
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

    /// (Deprecated) Patterns file path. Not used when `hotreload.yaml` is present.
    // #[arg(long = "patterns-file")]
    // patterns_file: Option<PathBuf>,

    /// Debounce in milliseconds
    #[arg(long = "debounce-ms", default_value_t = 200)]
    debounce_ms: u64,

    /// Websocket port
    #[arg(long = "port", default_value_t = 3012)]
    port: u16,

    /// Do not run the command on startup
    #[arg(long = "no-run-on-start")]
    no_run_on_start: bool,

    /// Path to watch (and where to look for `hotreload.yaml`). Defaults to current directory.
    #[arg(short = 'p', long = "path", default_value = ".")]
    path: PathBuf,

    /// Subcommands: reload or status (if omitted, run the watcher server)
    #[command(subcommand)]
    subcmd: Option<SubCommand>,
}

#[derive(Subcommand, Debug)]
enum SubCommand {
    /// Force a reload (send reload to connected clients)
    Reload,
    /// Query status of watched configs
    Status,
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

/// Config file definition (parsed from YAML files in configs/)
#[derive(Debug, Clone)]
struct ConfigFile {
    name: String,
    watches: Vec<String>,   // glob patterns
    watch_set: Option<GlobSet>,
    on_change: Option<String>,
    build: Option<String>,
    notify: Option<String>, // 'auto', 'reload', 'inject-css', 'none'
    ignore: Vec<String>,    // additional exclude patterns defined in the config file
}

impl ConfigFile {
    fn matches(&self, path: &str) -> bool {
        if let Some(gs) = &self.watch_set {
            return gs.is_match(Path::new(path));
        }
        // if no watch patterns defined, do not match
        false
    }
}



/// Parse adapter pseudo-language (simple key: value per line)
fn parse_config(s: &str) -> Result<ConfigFile, &'static str> {
    // Parse a YAML configuration file into ConfigFile struct.
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
        // read optional ignore list
        let mut ignore: Vec<String> = Vec::new();
        if let Some(iv) = val.get("ignore") {
            if iv.is_array() {
                for item in iv.as_array().unwrap() {
                    if let Some(pat) = item.as_str() {
                        ignore.push(normalize_pattern(pat));
                    }
                }
            } else if let Some(pat) = iv.as_str() {
                ignore.push(normalize_pattern(pat));
            }
        }
        return Ok(ConfigFile { name, watches, watch_set: None, on_change, build, notify, ignore });
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
    Ok(ConfigFile { name, watches, watch_set: None, on_change, build, notify, ignore: Vec::new() })
}

/// Parse a hotreload.yaml that may contain either a single config mapping or a sequence of configs.
fn parse_configs(s: &str) -> Vec<ConfigFile> {
    let mut out: Vec<ConfigFile> = Vec::new();
    if let Ok(val) = serde_yaml::from_str::<serde_json::Value>(s) {
        if val.is_array() {
            for item in val.as_array().unwrap() {
                // reuse YAML mapping parsing via serde_json::Value stringification
                if let Ok(text) = serde_json::to_string(item) {
                    if let Ok(cfg) = parse_config(&text) {
                        out.push(cfg);
                    }
                }
            }
            return out;
        } else {
            if let Ok(cfg) = parse_config(&s) {
                out.push(cfg);
                return out;
            }
        }
    }
    // fallback: try legacy single config parser
    if let Ok(cfg) = parse_config(s) {
        out.push(cfg);
    }
    out
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let cmd_template = args.cmd_template.clone();
    println!(
        "hotreload-watcher: running command on changes: {}",
        cmd_template
    );

    // If a subcommand was provided, act as a control client and exit
    if let Some(sub) = &args.subcmd {
        let control_addr = format!("127.0.0.1:{}", args.port + 1);
        match sub {
            SubCommand::Reload => {
                match TcpStream::connect(&control_addr).await {
                    Ok(mut s) => {
                        let req = serde_json::json!({"cmd":"reload"}).to_string() + "\n";
                        if let Err(e) = s.write_all(req.as_bytes()).await {
                            eprintln!("failed to send reload: {}", e);
                        }
                        let mut buf = Vec::new();
                        let _ = s.read_to_end(&mut buf).await;
                        println!("{}", String::from_utf8_lossy(&buf));
                    }
                    Err(e) => eprintln!("failed to connect to control socket at {}: {}", control_addr, e),
                }
                return Ok(());
            }
            SubCommand::Status => {
                match TcpStream::connect(&control_addr).await {
                    Ok(mut s) => {
                        let req = serde_json::json!({"cmd":"status"}).to_string() + "\n";
                        if let Err(e) = s.write_all(req.as_bytes()).await {
                            eprintln!("failed to send status request: {}", e);
                        }
                        let mut buf = Vec::new();
                        let _ = s.read_to_end(&mut buf).await;
                        println!("{}", String::from_utf8_lossy(&buf));
                    }
                    Err(e) => eprintln!("failed to connect to control socket at {}: {}", control_addr, e),
                }
                return Ok(());
            }
        }
    }

    // Broadcast channel for websocket messages
    let (btx, _brx) = broadcast::channel::<String>(128);

    // Determine watch root (where to look for `hotreload.yaml`) and load configs
    let watch_root = args.path.clone();
    let cfg_path = watch_root.join("hotreload.yaml");
    let mut configs: Vec<ConfigFile> = Vec::new();
    if cfg_path.exists() {
        match fs::read_to_string(&cfg_path) {
            Ok(text) => {
                let parsed = parse_configs(&text);
                for mut cfg in parsed {
                    // compile watch globs
                    if !cfg.watches.is_empty() {
                        let mut b = GlobSetBuilder::new();
                        for pat in &cfg.watches {
                            if let Ok(g) = Glob::new(pat) {
                                b.add(g);
                            }
                        }
                        if let Ok(gs) = b.build() {
                            cfg.watch_set = Some(gs);
                        }
                    }
                    println!("hotreload-watcher: loaded config '{}' from {}", cfg.name, cfg_path.display());
                    configs.push(cfg);
                }
            }
            Err(e) => eprintln!("hotreload-watcher: failed to read {}: {}", cfg_path.display(), e),
        }
    } else {
        println!("hotreload-watcher: no hotreload.yaml found at {}; continuing with defaults", watch_root.display());
    }

    // Build include/exclude globsets using default excludes plus any ignore entries from hotreload.yaml
    let mut exc = default_exclude_patterns();
    // merge ignore lists from all configs
    for cfg in &configs {
        for ig in &cfg.ignore {
            if !exc.contains(ig) {
                exc.push(ig.clone());
            }
        }
    }
    let inc: Vec<String> = Vec::new();
    let (include_set, exclude_set) = build_globsets(&inc, &exc);

    // Start websocket server
    let ws_addr = format!("127.0.0.1:{}", args.port);
    let listener = TcpListener::bind(&ws_addr).await?;
    println!(
        "hotreload-watcher: websocket server listening on ws://{}",
        ws_addr
    );

    // Status map for configs (shared between worker and control interface)
    let statuses: Arc<Mutex<std::collections::HashMap<String, String>>> = Arc::new(Mutex::new(std::collections::HashMap::new()));
    for cfg in &configs {
        statuses.lock().unwrap().insert(cfg.name.clone(), "up to date".to_string());
    }

    // Spawn a simple control TCP interface on port+1 for CLI commands (reload/status)
    let control_addr = format!("127.0.0.1:{}", args.port + 1);
    let ctrl_listener = TcpListener::bind(&control_addr).await?;
    println!("hotreload-watcher: control interface listening on tcp://{}", control_addr);
    let btx_ctrl = btx.clone();
    let statuses_ctrl = statuses.clone();
    tokio::spawn(async move {
        loop {
            match ctrl_listener.accept().await {
                Ok((mut socket, _addr)) => {
                    // read a single JSON command line (terminated by newline)
                    let mut buf: Vec<u8> = Vec::new();
                    {
                        let mut reader = BufReader::new(&mut socket);
                        match reader.read_until(b'\n', &mut buf).await {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("control read error: {}", e);
                                continue;
                            }
                        }
                    }

                    let txt = String::from_utf8_lossy(&buf).to_string();
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                        if let Some(cmd) = v.get("cmd").and_then(|c| c.as_str()) {
                            match cmd {
                                "reload" => {
                                    // send broadcast
                                    let _ = btx_ctrl.send(serde_json::json!({"type":"reload"}).to_string());
                                    // update statuses quickly while holding the lock briefly
                                    {
                                        let mut s = statuses_ctrl.lock().unwrap();
                                        let keys: Vec<_> = s.keys().cloned().collect();
                                        for k in &keys {
                                            s.insert(k.clone(), "reloading".to_string());
                                        }
                                        for k in &keys {
                                            s.insert(k.clone(), "up to date".to_string());
                                        }
                                    }
                                    let _ = socket.write_all(b"{\"status\":\"ok\"}\n").await;
                                }
                                "status" => {
                                    let reply = {
                                        let s = statuses_ctrl.lock().unwrap();
                                        let mut out = serde_json::Map::new();
                                        let mut arr = Vec::new();
                                        for (name, st) in s.iter() {
                                            let mut m = serde_json::Map::new();
                                            m.insert("name".to_string(), serde_json::Value::String(name.clone()));
                                            m.insert("status".to_string(), serde_json::Value::String(st.clone()));
                                            arr.push(serde_json::Value::Object(m));
                                        }
                                        out.insert("configs".to_string(), serde_json::Value::Array(arr));
                                        serde_json::Value::Object(out).to_string()
                                    };
                                    let _ = socket.write_all(reply.as_bytes()).await;
                                    let _ = socket.write_all(b"\n").await;
                                }
                                _ => {
                                    let _ = socket.write_all(b"{\"error\":\"unknown command\"}\n").await;
                                }
                            }
                        } else {
                            let _ = socket.write_all(b"{\"error\":\"bad request\"}\n").await;
                        }
                    } else {
                        let _ = socket.write_all(b"{\"error\":\"invalid json\"}\n").await;
                    }
                }
                Err(e) => {
                    eprintln!("control accept error: {}", e);
                    break;
                }
            }
        }
    });

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

    // watch the configured path
    watcher.watch(&watch_root, RecursiveMode::Recursive)?;

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
    let configs_worker = configs.clone();
    let statuses_worker = statuses.clone();

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

                        // Config handling: check each loaded config to see if it matches this path
                        let mut handled_by_config = false;
                        for cfg in &configs_worker {
                            if cfg.matches(&pnorm) {
                                handled_by_config = true;
                                println!("hotreload-watcher: config '{}' matched {}", cfg.name, pnorm);
                                // set status to reloading
                                {
                                    let mut s = statuses_worker.lock().unwrap();
                                    s.insert(cfg.name.clone(), "reloading".to_string());
                                }

                                if let Some(cmd_tpl) = &cfg.on_change {
                                    let cmd = cmd_tpl.replace("{path}", &pnorm);
                                    println!("hotreload-watcher: config '{}' running: {}", cfg.name, cmd);
                                    run_command_blocking(&cmd);
                                } else if let Some(build_cmd) = &cfg.build {
                                    let cmd = build_cmd.replace("{path}", &pnorm);
                                    println!("hotreload-watcher: config '{}' building: {}", cfg.name, cmd);
                                    run_command_blocking(&cmd);
                                }

                                // determine notification behavior
                                let notify_mode = cfg.notify.as_deref().unwrap_or("auto");
                                match notify_mode {
                                    "inject-css" => {
                                        if let Ok(content) = std::fs::read(&path) {
                                            let encoded = base64::engine::general_purpose::STANDARD.encode(&content);
                                            let msg = serde_json::json!({"type":"inject-css","path":pnorm,"content":encoded}).to_string();
                                            let _ = btx_worker.send(msg);
                                            println!("hotreload-watcher: config '{}' broadcast inject-css for {}", cfg.name, pnorm);
                                        }
                                    }
                                    "reload" => {
                                        let msg = serde_json::json!({"type":"reload"}).to_string();
                                        let _ = btx_worker.send(msg);
                                        println!("hotreload-watcher: config '{}' broadcast reload for {}", cfg.name, pnorm);
                                    }
                                    "auto" | _ => {
                                        // auto: reuse default extension-based behavior
                                        if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
                                            if ext.eq_ignore_ascii_case("css") {
                                                if let Ok(content) = std::fs::read(&path) {
                                                    let encoded = base64::engine::general_purpose::STANDARD.encode(&content);
                                                    let msg = serde_json::json!({"type":"inject-css","path":pnorm,"content":encoded}).to_string();
                                                    let _ = btx_worker.send(msg);
                                                    println!("hotreload-watcher: config '{}' broadcast inject-css for {}", cfg.name, pnorm);
                                                }
                                            } else {
                                                let msg = serde_json::json!({"type":"reload"}).to_string();
                                                let _ = btx_worker.send(msg);
                                                println!("hotreload-watcher: config '{}' broadcast reload for {}", cfg.name, pnorm);
                                            }
                                        } else {
                                            let msg = serde_json::json!({"type":"reload"}).to_string();
                                            let _ = btx_worker.send(msg);
                                            println!("hotreload-watcher: config '{}' broadcast reload for {}", cfg.name, pnorm);
                                        }
                                    }
                                }
                                // mark status up to date after handling
                                {
                                    let mut s = statuses_worker.lock().unwrap();
                                    s.insert(cfg.name.clone(), "up to date".to_string());
                                }
                                // continue to check other configs (multiple configs may apply)
                            }
                        }

                        if handled_by_config {
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
        let v = parse_configs(s);
        assert_eq!(v.len(), 1);
        let a = &v[0];
        assert_eq!(a.name, "demo");
        assert_eq!(a.watches.len(), 1);
        assert_eq!(a.on_change.as_deref(), Some("echo changed {path}"));
        assert_eq!(a.notify.as_deref(), Some("auto"));
    }

    #[test]
    fn test_parse_config_yaml() {
        let s = "name: yamldemo\nwatch:\n  - \"**/*.txt\"\non_change: \"echo yaml {path}\"\nnotify: reload\nignore:\n  - \"target/**\"";
        let v = parse_configs(s);
        assert_eq!(v.len(), 1);
        let a = &v[0];
        assert_eq!(a.name, "yamldemo");
        assert_eq!(a.watches.len(), 1);
        assert_eq!(a.on_change.as_deref(), Some("echo yaml {path}"));
        assert_eq!(a.notify.as_deref(), Some("reload"));
        assert_eq!(a.ignore.len(), 1);
    }

    #[test]
    fn test_parse_multiple_configs() {
        let s = "- name: a\n  watch:\n    - \"**/*.js\"\n- name: b\n  watch:\n    - \"**/*.css\"";
        let v = parse_configs(s);
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].name, "a");
        assert_eq!(v[1].name, "b");
    }
}
