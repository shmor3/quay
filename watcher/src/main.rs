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
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

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
        Ok((inc, exc)) => build_globsets(&inc, &exc),
        Err(_) => (None, None),
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
    std::thread::spawn(move || {
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

                        // pattern filtering: first check include/exclude rules
                        if !match_patterns(&pstr, include_set.as_ref(), exclude_set.as_ref()) {
                            // skipped by patterns
                            println!("hotreload-watcher: skipped by patterns: {}", pstr);
                            continue;
                        }

                        // decide action by extension
                        if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
                            if ext.eq_ignore_ascii_case("css") {
                                if let Ok(content) = std::fs::read_to_string(&path) {
                                    // base64-encode content to safely transport it
                                    let encoded = base64::engine::general_purpose::STANDARD
                                        .encode(content.as_bytes());
                                    let msg = serde_json::json!({
                                        "type": "inject-css",
                                        "path": pstr,
                                        "content": encoded,
                                    })
                                    .to_string();

                                    let _ = btx_worker.send(msg);
                                    println!(
                                        "hotreload-watcher: broadcast inject-css for {}",
                                        pstr
                                    );
                                    continue;
                                }
                            }

                            if ext.eq_ignore_ascii_case("html") {
                                let msg = serde_json::json!({ "type": "reload" }).to_string();
                                let _ = btx_worker.send(msg);
                                println!("hotreload-watcher: broadcast reload for {}", pstr);
                                continue;
                            }
                        }

                        // fallback: run the configured command template for this path
                        // replace {path} token in the template
                        let cmd = cmd_worker.replace("{path}", &pstr);
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
                inc.push(rest.trim().to_string());
                continue;
            }
            if let Some(rest) = line.strip_prefix('-') {
                exc.push(rest.trim().to_string());
                continue;
            }
            // default to include if no prefix
            inc.push(line.to_string());
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

    // Keep the main task alive; websocket accept loop and worker threads do the work.
    // Await on a never-ending future.
    futures::future::pending::<()>().await;

    // unreachable
    Ok(())
}
