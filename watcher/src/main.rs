//! `quay` – a minimal, language-agnostic file watcher.
//!
//! This binary watches a directory for changes, runs configured commands, and
//! broadcasts reload / CSS-inject messages to browser clients via WebSocket.
//!
//! See the project README for full usage documentation.
//!
//! ## Module organisation
//!
//! | Module        | Responsibility                                              |
//! |---------------|-------------------------------------------------------------|
//! | `cli`         | Command-line argument definitions (clap)                    |
//! | `client`      | Embedded JavaScript hot-reload client and snippet helpers   |
//! | `command`     | Shell escaping and blocking command execution               |
//! | `config`      | YAML config parsing with serde deserialization              |
//! | `control`     | TCP control socket server and command handlers              |
//! | `debounce`    | Per-path event debouncer with periodic pruning              |
//! | `error`       | Centralised error type (`WatchdError`)                      |
//! | `filter`      | Glob-based include/exclude path filtering                   |
//! | `health`      | Worker thread health monitoring and self-healing            |
//! | `kv`          | Bounded in-memory diff store for file change tracking       |
//! | `server`      | WebSocket server and shared status-map infrastructure       |
//! | `validate`    | Input validation helpers for CLI arguments                  |
//! | `watcher`     | File-system watcher, event handling, and orchestration      |

mod cli;
mod client;
mod command;
mod config;
mod control;
mod debounce;
mod error;
mod filter;
mod health;
mod kv;
mod server;
mod validate;
mod watcher;

use clap::Parser;
use std::fs;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use prometheus::{Encoder, TextEncoder, Registry, IntGauge, IntCounter};
use std::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialise structured logging via tracing.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    // Prometheus metrics setup
    let registry = Registry::new();
    let health_gauge = IntGauge::new("quay_health", "Health status of quay").unwrap();
    let reload_counter = IntCounter::new("quay_reload_count", "Reload events").unwrap();
    registry.register(Box::new(health_gauge.clone())).unwrap();
    registry.register(Box::new(reload_counter.clone())).unwrap();
    let registry = Arc::new(Mutex::new(registry));

    let args = cli::Args::parse();
    let cmd_template = args.cmd_template.clone();
    let bind_addr = args.bind_addr.clone();

    // -----------------------------------------------------------------------
    // --print-snippet: output the client <script> tag and exit
    // -----------------------------------------------------------------------
    // This check runs before control_port computation so that port 65535
    // (which would overflow port + 1) works fine with --print-snippet.
    if args.print_snippet {
        print!("{}", client::snippet_help(args.port));
        return Ok(());
    }

    // -----------------------------------------------------------------------
    // Input validation (beyond what clap's type system enforces)
    // -----------------------------------------------------------------------
    if let Err(msg) = validate::validate_bind_addr(&bind_addr, args.expose_network) {
        error!("{msg}");
        std::process::exit(1);
    }

    if let Err(msg) = validate::validate_debounce_ms(args.debounce_ms) {
        error!("{msg}");
        std::process::exit(1);
    }

    if let Err(msg) = validate::validate_cmd_timeout_ms(args.cmd_timeout_ms) {
        error!("{msg}");
        std::process::exit(1);
    }

    validate::warn_port_issues(args.port);
    validate::validate_diff_flags(args.diff, args.diff_max_file_size);

    // The control socket lives on port + 1.  Guard against overflow so that
    // port 65535 doesn't silently wrap to 0 (debug panic / release wrap).
    let control_port = args.port.checked_add(1).unwrap_or_else(|| {
        error!(
            port = args.port,
            "port value too high; control socket requires port + 1 but {} + 1 overflows u16",
            args.port
        );
        std::process::exit(1);
    });
    let control_addr = format!("{}:{}", bind_addr, control_port);

    // -----------------------------------------------------------------------
    // Client-mode subcommands (contact a running quay instance and exit)
    // -----------------------------------------------------------------------
    if let Some(sub) = &args.subcmd {
        match sub {
            cli::SubCommand::Reload => {
                if let Err(e) = control::send_reload(&control_addr).await {
                    error!("{e}");
                    std::process::exit(1);
                }
            }
            cli::SubCommand::Status => {
                if let Err(e) = control::send_status(&control_addr).await {
                    error!("{e}");
                    std::process::exit(1);
                }
            }
            cli::SubCommand::Diff { path } => {
                if let Err(e) = control::send_diff(&control_addr, path.as_deref()).await {
                    error!("{e}");
                    std::process::exit(1);
                }
            }
        }
        return Ok(());
    }

    // -----------------------------------------------------------------------
    // Server mode
    // -----------------------------------------------------------------------
    info!(cmd = %cmd_template, "quay starting");

    // Validate and canonicalize the watch root early so the user gets a clear
    // error message rather than a confusing notify failure.
    let watch_root = {
        let raw = &args.path;
        if !raw.exists() {
            error!(path = %raw.display(), "watch path does not exist");
            std::process::exit(1);
        }
        if !raw.is_dir() {
            error!(path = %raw.display(), "watch path is not a directory");
            std::process::exit(1);
        }
        // Canonicalize to an absolute path so that notify event paths and our
        // internal comparisons (e.g. config hot-reload) are consistent.
        raw.canonicalize().unwrap_or_else(|_| raw.clone())
    };

    // Cancellation token for coordinated graceful shutdown.
    let cancel = CancellationToken::new();

    // Shared WebSocket connection counter (for status reporting).
    let connection_count = Arc::new(AtomicUsize::new(0));

    // Load configs from `quay.yaml` in the watch root.
    let cfg_path = watch_root.join("quay.yaml");
    let mut configs: Vec<config::ConfigEntry> = Vec::new();

    if cfg_path.exists() {
        match fs::read_to_string(&cfg_path) {
            Ok(text) => {
                let mut parsed = config::parse_configs(&text);
                for cfg in &mut parsed {
                    cfg.compile_watch_set();
                    info!(
                        config = %cfg.name,
                        path = %cfg_path.display(),
                        "loaded config"
                    );
                }
                configs = parsed;
            }
            Err(e) => {
    warn!(path = %cfg_path.display(), error = %e, "failed to read config file");
    eprintln!("Actionable guidance: Could not read quay.yaml. Please check file permissions and YAML syntax.");
}
        }
    } else {
        info!(
            path = %watch_root.display(),
            "no quay.yaml found; continuing with defaults"
        );
    }

    // Build global path filter from default excludes + config-level ignores.
    let extra_excludes: Vec<String> = configs
        .iter()
        .flat_map(|c| c.ignore.iter().cloned())
        .collect();
    let path_filter = filter::PathFilter::with_defaults(&extra_excludes);

    // Optionally create the diff store (enabled by --diff).
    let diff_store = if args.diff {
        let store = kv::DiffStore::new_shared(50, 500, args.diff_max_file_size);
        info!(
            max_file_size = args.diff_max_file_size,
            "diff store enabled"
        );
        Some(store)
    } else {
        None
    };

    // Broadcast channel for WebSocket messages.
    let (btx, _brx) = broadcast::channel::<String>(256);

    // Status map shared between the worker and control socket.
    let statuses = server::new_status_map(configs.iter().map(|c| c.name.clone()));

    // Bind WebSocket and control listeners before starting the watcher so that
    // failures surface immediately rather than after everything is running.
    let ws_addr = format!("{}:{}", bind_addr, args.port);
    let ws_listener = TcpListener::bind(&ws_addr)
        .await
        .map_err(|e| error::WatchdError::Bind {
            addr: ws_addr.clone(),
            source: e,
            user_message: Some("Failed to bind WebSocket server. Check address and port.".to_string()),
        })?;
    info!(addr = %ws_addr, "WebSocket server listening");

    let ctrl_listener =
        TcpListener::bind(&control_addr)
            .await
            .map_err(|e| error::WatchdError::Bind {
                addr: control_addr.clone(),
                source: e,
                user_message: None,
            })?;
    info!(addr = %control_addr, "control interface listening");

    // Log the client snippet hint so developers know how to connect.
    info!(
        port = args.port,
        "add the hot-reload client to your HTML: {}",
        client::external_script_tag("/quay-client.js", args.port)
    );

    // Spawn server tasks.
    server::spawn_ws_server(
        ws_listener,
        btx.clone(),
        cancel.clone(),
        connection_count.clone(),
        args.tls_cert.clone(), // tls_cert
        args.tls_key.clone(),  // tls_key
        args.max_connections,  // max_connections
    );
    control::spawn_control_server(
        ctrl_listener,
        btx.clone(),
        statuses.clone(),
        cancel.clone(),
        connection_count.clone(),
        diff_store.clone(),
        args.auth_token.clone(), // auth_token
        args.tls_cert.clone(),   // tls_cert
        args.tls_key.clone(),    // tls_key
        args.max_connections,    // max_connections
    );

    // Health/readiness endpoint
    let registry_clone = registry.clone();
    tokio::spawn(async move {
        let health_addr = "127.0.0.1:9090";
        let listener = TcpListener::bind(health_addr).await.unwrap();
        info!(addr = %health_addr, "health endpoint listening");
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let encoder = TextEncoder::new();
            {
                let registry = registry_clone.lock().unwrap();
                let mf = registry.gather();
                encoder.encode(&mf, &mut buffer).unwrap();
            }
            use tokio::io::AsyncWriteExt;
            let _ = socket.write_all(&buffer).await;
        }
    });

    // Start the file-system watcher and blocking event worker.
    // We keep `_watcher` alive so that `notify` continues delivering events.
    // The `worker_handle` is monitored by the health quayog.
    let (_watcher, worker_handle) = watcher::start(watcher::WatcherParams {
        watch_root: watch_root.into_boxed_path(),
        cmd_template,
        debounce_ms: args.debounce_ms,
        skip_initial_run: args.no_run_on_start,
        filter: path_filter,
        configs,
        btx,
        statuses,
        cancel: cancel.clone(),
        cmd_timeout_ms: args.cmd_timeout_ms,
        connection_count,
        diff_store,
    })?;

    // Spawn the worker health quayog.  If the worker thread terminates
    // unexpectedly (panic, channel closure), the quayog triggers a
    // coordinated shutdown so the server doesn't run in a degraded state.
    let _quayog = health::WorkerWatchdog::new(worker_handle, cancel.clone()).spawn();

    // Wait for Ctrl-C to initiate graceful shutdown.
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("received Ctrl-C; shutting down"),
        Err(e) => {
    error!(error = %e, "failed to listen for Ctrl-C");
    eprintln!("Actionable guidance: Unable to listen for Ctrl-C. Try running with proper terminal permissions.");
}
    }

    // Signal all tasks to stop.
    cancel.cancel();

    // Give background tasks a moment to clean up.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    info!("quay exited");
    Ok(())
}
