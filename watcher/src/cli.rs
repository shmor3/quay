//! Command-line argument definitions for the `watchd` binary.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// A minimal, language-agnostic file watcher that runs commands on changes
/// and broadcasts reload or CSS-inject messages to browser clients via WebSocket.
#[derive(Parser, Debug)]
#[command(name = "watchd", version, about)]
pub struct Args {
    /// Command template to run on changes. Use `{path}` to substitute the changed file path.
    #[arg(default_value = "echo files changed")]
    pub cmd_template: String,

    /// Debounce delay in milliseconds.
    #[arg(long = "debounce-ms", default_value_t = 200)]
    pub debounce_ms: u64,

    /// WebSocket server port.
    #[arg(long = "port", default_value_t = 3012)]
    pub port: u16,

    /// Do not run configured commands on startup.
    #[arg(long = "no-run-on-start")]
    pub no_run_on_start: bool,

    /// Path to watch (and where to look for `hotreload.yaml`). Defaults to the current directory.
    #[arg(short = 'p', long = "path", default_value = ".")]
    pub path: PathBuf,

    /// Address to bind the WebSocket and control servers to.
    #[arg(long = "bind", default_value = "127.0.0.1")]
    pub bind_addr: String,

    /// Print a `<script>` snippet for embedding the hot-reload client in HTML pages, then exit.
    ///
    /// The snippet connects to the watchd WebSocket server and handles `reload`
    /// and `inject-css` messages automatically.  Use `--port` to match the
    /// running server's port if it differs from the default.
    #[arg(long = "print-snippet")]
    pub print_snippet: bool,

    /// Maximum time (in milliseconds) to wait for a command to finish before
    /// killing it.  When unset, commands are allowed to run indefinitely.
    #[arg(long = "cmd-timeout-ms")]
    pub cmd_timeout_ms: Option<u64>,

    /// Optional subcommand: `reload` or `status` (if omitted, run the watcher server).
    #[command(subcommand)]
    pub subcmd: Option<SubCommand>,
}

/// Client-mode subcommands that contact the running watchd control socket.
#[derive(Subcommand, Debug, Clone)]
pub enum SubCommand {
    /// Force a reload: run configured build/on_change commands and broadcast a `reload` message.
    Reload,
    /// Query status of loaded configs from the running watchd instance.
    Status,
}
