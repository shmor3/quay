//! Command-line argument definitions for the `quay` binary.

use clap::{Parser, Subcommand as ClapSubcommand};
use std::path::PathBuf;

/// A minimal, language-agnostic file watcher that runs commands on changes
/// and broadcasts reload or CSS-inject messages to browser clients via WebSocket.
#[derive(Parser, Debug)]
#[command(
    name = "quay",
    version,
    about = "A minimal, language-agnostic file watcher that runs commands on changes and broadcasts reload or CSS-inject messages to browser clients via WebSocket.",
    long_about = "USAGE:\n    quay [OPTIONS] [CMD_TEMPLATE]\n\nEXAMPLES:\n    # Run the watcher server and watch a directory\n    quay --path /path/to/project --port 3012\n\n    # Use a command timeout to kill stuck builds after 30 seconds\n    quay --path . --cmd-timeout-ms 30000\n\n    # Enable the diff store to track file changes\n    quay --path . --diff\n\n    # Print the browser client snippet for embedding\n    quay --print-snippet\n\n    # Query status (client-mode subcommand)\n    quay --port 3012 status\n\n    # Trigger reload (client-mode subcommand)\n    quay --port 3012 reload\n\n    # Query the latest diff for a file (client-mode subcommand)\n    quay --port 3012 diff --path src/styles/main.css\n\nOPTIONS:\n    -p, --path <PATH>           Directory to watch and where to look for quay.yaml [default: .]\n    --port <PORT>               WebSocket server port [default: 3012]\n    --bind <ADDR>               Address to bind the WebSocket and control servers to [default: 127.0.0.1]\n    --debounce-ms <MS>          Debounce delay in milliseconds [default: 200]\n    --no-run-on-start           Do not run configured commands on startup\n    --cmd-timeout-ms <MS>       Maximum time to wait for a command before killing it\n    --print-snippet             Print the HTML <script> snippet for embedding the client, then exit\n    --diff                      Enable the in-memory diff store for file change tracking\n    --diff-max-file-size <B>    Maximum file size (bytes) the diff store will process (ignored without --diff) [default: 524288]\n\nSUBCOMMANDS:\n    reload                     Force a reload: run configured build/on_change commands and broadcast a reload message\n    status                     Query status of loaded configs from the running quay instance\n    diff                       Query stored file diffs from the running quay instance. Shows the latest diff for a specific file, or lists all tracked files\n",
    help_template = "{about}\n\n{usage}\n\n{all-args}\n\n{after-help}"
)]
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

    /// Path to watch (and where to look for `quay.yaml`). Defaults to the current directory.
    #[arg(short = 'p', long = "path", default_value = ".")]
    pub path: PathBuf,

    /// Address to bind the WebSocket and control servers to.
    #[arg(long = "bind", default_value = "127.0.0.1")]
    pub bind_addr: String,

    /// Allow binding to all interfaces (e.g., 0.0.0.0) exposing the server to the network.
    #[arg(long = "expose-network")]
    pub expose_network: bool,

    /// Print a `<script>` snippet for embedding the hot-reload client in HTML pages, then exit.
    ///
    /// The snippet connects to the quay WebSocket server and handles `reload`
    /// and `inject-css` messages automatically.  Use `--port` to match the
    /// running server's port if it differs from the default.
    #[arg(long = "print-snippet")]
    pub print_snippet: bool,

    /// Maximum time (in milliseconds) to wait for a command to finish before
    /// killing it.  When unset, commands are allowed to run indefinitely.
    #[arg(long = "cmd-timeout-ms")]
    pub cmd_timeout_ms: Option<u64>,

    /// Enable the in-memory diff store.  When set, quay records a unified
    /// diff (using `+` / `-` prefixes) for every file change and exposes it
    /// via the control socket (`diff`, `diffs`, `diff-clear` commands).
    #[arg(long = "diff")]
    pub diff: bool,

    /// Maximum file size (in bytes) the diff store will process.  Files larger
    /// than this are recorded with a placeholder instead of a real diff, and
    /// their content is not kept in memory.  Ignored unless `--diff` is set.
    /// Defaults to 524288 (512 KiB).
    #[arg(long = "diff-max-file-size", default_value_t = 512 * 1024)]
    pub diff_max_file_size: usize,

    /// Optional TLS certificate path.
    #[arg(long = "tls-cert")]
    pub tls_cert: Option<String>,

    /// Optional TLS key path.
    #[arg(long = "tls-key")]
    pub tls_key: Option<String>,

    /// Optional authentication token for the control socket.
    #[arg(long = "auth-token")]
    pub auth_token: Option<String>,

    /// Optional maximum number of concurrent WebSocket connections.
    #[arg(long = "max-connections")]
    pub max_connections: Option<u32>,

    /// Optional subcommand: `reload`, `status`, or `diff` (if omitted, run the watcher server).
    #[command(subcommand)]
    pub subcmd: Option<Subcommand>,
}

/// Client-mode subcommands that contact the running quay control socket.
#[derive(ClapSubcommand, Debug, Clone)]
pub enum Subcommand {
    /// Force a reload: run configured build/on_change commands and broadcast a reload message to all browser clients.
    #[command(
        about = "Force a reload: run configured build/on_change commands and broadcast a reload message to all browser clients."
    )]
    Reload,
    /// Query status of loaded configs and active WebSocket connections from the running quay instance.
    #[command(
        about = "Query status of loaded configs and active WebSocket connections from the running quay instance."
    )]
    Status,
    /// Query stored file diffs from the running quay instance. Shows the latest diff for a specific file, or lists all tracked files.
    #[command(
        about = "Query stored file diffs from the running quay instance. Shows the latest diff for a specific file, or lists all tracked files."
    )]
    Diff {
        /// File path to show the diff for. When omitted, lists all tracked files with a summary.
        #[arg(
            long = "path",
            help = "File path to show the diff for. When omitted, lists all tracked files with a summary."
        )]
        path: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: parse arguments from an iterator, prepending the binary name.
    fn parse(args: &[&str]) -> Args {
        let mut full: Vec<&str> = vec!["quay"];
        full.extend_from_slice(args);
        Args::try_parse_from(full).expect("failed to parse args")
    }

    /// Helper: attempt to parse arguments, returning the error on failure.
    fn try_parse(args: &[&str]) -> std::result::Result<Args, clap::Error> {
        let mut full: Vec<&str> = vec!["quay"];
        full.extend_from_slice(args);
        Args::try_parse_from(full)
    }

    // -- Default values ----------------------------------------------------

    #[test]
    fn defaults_with_no_args() {
        let args = parse(&[]);
        assert_eq!(args.cmd_template, "echo files changed");
        assert_eq!(args.debounce_ms, 200);
        assert_eq!(args.port, 3012);
        assert!(!args.no_run_on_start);
        assert_eq!(args.path, PathBuf::from("."));
        assert_eq!(args.bind_addr, "127.0.0.1");
        assert!(!args.print_snippet);
        assert!(args.cmd_timeout_ms.is_none());
        assert!(!args.diff);
        assert_eq!(args.diff_max_file_size, 512 * 1024);
        assert!(args.subcmd.is_none());
    }

    // -- Custom values -----------------------------------------------------

    #[test]
    fn custom_cmd_template() {
        let args = parse(&["npm run build"]);
        assert_eq!(args.cmd_template, "npm run build");
    }

    #[test]
    fn custom_debounce_ms() {
        let args = parse(&["--debounce-ms", "500"]);
        assert_eq!(args.debounce_ms, 500);
    }

    #[test]
    fn custom_port() {
        let args = parse(&["--port", "8080"]);
        assert_eq!(args.port, 8080);
    }

    #[test]
    fn port_min_value() {
        let args = parse(&["--port", "0"]);
        assert_eq!(args.port, 0);
    }

    #[test]
    fn port_max_value() {
        let args = parse(&["--port", "65535"]);
        assert_eq!(args.port, 65535);
    }

    #[test]
    fn custom_path_short_flag() {
        let args = parse(&["-p", "/tmp/project"]);
        assert_eq!(args.path, PathBuf::from("/tmp/project"));
    }

    #[test]
    fn custom_path_long_flag() {
        let args = parse(&["--path", "/home/user/project"]);
        assert_eq!(args.path, PathBuf::from("/home/user/project"));
    }

    #[test]
    fn custom_bind_addr() {
        let args = parse(&["--bind", "0.0.0.0"]);
        assert_eq!(args.bind_addr, "0.0.0.0");
    }

    #[test]
    fn no_run_on_start_flag() {
        let args = parse(&["--no-run-on-start"]);
        assert!(args.no_run_on_start);
    }

    #[test]
    fn print_snippet_flag() {
        let args = parse(&["--print-snippet"]);
        assert!(args.print_snippet);
    }

    #[test]
    fn cmd_timeout_ms_set() {
        let args = parse(&["--cmd-timeout-ms", "30000"]);
        assert_eq!(args.cmd_timeout_ms, Some(30000));
    }

    #[test]
    fn cmd_timeout_ms_zero() {
        let args = parse(&["--cmd-timeout-ms", "0"]);
        assert_eq!(args.cmd_timeout_ms, Some(0));
    }

    // -- Diff flags --------------------------------------------------------

    #[test]
    fn diff_flag_off_by_default() {
        let args = parse(&[]);
        assert!(!args.diff);
    }

    #[test]
    fn diff_flag_enabled() {
        let args = parse(&["--diff"]);
        assert!(args.diff);
    }

    #[test]
    fn diff_max_file_size_default() {
        let args = parse(&[]);
        assert_eq!(args.diff_max_file_size, 512 * 1024);
    }

    #[test]
    fn diff_max_file_size_custom() {
        let args = parse(&["--diff", "--diff-max-file-size", "1048576"]);
        assert!(args.diff);
        assert_eq!(args.diff_max_file_size, 1048576);
    }

    // -- Subcommands -------------------------------------------------------

    #[test]
    fn reload_subcommand() {
        let args = parse(&["reload"]);
        assert!(matches!(args.subcmd, Some(Subcommand::Reload)));
    }

    #[test]
    fn status_subcommand() {
        let args = parse(&["status"]);
        assert!(matches!(args.subcmd, Some(Subcommand::Status)));
    }

    #[test]
    fn diff_subcommand_no_path() {
        let args = parse(&["diff"]);
        assert!(matches!(args.subcmd, Some(Subcommand::Diff { path: None })));
    }

    #[test]
    fn diff_subcommand_with_path() {
        let args = parse(&["diff", "--path", "src/main.css"]);
        match &args.subcmd {
            Some(Subcommand::Diff { path }) => {
                assert_eq!(path.as_deref(), Some("src/main.css"));
            }
            other => panic!("expected Diff subcommand, got {:?}", other),
        }
    }

    #[test]
    fn subcommand_with_port() {
        let args = parse(&["--port", "4000", "reload"]);
        assert_eq!(args.port, 4000);
        assert!(matches!(args.subcmd, Some(Subcommand::Reload)));
    }

    #[test]
    fn diff_subcommand_with_port() {
        let args = parse(&["--port", "4000", "diff", "--path", "x.css"]);
        assert_eq!(args.port, 4000);
        assert!(matches!(args.subcmd, Some(Subcommand::Diff { .. })));
    }

    #[test]
    fn subcommand_with_bind() {
        let args = parse(&["--bind", "0.0.0.0", "status"]);
        assert_eq!(args.bind_addr, "0.0.0.0");
        assert!(matches!(args.subcmd, Some(Subcommand::Status)));
    }

    // -- Combined flags ----------------------------------------------------

    #[test]
    fn all_flags_combined() {
        let args = parse(&[
            "--port",
            "5000",
            "--bind",
            "0.0.0.0",
            "--debounce-ms",
            "100",
            "-p",
            "/srv/app",
            "--no-run-on-start",
            "--cmd-timeout-ms",
            "15000",
            "--diff",
            "--diff-max-file-size",
            "262144",
            "make build",
        ]);
        assert_eq!(args.port, 5000);
        assert_eq!(args.bind_addr, "0.0.0.0");
        assert_eq!(args.debounce_ms, 100);
        assert_eq!(args.path, PathBuf::from("/srv/app"));
        assert!(args.no_run_on_start);
        assert_eq!(args.cmd_timeout_ms, Some(15000));
        assert!(args.diff);
        assert_eq!(args.diff_max_file_size, 262144);
        assert_eq!(args.cmd_template, "make build");
        assert!(args.subcmd.is_none());
    }

    // -- Validation / rejection --------------------------------------------

    #[test]
    fn invalid_port_rejected() {
        // Port 99999 exceeds u16 range
        let result = try_parse(&["--port", "99999"]);
        assert!(result.is_err());
    }

    #[test]
    fn negative_port_rejected() {
        let result = try_parse(&["--port", "-1"]);
        assert!(result.is_err());
    }

    #[test]
    fn non_numeric_port_rejected() {
        let result = try_parse(&["--port", "abc"]);
        assert!(result.is_err());
    }

    #[test]
    fn non_numeric_debounce_rejected() {
        let result = try_parse(&["--debounce-ms", "fast"]);
        assert!(result.is_err());
    }

    #[test]
    fn unknown_flag_rejected() {
        let result = try_parse(&["--does-not-exist"]);
        assert!(result.is_err());
    }

    #[test]
    fn unknown_subcommand_rejected() {
        let result = try_parse(&["restart"]);
        // "restart" is treated as the cmd_template positional, not a subcommand,
        // so it should actually parse successfully as a command template.
        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.cmd_template, "restart");
        assert!(args.subcmd.is_none());
    }

    // -- Debug impls -------------------------------------------------------

    #[test]
    fn args_implements_debug() {
        let args = parse(&[]);
        let dbg = format!("{:?}", args);
        assert!(dbg.contains("Args"));
        assert!(dbg.contains("cmd_template"));
        assert!(dbg.contains("port"));
    }

    #[test]
    fn subcommand_implements_debug_and_clone() {
        let sub = Subcommand::Reload;
        let dbg = format!("{:?}", sub);
        assert!(dbg.contains("Reload"));

        let cloned = sub.clone();
        let dbg2 = format!("{:?}", cloned);
        assert_eq!(dbg, dbg2);
    }

    #[test]
    fn subcommand_status_debug_and_clone() {
        let sub = Subcommand::Status;
        let dbg = format!("{:?}", sub);
        assert!(dbg.contains("Status"));

        let cloned = sub.clone();
        assert!(format!("{:?}", cloned).contains("Status"));
    }

    #[test]
    fn subcommand_diff_debug_and_clone() {
        let sub = Subcommand::Diff {
            path: Some("test.css".to_string()),
        };
        let dbg = format!("{:?}", sub);
        assert!(dbg.contains("Diff"));
        assert!(dbg.contains("test.css"));

        let cloned = sub.clone();
        assert!(format!("{:?}", cloned).contains("Diff"));
    }
}
