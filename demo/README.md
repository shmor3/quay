# Demo site for hotreload-watcher

This folder contains a tiny static example you can serve to test the watcher and client.

Files:
- `index.html` — example page that includes `hotreload-client.js`.
- `style.css` — styling that will be hot-injected.
- `hotreload-client.js` — client script (same as `watcher/hotreload-client.js` but copied here for a self-contained demo).

How to run a quick static server (Python):

```bash
cd demo
python -m http.server 8080
```

Then open http://localhost:8080 in your browser. In another terminal, run the watcher from the repo root:

```bash
cargo run --manifest-path watcher/Cargo.toml -- "echo built {path}"
```

Edit `demo/style.css` to see CSS injection without full page reload; edit `index.html` to see a reload.
