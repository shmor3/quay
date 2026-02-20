# Ruby Hot-Reload Client

A WebSocket client for Ruby that connects to a running `watchd` server and
reacts to file-change notifications.

## Requirements

- Ruby 2.7+
- [`websocket-client-simple`](https://rubygems.org/gems/websocket-client-simple) gem

## Installation

```bash
gem install websocket-client-simple
```

Or add to your `Gemfile`:

```ruby
gem "websocket-client-simple"
```

## Usage

### Standalone

```bash
# Connect to the default address (localhost:3012)
ruby hotreload_client.rb

# Custom host and port
ruby hotreload_client.rb 192.168.1.10 4000
```

### As a Library

```ruby
require_relative "hotreload_client"

client = HotReload::Client.new(host: "localhost", port: 3012)

client.on_reload do
  puts "Files changed — restarting server..."
  exec("ruby", "my_app.rb")
end

client.on_css_inject do |path, css|
  File.write("public/#{File.basename(path)}", css)
  puts "Updated #{path}"
end

client.on_message do |msg|
  puts "Raw message: #{msg}"
end

# Blocks the current thread; reconnects automatically on disconnect.
client.start
```

### Integration with Rails

```ruby
# config/initializers/hotreload.rb (development only)
if Rails.env.development?
  Thread.new do
    require_relative "../../lib/hotreload_client"

    client = HotReload::Client.new(port: 3012)

    client.on_reload do
      Rails.logger.info "[hotreload] reload triggered"
      # ActionCable or Turbo Streams could forward this to the browser
    end

    client.on_css_inject do |path, css|
      dest = Rails.root.join("public", "hotreload", File.basename(path))
      FileUtils.mkdir_p(dest.dirname)
      File.write(dest, css)
      Rails.logger.info "[hotreload] CSS written to #{dest}"
    end

    client.start
  end
end
```

## Features

- **Automatic reconnection** with exponential backoff (1 s → 30 s cap)
- **Callback-based API** via `on_reload`, `on_css_inject`, and `on_message`
- **Base64 decoding** of CSS content from `inject-css` messages
- **Clean shutdown** on `SIGINT` / `SIGTERM`
- **Structured logging** via Ruby's `Logger`

## Protocol

The client listens for JSON messages from the `watchd` WebSocket server:

| Message Type  | Fields                     | Description                       |
|---------------|----------------------------|-----------------------------------|
| `reload`      | `type`                     | Trigger a full reload             |
| `inject-css`  | `type`, `path`, `content`  | Hot-inject CSS (base64-encoded)   |

See the [examples README](../README.md) for the full protocol specification.