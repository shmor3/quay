#!/usr/bin/env ruby
# frozen_string_literal: true

# watchd hot-reload client for Ruby
#
# A standalone WebSocket client that connects to a running watchd server and
# reacts to `reload` and `inject-css` messages. Designed for use in Ruby
# development workflows — trigger custom callbacks when files change.
#
# Dependencies:
#   gem install websocket-client-simple
#
# Usage:
#   ruby hotreload_client.rb [HOST] [PORT]
#
# Examples:
#   ruby hotreload_client.rb
#   ruby hotreload_client.rb localhost 4000
#
# License: MIT

require "json"
require "base64"
require "logger"

begin
  require "websocket-client-simple"
rescue LoadError
  warn "[hotreload] Missing dependency: websocket-client-simple"
  warn "[hotreload] Install it with: gem install websocket-client-simple"
  exit 1
end

module HotReload
  # Default WebSocket port used by watchd.
  DEFAULT_PORT = 3012

  # Default host to connect to.
  DEFAULT_HOST = "localhost"

  # Initial reconnect delay in seconds.
  INITIAL_DELAY = 1.0

  # Maximum reconnect delay in seconds.
  MAX_DELAY = 30.0

  # Client connects to a watchd WebSocket server and dispatches messages to
  # user-defined callbacks.
  #
  # Example:
  #   client = HotReload::Client.new(host: "localhost", port: 3012)
  #
  #   client.on_reload do
  #     puts "Files changed — restarting..."
  #     exec("ruby", "my_app.rb")
  #   end
  #
  #   client.on_css_inject do |path, css|
  #     File.write("public/#{File.basename(path)}", css)
  #   end
  #
  #   client.start  # blocking
  #
  class Client
    attr_reader :host, :port

    def initialize(host: DEFAULT_HOST, port: DEFAULT_PORT, logger: nil)
      @host = host
      @port = port
      @logger = logger || default_logger
      @reconnect_delay = INITIAL_DELAY
      @reload_callbacks = []
      @css_inject_callbacks = []
      @message_callbacks = []
      @running = false
    end

    # Register a callback to be invoked on `reload` messages.
    #
    #   client.on_reload { puts "reloading!" }
    #
    def on_reload(&block)
      @reload_callbacks << block
      self
    end

    # Register a callback to be invoked on `inject-css` messages.
    # The block receives two arguments: the file path and the decoded CSS string.
    #
    #   client.on_css_inject do |path, css|
    #     File.write(path, css)
    #   end
    #
    def on_css_inject(&block)
      @css_inject_callbacks << block
      self
    end

    # Register a callback to be invoked on any message.
    # The block receives the parsed JSON hash.
    #
    #   client.on_message do |msg|
    #     puts "Received: #{msg}"
    #   end
    #
    def on_message(&block)
      @message_callbacks << block
      self
    end

    # Start the client. This method blocks and reconnects automatically on
    # disconnection with exponential backoff.
    def start
      @running = true
      connect_loop
    end

    # Stop the client and close the WebSocket connection.
    def stop
      @running = false
      @ws&.close
    end

    private

    def connect_loop
      while @running
        begin
          connect
        rescue StandardError => e
          @logger.warn("[hotreload] connection error: #{e.message}")
        end

        break unless @running

        @logger.info("[hotreload] reconnecting in #{@reconnect_delay}s")
        sleep(@reconnect_delay)
        @reconnect_delay = [@reconnect_delay * 2, MAX_DELAY].min
      end
    end

    def connect
      url = "ws://#{@host}:#{@port}"
      @logger.info("[hotreload] connecting to #{url}")

      connected = false
      disconnected = false
      client_ref = self
      logger_ref = @logger
      reconnect_reset = -> { @reconnect_delay = INITIAL_DELAY }

      @ws = WebSocket::Client::Simple.connect(url)

      @ws.on :open do
        logger_ref.info("[hotreload] connected")
        connected = true
        reconnect_reset.call
      end

      @ws.on :message do |msg|
        client_ref.send(:handle_message, msg.data)
      end

      @ws.on :close do |e|
        code = e.respond_to?(:code) ? e.code : "unknown"
        logger_ref.info("[hotreload] disconnected (code #{code})")
        disconnected = true
      end

      @ws.on :error do |e|
        logger_ref.warn("[hotreload] error: #{e.message}")
        disconnected = true
      end

      # Block until disconnected.
      sleep 0.1 until disconnected || !@running
    end

    def handle_message(data)
      msg = JSON.parse(data)
      @message_callbacks.each { |cb| cb.call(msg) }

      case msg["type"]
      when "reload"
        @logger.info("[hotreload] reload requested")
        @reload_callbacks.each(&:call)

      when "inject-css"
        path = msg["path"]
        content = msg["content"]

        if path && content
          css = Base64.decode64(content)
          @logger.info("[hotreload] CSS inject for #{path} (#{css.bytesize} bytes)")
          @css_inject_callbacks.each { |cb| cb.call(path, css) }
        else
          @logger.warn("[hotreload] inject-css message missing path or content")
        end

      else
        @logger.debug("[hotreload] unknown message type: #{msg["type"]}")
      end
    rescue JSON::ParserError => e
      @logger.warn("[hotreload] failed to parse message: #{e.message}")
    end

    def default_logger
      logger = Logger.new($stdout)
      logger.formatter = proc do |severity, _datetime, _progname, msg|
        "#{severity}: #{msg}\n"
      end
      logger.level = Logger::INFO
      logger
    end
  end
end

# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __FILE__ == $PROGRAM_NAME
  host = ARGV[0] || HotReload::DEFAULT_HOST
  port = (ARGV[1] || HotReload::DEFAULT_PORT).to_i

  client = HotReload::Client.new(host: host, port: port)

  client.on_reload do
    puts "[hotreload] 🔄 reload triggered"
  end

  client.on_css_inject do |path, css|
    puts "[hotreload] 🎨 CSS injected for #{path} (#{css.bytesize} bytes)"
  end

  # Handle Ctrl-C gracefully.
  trap("INT") do
    puts "\n[hotreload] shutting down"
    client.stop
    exit 0
  end

  trap("TERM") do
    client.stop
    exit 0
  end

  puts "[hotreload] Ruby client starting (#{host}:#{port})"
  puts "[hotreload] Press Ctrl-C to stop"
  puts

  client.start
end
