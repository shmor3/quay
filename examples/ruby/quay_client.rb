#!/usr/bin/env ruby
# frozen_string_literal: true

# quay hot-reload client for Ruby
#
# A standalone WebSocket client that connects to a running quay server and
# reacts to `reload` and `inject-css` messages. Designed for use in Ruby
# development workflows — trigger custom callbacks when files change.
#
# Dependencies:
#   gem install websocket-client-simple
#
# Usage:
#   ruby quay_client.rb [HOST] [PORT]
#
# Examples:
#   ruby quay_client.rb
#   ruby quay_client.rb localhost 4000
#
# License: MIT

require "json"
require "base64"
require "logger"

begin
  require "websocket-client-simple"
rescue LoadError
  warn "[quay] Missing dependency: websocket-client-simple"
  warn "[quay] Install it with: gem install websocket-client-simple"
  exit 1
end

module HotReload
  # Default WebSocket port used by quay.
  DEFAULT_PORT = 3012

  # Default host to connect to.
  DEFAULT_HOST = "localhost"

  # Initial reconnect delay in seconds.
  INITIAL_DELAY = 1.0

  # Maximum reconnect delay in seconds.
  MAX_DELAY = 30.0

  # Client connects to a quay WebSocket server and dispatches messages to
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
          @logger.warn("[quay] connection error: #{e.message}")
        end

        break unless @running

        @logger.info("[quay] reconnecting in #{@reconnect_delay}s")
        sleep(@reconnect_delay)
        @reconnect_delay = [@reconnect_delay * 2, MAX_DELAY].min
      end
    end

    def connect
      url = "ws://#{@host}:#{@port}"
      @logger.info("[quay] connecting to #{url}")

      connected = false
      disconnected = false
      client_ref = self
      logger_ref = @logger
      reconnect_reset = -> { @reconnect_delay = INITIAL_DELAY }

      @ws = WebSocket::Client::Simple.connect(url)

      @ws.on :open do
        logger_ref.info("[quay] connected")
        connected = true
        reconnect_reset.call
      end

      @ws.on :message do |msg|
        client_ref.send(:handle_message, msg.data)
      end

      @ws.on :close do |e|
        code = e.respond_to?(:code) ? e.code : "unknown"
        logger_ref.info("[quay] disconnected (code #{code})")
        disconnected = true
      end

      @ws.on :error do |e|
        logger_ref.warn("[quay] error: #{e.message}")
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
        @logger.info("[quay] reload requested")
        @reload_callbacks.each(&:call)

      when "inject-css"
        path = msg["path"]
        content = msg["content"]

        if path && content
          css = Base64.decode64(content)
          @logger.info("[quay] CSS inject for #{path} (#{css.bytesize} bytes)")
          @css_inject_callbacks.each { |cb| cb.call(path, css) }
        else
          @logger.warn("[quay] inject-css message missing path or content")
        end

      else
        @logger.debug("[quay] unknown message type: #{msg["type"]}")
      end
    rescue JSON::ParserError => e
      @logger.warn("[quay] failed to parse message: #{e.message}")
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
    puts "[quay] 🔄 reload triggered"
  end

  client.on_css_inject do |path, css|
    puts "[quay] 🎨 CSS injected for #{path} (#{css.bytesize} bytes)"
  end

  # Handle Ctrl-C gracefully.
  trap("INT") do
    puts "\n[quay] shutting down"
    client.stop
    exit 0
  end

  trap("TERM") do
    client.stop
    exit 0
  end

  puts "[quay] Ruby client starting (#{host}:#{port})"
  puts "[quay] Press Ctrl-C to stop"
  puts

  client.start
end
