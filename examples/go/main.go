// quay client for Go
//
// A standalone WebSocket client that connects to a running quay server and
// reacts to hot-reload messages. Useful for triggering rebuilds, restarting
// services, or executing arbitrary callbacks when files change.
//
// Usage:
//
//	go run main.go                          # connect to ws://127.0.0.1:3012
//	go run main.go -addr ws://0.0.0.0:4000  # custom address
//
// Dependencies:
//
//	go get github.com/gorilla/websocket
//
// Protocol:
//
//	The quay server sends JSON messages over WebSocket:
//	  - {"type": "reload"}                                      → full reload
//	  - {"type": "inject-css", "path": "...", "content": "..."}  → CSS injection (base64 content)

package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// Message represents a quay WebSocket message.
type Message struct {
	Type    string `json:"type"`
	Path    string `json:"path,omitempty"`
	Content string `json:"content,omitempty"`
}

const (
	initialDelay = 1 * time.Second
	maxDelay     = 30 * time.Second
	logPrefix    = "[quay] "
)

func main() {
	addr := flag.String("addr", "ws://127.0.0.1:3012", "quay WebSocket server address")
	flag.Parse()

	log.SetFlags(log.Ltime)
	log.SetPrefix(logPrefix)

	// Graceful shutdown on SIGINT / SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	connCh := make(chan struct{}, 1)
	connCh <- struct{}{} // trigger initial connection

	delay := initialDelay

	for {
		select {
		case <-sigCh:
			log.Println("shutting down")
			return

		case <-connCh:
			err := connectAndListen(*addr, sigCh)
			if err != nil {
				log.Printf("disconnected: %v", err)
			} else {
				log.Println("disconnected")
			}

			// Schedule reconnection with exponential backoff.
			log.Printf("reconnecting in %v", delay)
			go func(d time.Duration) {
				time.Sleep(d)
				select {
				case connCh <- struct{}{}:
				default:
				}
			}(delay)

			delay = time.Duration(math.Min(float64(delay*2), float64(maxDelay)))
		}
	}
}

// connectAndListen establishes a WebSocket connection to the quay server,
// reads messages in a loop, and dispatches them to handler functions.
// It returns when the connection is closed or an error occurs.
func connectAndListen(addr string, sigCh chan os.Signal) error {
	log.Printf("connecting to %s", addr)

	conn, _, err := websocket.DefaultDialer.Dial(addr, nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	defer conn.Close()

	log.Println("connected")

	// Reset backoff on successful connection (caller handles this via channel signal).

	for {
		// Check for shutdown signal without blocking.
		select {
		case <-sigCh:
			log.Println("shutting down")
			conn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			)
			os.Exit(0)
		default:
		}

		_, rawMsg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}

		var msg Message
		if err := json.Unmarshal(rawMsg, &msg); err != nil {
			log.Printf("failed to parse message: %s", string(rawMsg))
			continue
		}

		switch msg.Type {
		case "reload":
			onReload()
		case "inject-css":
			onInjectCSS(msg.Path, msg.Content)
		default:
			log.Printf("unknown message type: %s", msg.Type)
		}
	}
}

// ---------------------------------------------------------------------------
// Handlers — customise these for your use case
// ---------------------------------------------------------------------------

// onReload is called when the server broadcasts a reload message.
//
// In a browser context this would reload the page. In a Go application you
// might restart a subprocess, re-read configuration, or trigger a rebuild.
func onReload() {
	log.Println("reload triggered")

	// Example: restart a child process
	// cmd := exec.Command("go", "run", ".")
	// cmd.Stdout = os.Stdout
	// cmd.Stderr = os.Stderr
	// if err := cmd.Start(); err != nil {
	//     log.Printf("failed to restart: %v", err)
	// }

	fmt.Println("→ files changed — reload your application")
}

// onInjectCSS is called when the server broadcasts a CSS injection message.
//
// The content is base64-encoded CSS. In a browser this would update a <style>
// element in-place. In a Go application you might write it to a file or
// forward it to a template engine.
func onInjectCSS(path, encodedContent string) {
	css, err := base64.StdEncoding.DecodeString(encodedContent)
	if err != nil {
		log.Printf("failed to decode CSS content for %s: %v", path, err)
		return
	}

	log.Printf("CSS update for %s (%d bytes)", path, len(css))

	// Example: write the CSS to the corresponding output file
	// if err := os.WriteFile(path, css, 0644); err != nil {
	//     log.Printf("failed to write %s: %v", path, err)
	// }

	fmt.Printf("→ CSS injected: %s\n", path)
}
