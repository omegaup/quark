package broadcaster

import (
	"fmt"
	"github.com/gorilla/websocket"
	"net/http"
	"time"
)

// A Transport exposes the interface needed to broadcast events to a
// subscriber.
type Transport interface {
	fmt.Stringer

	// Init performs any initialization and sets a channel that will be closed by
	// the Transport if the connection is closed.
	Init(close chan<- struct{})

	// Close writes the WebSockets Close message to notify the subscriber that
	// the socket is going away.
	Close()

	// Ping sends an empty message to the subscriber, to ensure that the
	// underlying channel is not closed due to inactivity.
	Ping() error

	// ReadLoop consumes any input sent by the subscriber.
	ReadLoop()

	// Send sends a message to the subscriber.
	Send(message *QueuedMessage) error
}

// A WebSocketTransport is a transport that uses WebSockets to deliver events.
type WebSocketTransport struct {
	sock               *websocket.Conn
	close              chan<- struct{}
	writeDeadlineDelay time.Duration
}

// NewWebSocketTransport creates a new WebSocketTransport for the provided websocket.
func NewWebSocketTransport(
	sock *websocket.Conn,
	writeDeadlineDelay time.Duration,
) Transport {
	return &WebSocketTransport{
		sock:               sock,
		writeDeadlineDelay: writeDeadlineDelay,
	}
}

func (t *WebSocketTransport) String() string {
	return "WebSocket"
}

// Close writes the WebSockets Close message to notify the subscriber that the
// socket is going away. It does not actually signals the close channel, since
// that will happen as soon as we receive the Close message from the
// subscriber.
func (t *WebSocketTransport) Close() {
	t.sock.WriteControl(
		websocket.CloseMessage,
		[]byte{},
		t.writeDeadline(),
	)
}

// Init sets a channel that will be closed by the Transport if the connection
// is closed.
func (t *WebSocketTransport) Init(close chan<- struct{}) {
	t.close = close
}

// Ping sends a WebSockets Ping message to prevent the underlying socket from
// being closed due to inactivity.
func (t *WebSocketTransport) Ping() error {
	return t.sock.WriteControl(
		websocket.PingMessage,
		[]byte{},
		t.writeDeadline(),
	)
}

// ReadLoop reads (and discards) all incoming messages.
func (t *WebSocketTransport) ReadLoop() {
	for {
		_, _, err := t.sock.ReadMessage()
		if err != nil {
			break
		}
	}
	close(t.close)
}

// Send sends the provided message.
func (t *WebSocketTransport) Send(message *QueuedMessage) error {
	defer message.Dispatched()
	t.sock.SetWriteDeadline(t.writeDeadline())
	return t.sock.WriteMessage(
		websocket.TextMessage,
		[]byte(message.message.Message),
	)
}

func (t *WebSocketTransport) writeDeadline() time.Time {
	return time.Now().Add(t.writeDeadlineDelay)
}

// A SSETransport is a Transport that uses Server-Side Events to deliver events.
type SSETransport struct {
	w     http.ResponseWriter
	close chan<- struct{}
}

// NewSSETransport creates a new SSETransport for the provided ResponseWriter.
func NewSSETransport(w http.ResponseWriter) Transport {
	return &SSETransport{
		w: w,
	}
}

func (t *SSETransport) String() string {
	return "SSE"
}

// Close signals the close channel.
func (t *SSETransport) Close() {
	close(t.close)
}

// Init sends the necessary headers to signal that this is a SSE response.
func (t *SSETransport) Init(close chan<- struct{}) {
	t.w.Header().Set("Content-Type", "text/event-stream")
	t.w.Header().Set("X-Accel-Buffering", "no")
	t.w.WriteHeader(http.StatusOK)
	if flusher, ok := t.w.(http.Flusher); ok {
		flusher.Flush()
	}
	t.close = close
}

// Ping sends an empty message to prevent the underlying connection from being
// closed due to inactivity.
func (t *SSETransport) Ping() error {
	if _, err := t.w.Write([]byte(":\n")); err != nil {
		return err
	}
	if flusher, ok := t.w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

// ReadLoop waits until the underlying connection is closed, since the
// subscriber cannot send anything to us.
func (t *SSETransport) ReadLoop() {
	closeNotifier, ok := t.w.(http.CloseNotifier)
	if !ok {
		return
	}
	<-closeNotifier.CloseNotify()
	close(t.close)
}

// Send sends the provided message.
func (t *SSETransport) Send(message *QueuedMessage) error {
	defer message.Dispatched()
	_, err := fmt.Fprintf(t.w, "data: %s\n\n", message.message.Message)
	if err != nil {
		return err
	}
	if flusher, ok := t.w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}
