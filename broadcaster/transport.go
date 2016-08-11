package broadcaster

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"net/http"
	"time"
)

type Transport interface {
	fmt.Stringer
	Init(close chan<- struct{})
	Close()
	Ping() error
	ReadLoop()
	Send(message *QueuedMessage) error
}

type WebSocketTransport struct {
	sock               *websocket.Conn
	close              chan<- struct{}
	writeDeadlineDelay time.Duration
}

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

func (t *WebSocketTransport) Close() {
	t.sock.WriteControl(
		websocket.CloseMessage,
		[]byte{},
		t.writeDeadline(),
	)
}

func (t *WebSocketTransport) Init(close chan<- struct{}) {
	t.close = close
}

func (t *WebSocketTransport) Ping() error {
	return t.sock.WriteControl(
		websocket.PingMessage,
		[]byte{},
		t.writeDeadline(),
	)
}

func (t *WebSocketTransport) ReadLoop() {
	for {
		_, _, err := t.sock.ReadMessage()
		if err != nil {
			break
		}
	}
	close(t.close)
}

func (t *WebSocketTransport) Send(message *QueuedMessage) error {
	defer message.Dispatched()
	t.sock.SetWriteDeadline(t.writeDeadline())
	return t.sock.WriteJSON(*message)
}

func (t *WebSocketTransport) writeDeadline() time.Time {
	return time.Now().Add(t.writeDeadlineDelay)
}

type SSETransport struct {
	w     http.ResponseWriter
	close chan<- struct{}
}

func NewSSETransport(w http.ResponseWriter) Transport {
	return &SSETransport{
		w: w,
	}
}

func (t *SSETransport) String() string {
	return "SSE"
}

func (t *SSETransport) Close() {
	close(t.close)
}

func (t *SSETransport) Init(close chan<- struct{}) {
	t.w.Header().Set("Content-Type", "text/event-stream")
	t.w.Header().Set("X-Accel-Buffering", "no")
	t.w.WriteHeader(http.StatusOK)
	if flusher, ok := t.w.(http.Flusher); ok {
		flusher.Flush()
	}
	t.close = close
}

func (t *SSETransport) Ping() error {
	if _, err := t.w.Write([]byte(":\n")); err != nil {
		return err
	}
	if flusher, ok := t.w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

func (t *SSETransport) ReadLoop() {
	closeNotifier, ok := t.w.(http.CloseNotifier)
	if !ok {
		return
	}
	<-closeNotifier.CloseNotify()
	close(t.close)
}

func (t *SSETransport) Send(message *QueuedMessage) error {
	defer message.Dispatched()
	msg, err := json.Marshal(*message)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(t.w, "data: %s\n\n", string(msg))
	if err != nil {
		return err
	}
	if flusher, ok := t.w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}
