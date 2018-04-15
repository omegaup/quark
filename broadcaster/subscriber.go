package broadcaster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/omegaup/quark/common"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// An UpstreamError represents an error that comes from the frontend.
type UpstreamError struct {
	HTTPStatusCode int
	Contents       []byte
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("http %d", e.HTTPStatusCode)
}

// A QueuedMessage is a message that may be broadcast to relevant Subscribers.
// It also performs latency analysis.
type QueuedMessage struct {
	time    time.Time
	metrics Metrics
	message *Message
}

// Processed signals that this message has been processed and has been enqueued
// in all the relevant Subscribers' queues.
func (m *QueuedMessage) Processed() {
	m.metrics.ObserveProcessMessageLatency(time.Since(m.time))
}

// Dispatched signals that this message has been dispatched. It is used to
// perform latency analysis.
func (m *QueuedMessage) Dispatched() {
	m.metrics.ObserveDispatchMessageLatency(time.Since(m.time))
}

// A Message is a message that will be broadcast to Subscribers.
type Message struct {
	Contest    string `json:"contest,omitempty"`
	Problemset int    `json:"problemset,omitempty"`
	Problem    string `json:"problem,omitempty"`
	User       string `json:"user,omitempty"`
	Public     bool   `json:"public"`
	Message    string `json:"message"`
}

// A ValidateFilterResponse holds the results of a Validate request.
type ValidateFilterResponse struct {
	User            string   `json:"user"`
	Admin           bool     `json:"admin"`
	ProblemAdmin    []string `json:"problem_admin"`
	ContestAdmin    []string `json:"contest_admin"`
	ProblemsetAdmin []int    `json:"problemset_admin"`
}

// Metrics is the interface needed to publish performance metrics.
type Metrics interface {
	IncrementWebSocketsCount(delta int)
	IncrementSSECount(delta int)
	IncrementMessagesCount()
	IncrementChannelDropCount()
	ObserveDispatchMessageLatency(latency time.Duration)
	ObserveProcessMessageLatency(latency time.Duration)
}

// A Broadcaster can send messages to Subscribers.
type Broadcaster struct {
	ctx         *common.Context
	subscribers map[*Subscriber]struct{}
	deauth      chan string
	messages    chan *QueuedMessage
	subscribe   chan *Subscriber
	unsubscribe chan *Subscriber
	metrics     Metrics
}

// NewBroadcaster returns a new Broadcaster.
func NewBroadcaster(ctx *common.Context, metrics Metrics) *Broadcaster {
	return &Broadcaster{
		ctx:         ctx,
		subscribers: make(map[*Subscriber]struct{}),
		deauth:      make(chan string, 5),
		messages:    make(chan *QueuedMessage, ctx.Config.Broadcaster.ChannelLength),
		subscribe:   make(chan *Subscriber, 5),
		unsubscribe: make(chan *Subscriber, 5),
		metrics:     metrics,
	}
}

func (b *Broadcaster) remove(s *Subscriber) {
	delete(b.subscribers, s)
	close(s.Send())
	if s.transport.String() == "WebSocket" {
		b.metrics.IncrementWebSocketsCount(-1)
	} else {
		b.metrics.IncrementSSECount(-1)
	}
}

// Run is the main Broadcaster loop. It listens for
// subscribe/unsubscribe/deauth events to manage the Subscribers, as well as
// new incoming messages that will be sent to all matching Subscribers.
func (b *Broadcaster) Run() {
	for {
		select {
		case s := <-b.subscribe:
			b.subscribers[s] = struct{}{}
			if s.transport.String() == "WebSocket" {
				b.metrics.IncrementWebSocketsCount(1)
			} else {
				b.metrics.IncrementSSECount(1)
			}

		case u := <-b.deauth:
			for s := range b.subscribers {
				if s.user == u {
					b.remove(s)
				}
			}

		case s := <-b.unsubscribe:
			_, ok := b.subscribers[s]
			if ok {
				b.remove(s)
			}

		case m := <-b.messages:
			b.metrics.IncrementMessagesCount()
			for s := range b.subscribers {
				if !s.Matches(m.message) {
					continue
				}
				select {
				case s.Send() <- m:
					break

				default:
					b.metrics.IncrementChannelDropCount()
					b.ctx.Log.Error("Dropped message on subscriber", "subscriber", s)
					b.remove(s)
				}
			}
			m.Processed()
			break
		}
	}
}

// Deauthenticate removes all the Subscribers belonging to the provided user
// from the Broadcaster.
func (b *Broadcaster) Deauthenticate(user string) {
	b.deauth <- user
}

// Broadcast delivers the provided message to all matching Subscribers.
func (b *Broadcaster) Broadcast(message *Message) bool {
	queuedMessage := &QueuedMessage{
		time:    time.Now(),
		metrics: b.metrics,
		message: message,
	}
	select {
	case b.messages <- queuedMessage:
		return true

	default:
		queuedMessage.Processed()
		b.metrics.IncrementChannelDropCount()
		b.ctx.Log.Error("Dropped broadcast message")
		return false
	}
}

// Subscribe adds one subscriber to the Broadcaster.
func (b *Broadcaster) Subscribe(subscriber *Subscriber) bool {
	select {
	case b.subscribe <- subscriber:
		return true

	default:
		b.metrics.IncrementChannelDropCount()
		b.ctx.Log.Error("Dropped subscribe request", "subscriber", subscriber)
		return false
	}
}

// Unsubscribe removes one subscriber from the Broadcaster.
func (b *Broadcaster) Unsubscribe(subscriber *Subscriber) bool {
	select {
	case b.unsubscribe <- subscriber:
		return true

	default:
		b.metrics.IncrementChannelDropCount()
		b.ctx.Log.Error("Dropped unsubscribe request", "subscriber", subscriber)
		return false
	}
}

// A Subscriber represents a user that wishes to receive broadcast
// notifications.
type Subscriber struct {
	authToken          string
	user               string
	admin              bool
	problemAdminMap    map[string]struct{}
	contestAdminMap    map[string]struct{}
	problemsetAdminMap map[int]struct{}
	filters            []Filter

	ctx       *common.Context
	close     chan struct{}
	send      chan *QueuedMessage
	transport Transport
}

// NewSubscriber creates a new Subscriber.
func NewSubscriber(
	ctx *common.Context,
	client *http.Client,
	requestURL *url.URL,
	authToken string,
	filterString string,
	transport Transport,
) (*Subscriber, error) {
	s := &Subscriber{
		ctx:                ctx,
		authToken:          authToken,
		problemAdminMap:    make(map[string]struct{}),
		contestAdminMap:    make(map[string]struct{}),
		problemsetAdminMap: make(map[int]struct{}),
		filters:            make([]Filter, 0),
		close:              make(chan struct{}, 0),
		send:               make(chan *QueuedMessage, ctx.Config.Broadcaster.ChannelLength),
		transport:          transport,
	}

	for _, filter := range strings.Split(filterString, ",") {
		f, err := NewFilter(filter)
		if err != nil {
			return nil, err
		}
		s.filters = append(s.filters, f)
	}

	q := requestURL.Query()
	q.Set("filter", filterString)
	requestURL.RawQuery = q.Encode()

	request, err := http.NewRequest("GET", requestURL.String(), nil)
	if err != nil {
		return nil, err
	}
	if authToken != "" {
		request.Header.Set(
			"Cookie",
			fmt.Sprintf("ouat=%s", authToken),
		)
	}

	response, err := client.Do(request)
	if err != nil {
		ctx.Log.Error("Error", "err", err)
		return nil, err
	}
	defer response.Body.Close()

	var msg ValidateFilterResponse
	var buf bytes.Buffer
	io.Copy(&buf, response.Body)
	ctx.Log.Debug(
		"ValidateFilterResponse",
		"status", response.Status,
		"data", buf.String(),
	)
	decoder := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	if err := decoder.Decode(&msg); err != nil || response.StatusCode != http.StatusOK {
		if err == nil {
			return nil, &UpstreamError{
				HTTPStatusCode: response.StatusCode,
				Contents:       buf.Bytes(),
			}
		}
		return nil, err
	}

	s.user = msg.User
	s.admin = msg.Admin

	for _, problemAdmin := range msg.ProblemAdmin {
		s.problemAdminMap[problemAdmin] = struct{}{}
	}

	for _, contestAdmin := range msg.ContestAdmin {
		s.contestAdminMap[contestAdmin] = struct{}{}
	}

	for _, problemsetAdmin := range msg.ProblemsetAdmin {
		s.problemsetAdminMap[problemsetAdmin] = struct{}{}
	}

	return s, nil
}

// Send returns the send channel, where messages can be added to.
func (s *Subscriber) Send() chan<- *QueuedMessage {
	return s.send
}

// Matches returns whether the provided message should be sent to the current
// subscriber.
func (s *Subscriber) Matches(msg *Message) bool {
	for _, filter := range s.filters {
		if filter.Matches(msg, s) {
			return true
		}
	}
	return false
}

// Run loops waiting for one of three events to happen: connection closure, a
// new message is ready to be delivered to this subscriber, and periodic ping
// ticks.
func (s *Subscriber) Run() {
	s.transport.Init(s.close)
	go s.transport.ReadLoop()

	ticker := time.NewTicker(time.Duration(s.ctx.Config.Broadcaster.PingPeriod))
	defer func() {
		ticker.Stop()
		s.ctx.Log.Info(
			"Subscriber gone",
			"transport", s.transport,
			"user", s.user,
			"filters", s.filters,
		)
	}()

	s.ctx.Log.Info(
		"New subscriber",
		"transport", s.transport,
		"user", s.user,
		"filters", s.filters,
	)
	for {
		select {
		case <-s.close:
			return

		case message, ok := <-s.send:
			if !ok {
				s.transport.Close()
				return
			}
			if err := s.transport.Send(message); err != nil {
				s.ctx.Log.Error("Error sending message", "err", err)
				return
			}

		case <-ticker.C:
			if err := s.transport.Ping(); err != nil {
				s.ctx.Log.Error("Write error", "err", err)
				return
			}
		}
	}
}
