package broadcaster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/lhchavez/quark/common"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type UpstreamError struct {
	HTTPStatusCode int
	Contents       []byte
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("http %d", e.HTTPStatusCode)
}

type QueuedMessage struct {
	time    time.Time
	metrics Metrics
	message *Message
}

func (m *QueuedMessage) Processed() {
	m.metrics.ObserveProcessMessageLatency(time.Since(m.time))
}

func (m *QueuedMessage) Dispatched() {
	m.metrics.ObserveDispatchMessageLatency(time.Since(m.time))
}

type Message struct {
	Contest string `json:"contest,omitempty"`
	Problem string `json:"problem,omitempty"`
	User    string `json:"user,omitempty"`
	Public  bool   `json:"public"`
	Message string `json:"message"`
}

type ValidateFilterResponse struct {
	User         string   `json:"user"`
	Admin        bool     `json:"admin"`
	ProblemAdmin []string `json:"problem_admin"`
	ContestAdmin []string `json:"contest_admin"`
}

type Metrics interface {
	IncrementWebSocketsCount(delta int)
	IncrementSSECount(delta int)
	IncrementMessagesCount()
	IncrementChannelDropCount()
	ObserveDispatchMessageLatency(latency time.Duration)
	ObserveProcessMessageLatency(latency time.Duration)
}

type Broadcaster struct {
	ctx         *common.Context
	subscribers map[*Subscriber]struct{}
	deauth      chan string
	messages    chan *QueuedMessage
	subscribe   chan *Subscriber
	unsubscribe chan *Subscriber
	metrics     Metrics
}

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

func (b *Broadcaster) Deauthenticate(user string) {
	b.deauth <- user
}

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

type Subscriber struct {
	authToken       string
	user            string
	admin           bool
	problemAdminMap map[string]struct{}
	contestAdminMap map[string]struct{}
	filters         []Filter

	ctx       *common.Context
	close     chan struct{}
	send      chan *QueuedMessage
	transport Transport
}

func NewSubscriber(
	ctx *common.Context,
	authToken string,
	filterString string,
	transport Transport,
) (*Subscriber, error) {
	s := &Subscriber{
		ctx:             ctx,
		authToken:       authToken,
		problemAdminMap: make(map[string]struct{}),
		contestAdminMap: make(map[string]struct{}),
		filters:         make([]Filter, 0),
		close:           make(chan struct{}, 0),
		send:            make(chan *QueuedMessage, ctx.Config.Broadcaster.ChannelLength),
		transport:       transport,
	}

	for _, filter := range strings.Split(filterString, ",") {
		f, err := NewFilter(filter)
		if err != nil {
			return nil, err
		}
		s.filters = append(s.filters, f)
	}

	requestURL := mustParseURL(
		ctx.Config.Broadcaster.FrontendURL,
		"api/user/validateFilter/",
	)

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

	var client http.Client
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

	return s, nil
}

func (s *Subscriber) Send() chan<- *QueuedMessage {
	return s.send
}

func (s *Subscriber) Matches(msg *Message) bool {
	for _, filter := range s.filters {
		if filter.Matches(msg, s) {
			return true
		}
	}
	return false
}

func (s *Subscriber) Run() {
	s.transport.Init(s.close)
	go s.transport.ReadLoop()

	ticker := time.NewTicker(
		time.Duration(s.ctx.Config.Broadcaster.PingPeriod) * time.Second,
	)
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

func mustParseURL(rawurl string, relative ...string) *url.URL {
	parsed, err := url.Parse(rawurl)
	if err != nil {
		panic(err)
	}
	for _, rel := range relative {
		parsed, err = parsed.Parse(rel)
		if err != nil {
			panic(err)
		}
	}
	return parsed
}
