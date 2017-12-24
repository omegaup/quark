package main

import (
	"container/heap"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/lhchavez/quark/broadcaster"
	"github.com/lhchavez/quark/common"
	"github.com/prometheus/client_golang/prometheus"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var (
	insecure   = flag.Bool("insecure", false, "Do not use TLS")
	configPath = flag.String(
		"config",
		"/etc/omegaup/broadcaster/config.json",
		"Grader configuration file",
	)
	globalContext atomic.Value
	upgrader      = websocket.Upgrader{
		Subprotocols: []string{"com.omegaup.events"},
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func loadContext() error {
	f, err := os.Open(*configPath)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx, err := common.NewContextFromReader(f)
	if err != nil {
		return err
	}
	globalContext.Store(ctx)
	return nil
}

func context() *common.Context {
	return globalContext.Load().(*common.Context)
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

type updateScoreboardEvent struct {
	contestAlias string
	deadline     time.Time
}

type updateScoreboardEventHeap []*updateScoreboardEvent

func (h updateScoreboardEventHeap) Len() int      { return len(h) }
func (h updateScoreboardEventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h updateScoreboardEventHeap) Less(i, j int) bool {
	return h[i].deadline.Before(h[j].deadline)
}

func (h *updateScoreboardEventHeap) Push(x interface{}) {
	*h = append(*h, x.(*updateScoreboardEvent))
}

func (h *updateScoreboardEventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func updateScoreboardForContest(
	ctx *common.Context,
	client *http.Client,
	updateScoreboardURL *url.URL,
	contestAlias string,
) {
	ctx.Log.Info("Requesting scoreboard update", "contest", contestAlias)
	resp, err := client.PostForm(
		updateScoreboardURL.String(),
		url.Values{
			"token": {ctx.Config.Broadcaster.ScoreboardUpdateSecret},
			"alias": {contestAlias},
		},
	)
	if err != nil {
		ctx.Log.Error(
			"Error requesting scoreboard update",
			"contest", contestAlias,
			"err", err,
		)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		ctx.Log.Error(
			"Failed to request scoreboard update",
			"contest", contestAlias,
			"status code", resp.StatusCode,
		)
	}
}

func updateScoreboardLoop(
	ctx *common.Context,
	client *http.Client,
	updateScoreboardURL *url.URL,
	contestChan <-chan string,
) {
	const infinity = time.Duration(math.MaxInt64)
	timer := time.NewTimer(infinity)
	events := updateScoreboardEventHeap{}
	eventSet := make(map[string]bool)

	for {
		select {
		case contestAlias := <-contestChan:
			if _, ok := eventSet[contestAlias]; ok {
				eventSet[contestAlias] = true
				continue
			}

			eventSet[contestAlias] = false
			heap.Push(&events, &updateScoreboardEvent{
				contestAlias: contestAlias,
				deadline:     time.Now().Add(ctx.Config.Broadcaster.ScoreboardUpdateTimeout),
			})
			if len(events) == 1 {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(ctx.Config.Broadcaster.ScoreboardUpdateTimeout)
			}

			updateScoreboardForContest(
				ctx,
				client,
				updateScoreboardURL,
				contestAlias,
			)
		case <-timer.C:
			if len(events) == 0 {
				timer.Reset(infinity)
				continue
			}
			event := heap.Pop(&events).(*updateScoreboardEvent)
			if len(events) == 0 {
				timer.Reset(infinity)
			} else {
				timer.Reset(events[0].deadline.Sub(time.Now()))
			}

			if eventSet[event.contestAlias] {
				updateScoreboardForContest(
					ctx,
					client,
					updateScoreboardURL,
					event.contestAlias,
				)
			}
			delete(eventSet, event.contestAlias)
		}
	}
}

func main() {
	flag.Parse()

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := context()
	expvar.Publish("config", &ctx.Config)

	b := broadcaster.NewBroadcaster(ctx, &PrometheusMetrics{})
	contestChan := make(chan string, 1)

	client := http.Client{}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", prometheus.Handler())
	go func() {
		addr := fmt.Sprintf(":%d", ctx.Config.Metrics.Port)
		ctx.Log.Error(
			"http listen and serve",
			"err", http.ListenAndServe(addr, metricsMux),
		)
	}()

	http.HandleFunc("/deauthenticate/", func(w http.ResponseWriter, r *http.Request) {
		pathComponents := strings.Split(r.URL.Path, "/")
		if len(pathComponents) < 3 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.Deauthenticate(pathComponents[2])
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/broadcast/", func(w http.ResponseWriter, r *http.Request) {
		if *insecure {
			w.Header().Set("Access-Control-Methods", "POST")
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		if r.Method != "POST" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var message broadcaster.Message
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&message); err != nil {
			ctx.Log.Error("Error decoding broadcast message", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx.Log.Debug("/broadcast/", "message", message)
		if !b.Broadcast(&message) {
			ctx.Log.Error("Error sending message, queue too large")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		// TODO(lhchavez): Figure out a better way of checking this.
		if len(message.Contest) > 0 && strings.Contains(message.Message, "\"message\":\"/run/update/\"") {
			contestChan <- message.Contest
		}
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		w.Write([]byte("{\"status\":\"ok\"}"))
	})

	eventsMux := http.NewServeMux()
	eventsMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context()

		authToken := ""
		if ouat, _ := r.Cookie("ouat"); ouat != nil {
			authToken = ouat.Value
		}

		var transport broadcaster.Transport

		if common.AcceptsMimeType(r, "text/event-stream") {
			transport = broadcaster.NewSSETransport(w)
		} else {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				ctx.Log.Error("Failed to upgrade connection", "err", err)
				return
			}
			defer conn.Close()

			transport = broadcaster.NewWebSocketTransport(
				conn,
				ctx.Config.Broadcaster.WriteDeadline,
			)
		}

		subscriber, err := broadcaster.NewSubscriber(
			ctx,
			&client,
			mustParseURL(
				ctx.Config.Broadcaster.FrontendURL,
				"api/user/validateFilter/",
			),
			authToken,
			strings.Join(r.URL.Query()["filter"], ","),
			transport,
		)
		if err != nil {
			ctx.Log.Error("Failed to create subscriber", "err", err)
			if upstream, ok := err.(*broadcaster.UpstreamError); ok {
				w.WriteHeader(upstream.HTTPStatusCode)
				w.Write(upstream.Contents)
			}
			return
		}
		if !b.Subscribe(subscriber) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer b.Unsubscribe(subscriber)

		subscriber.Run()
	})
	ctx.Log.Info(
		"omegaUp broadcaster started",
		"broadcaster port", ctx.Config.Broadcaster.Port,
		"events port", ctx.Config.Broadcaster.EventsPort,
	)
	go b.Run()

	eventsHost := ""
	if ctx.Config.Broadcaster.Proxied {
		eventsHost = "localhost"
	}
	go common.RunServer(
		&ctx.Config.Broadcaster.TLS,
		eventsMux,
		fmt.Sprintf("%s:%d", eventsHost, ctx.Config.Broadcaster.EventsPort),
		ctx.Config.Broadcaster.Proxied,
	)
	go updateScoreboardLoop(
		ctx,
		&client,
		mustParseURL(
			ctx.Config.Broadcaster.FrontendURL,
			"api/scoreboard/refresh/",
		),
		contestChan,
	)
	common.RunServer(
		&ctx.Config.TLS,
		nil,
		fmt.Sprintf(":%d", ctx.Config.Broadcaster.Port),
		*insecure,
	)
}
