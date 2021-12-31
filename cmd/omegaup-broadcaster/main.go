package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"math"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/omegaup/quark/broadcaster"
	"github.com/omegaup/quark/common"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	version    = flag.Bool("version", false, "Print the version and exit")
	insecure   = flag.Bool("insecure", false, "Do not use TLS")
	configPath = flag.String(
		"config",
		"/etc/omegaup/broadcaster/config.json",
		"Broadcaster configuration file",
	)
	globalContext atomic.Value
	upgrader      = websocket.Upgrader{
		Subprotocols: []string{"com.omegaup.events"},
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	// ProgramVersion is the version of the code from which the binary was built from.
	ProgramVersion string
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

func broadcasterContext() *common.Context {
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
	ctx.Log.Info(
		"Requesting scoreboard update",
		map[string]interface{}{
			"contest": contestAlias,
		},
	)
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
			map[string]interface{}{
				"contest": contestAlias,
				"err":     err,
			},
		)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		ctx.Log.Error(
			"Failed to request scoreboard update",
			map[string]interface{}{
				"contest":     contestAlias,
				"status code": resp.StatusCode,
			},
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
				deadline:     time.Now().Add(time.Duration(ctx.Config.Broadcaster.ScoreboardUpdateTimeout)),
			})
			if len(events) == 1 {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(time.Duration(ctx.Config.Broadcaster.ScoreboardUpdateTimeout))
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

	if *version {
		fmt.Printf("omegaup-broadcaster %s\n", ProgramVersion)
		return
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := broadcasterContext()
	expvar.Publish("config", &ctx.Config)

	b := broadcaster.NewBroadcaster(ctx, &PrometheusMetrics{})
	contestChan := make(chan string, 1)

	client := http.Client{}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	go func() {
		addr := fmt.Sprintf(":%d", ctx.Config.Metrics.Port)
		err := http.ListenAndServe(addr, metricsMux)
		if !errors.Is(err, http.ErrServerClosed) {
			ctx.Log.Error(
				"http listen and serve",
				map[string]interface{}{
					"err": err,
				},
			)
		}
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
			ctx.Log.Error(
				"Error decoding broadcast message",
				map[string]interface{}{
					"err": err,
				},
			)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx.Log.Debug(
			"/broadcast/",
			map[string]interface{}{
				"message": message,
			},
		)
		if !b.Broadcast(&message) {
			ctx.Log.Error("Error sending message, queue too large", nil)
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
		ctx := broadcasterContext()

		var auth broadcaster.Authorization
		if ouat, _ := r.Cookie("ouat"); ouat != nil {
			auth.Cookie = ouat.Value
		} else if authorization := r.Header.Get("Authorization"); strings.HasPrefix(authorization, "token ") {
			auth.APIToken = strings.TrimPrefix(authorization, "token ")
		} else if token, _ := r.Cookie("api_token"); ouat != nil {
			// WebSockets don't allow to specify additional headers, which means that
			// we need to also be able to send the API token through cookies.
			auth.APIToken = token.Value
		}

		var transport broadcaster.Transport

		if common.AcceptsMimeType(r, "text/event-stream") {
			transport = broadcaster.NewSSETransport(w)
		} else {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				ctx.Log.Error(
					"Failed to upgrade connection",
					map[string]interface{}{
						"err": err,
					},
				)
				return
			}
			defer conn.Close()

			transport = broadcaster.NewWebSocketTransport(
				conn,
				time.Duration(ctx.Config.Broadcaster.WriteDeadline),
			)
		}

		subscriber, err := broadcaster.NewSubscriber(
			ctx,
			&client,
			mustParseURL(
				ctx.Config.Broadcaster.FrontendURL,
				"api/user/validateFilter/",
			),
			auth,
			strings.Join(r.URL.Query()["filter"], ","),
			transport,
		)
		if err != nil {
			ctx.Log.Error(
				"Failed to create subscriber",
				map[string]interface{}{
					"err": err,
				},
			)
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
	go b.Run()

	var servers []*http.Server
	var wg sync.WaitGroup
	servers = append(
		servers,
		common.RunServer(
			&ctx.Config.Broadcaster.TLS,
			eventsMux,
			&wg,
			fmt.Sprintf(":%d", ctx.Config.Broadcaster.EventsPort),
			ctx.Config.Broadcaster.Proxied,
		),
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
	servers = append(
		servers,
		common.RunServer(
			&ctx.Config.Broadcaster.TLS,
			nil,
			&wg,
			fmt.Sprintf(":%d", ctx.Config.Broadcaster.Port),
			*insecure,
		),
	)

	ctx.Log.Info(
		"omegaUp broadcaster ready",
		map[string]interface{}{
			"version":          ProgramVersion,
			"broadcaster port": ctx.Config.Broadcaster.Port,
			"events port":      ctx.Config.Broadcaster.EventsPort,
		},
	)
	daemon.SdNotify(false, "READY=1")

	<-stopChan

	daemon.SdNotify(false, "STOPPING=1")
	ctx.Log.Info("Shutting down server...", nil)
	cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, server := range servers {
		server.Shutdown(cancelCtx)
	}

	cancel()
	wg.Wait()

	ctx.Log.Info("Server gracefully stopped.", nil)
}
