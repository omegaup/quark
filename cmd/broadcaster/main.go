package main

import (
	"encoding/json"
	"expvar"
	"flag"
	"github.com/gorilla/websocket"
	"github.com/lhchavez/quark/broadcaster"
	"github.com/lhchavez/quark/common"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
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

func main() {
	flag.Parse()

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := context()
	expvar.Publish("config", &ctx.Config)

	b := broadcaster.NewBroadcaster(ctx, &PrometheusMetrics{})

	http.Handle("/metrics", prometheus.Handler())
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
		if !b.Broadcast(&message) {
			ctx.Log.Error("Error sending message, queue too large")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
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
	ctx.Log.Info("omegaUp broadcaster started", "port", ctx.Config.Broadcaster.EventsPort)
	go b.Run()
	go common.RunServer(
		&ctx.Config.Broadcaster.TLS,
		eventsMux,
		ctx.Config.Broadcaster.EventsPort,
		ctx.Config.Broadcaster.Proxied,
	)
	common.RunServer(&ctx.Config.TLS, nil, ctx.Config.Broadcaster.Port, *insecure)
}
