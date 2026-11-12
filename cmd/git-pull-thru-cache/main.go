package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dwalters/git-pull-thru-cache/internal/cache"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var (
		listen   = flag.String("listen", env("GPTC_LISTEN", ":8080"), "listen address")
		upstream = flag.String("upstream", env("GPTC_UPSTREAM", ""), "upstream base URL, e.g. https://ghe.example.com")
		dataDir  = flag.String("data-dir", env("GPTC_DATA_DIR", "./data"), "directory for bare mirrors")
		caFile   = flag.String("ca-file", env("GPTC_CA_FILE", ""), "PEM CA bundle for the upstream")
		authTTL  = flag.Duration("auth-ttl", 60*time.Second, "trust a successful upstream auth check for this long")
		outage   = flag.Duration("outage-auth-ttl", 24*time.Hour, "while upstream is down, honour credentials that passed within this window")
		refresh  = flag.Duration("refresh-interval", time.Second, "minimum spacing between ref-listing-triggered syncs")
		metricsL = flag.String("metrics-listen", env("GPTC_METRICS_LISTEN", ":9090"), "metrics listen address (empty disables)")
		evHigh   = flag.Float64("evict-high", 0.85, "start evicting least-recently-used repos above this used fraction of the filesystem")
		evLow    = flag.Float64("evict-low", 0.70, "evict down to this used fraction")
		maxSize  = flag.Int64("max-size-bytes", 0, "optional cap on total mirror size (evict-high/low apply to it too); 0 = off")
		shardN   = flag.Int("shard-count", 1, "number of replicas; repos are sharded across them when > 1")
		shardIdx = flag.Int("shard-index", -1, "this replica's ordinal; -1 derives it from the HOSTNAME suffix (StatefulSet)")
		peerTmpl = flag.String("shard-peer-template", "", "peer base URL with %d for the ordinal, e.g. http://cache-%d.cache-headless.ns.svc:8080")
		janitor  = flag.Duration("janitor-interval", 2*time.Minute, "storage accounting/eviction interval")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	u, err := url.Parse(*upstream)
	if err != nil || u.Scheme == "" || u.Host == "" {
		log.Error("-upstream must be an absolute URL")
		os.Exit(2)
	}
	var peers []string
	idx := *shardIdx
	if *shardN > 1 {
		if !strings.Contains(*peerTmpl, "%d") {
			log.Error("-shard-peer-template containing %d is required when -shard-count > 1")
			os.Exit(2)
		}
		for i := 0; i < *shardN; i++ {
			peers = append(peers, fmt.Sprintf(*peerTmpl, i))
		}
		if idx < 0 {
			host, _ := os.Hostname()
			n, err := strconv.Atoi(host[strings.LastIndex(host, "-")+1:])
			if err != nil {
				log.Error("cannot derive shard index from hostname; set -shard-index", "hostname", host)
				os.Exit(2)
			}
			idx = n
		}
	}
	srv, err := cache.New(cache.Config{
		Peers: peers, ShardIndex: idx,
		Upstream: u, DataDir: *dataDir, CAFile: *caFile,
		AuthTTL: *authTTL, OutageAuthTTL: *outage, RefreshInterval: *refresh, Logger: log,
		EvictHigh: *evHigh, EvictLow: *evLow, MaxSize: *maxSize, JanitorInterval: *janitor,
	})
	if err != nil {
		log.Error("init failed", "err", err)
		os.Exit(1)
	}

	hs := &http.Server{Addr: *listen, Handler: srv, ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go srv.RunJanitor(ctx)
	var ms *http.Server
	if *metricsL != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", srv.MetricsHandler())
		ms = &http.Server{Addr: *metricsL, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := ms.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics server failed", "err", err)
			}
		}()
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		hs.Shutdown(sctx)
		if ms != nil {
			ms.Shutdown(sctx)
		}
	}()
	log.Info("listening", "addr", *listen, "upstream", u.String())
	if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
}
