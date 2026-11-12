package cache

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metrics struct {
	reg          *prometheus.Registry
	requests     *prometheus.CounterVec
	authChecks   *prometheus.CounterVec
	forwarded    *prometheus.CounterVec
	syncDuration prometheus.Histogram
	syncErrors   prometheus.Counter
	upstreamUp   prometheus.Gauge
	repos        prometheus.Gauge
	bytes        prometheus.Gauge
	fsUsedRatio  prometheus.Gauge
	evictions    prometheus.Counter
	evictedBytes prometheus.Counter
}

func newMetrics() *metrics {
	m := &metrics{
		reg: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gitcache_requests_total",
			Help: "Git requests served. result: hit (served from mirror), synced (upstream sync needed and succeeded), stale (sync needed but failed or upstream down; served from mirror), unavailable, denied.",
		}, []string{"op", "result"}),
		authChecks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gitcache_auth_checks_total",
			Help: "Upstream authorisation decisions. result: cached, allowed, denied, outage_allowed, outage_denied.",
		}, []string{"result"}),
		forwarded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gitcache_forwarded_requests_total",
			Help: "Requests proxied to the replica owning the repository. result: ok, error.",
		}, []string{"result"}),
		syncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "gitcache_sync_duration_seconds", Help: "Duration of mirror syncs from upstream.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2.5, 10),
		}),
		syncErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "gitcache_sync_errors_total", Help: "Failed mirror syncs."}),
		upstreamUp: prometheus.NewGauge(prometheus.GaugeOpts{Name: "gitcache_upstream_up", Help: "1 if the last upstream request got a non-5xx response."}),
		repos:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "gitcache_mirror_repositories", Help: "Repositories held in the cache."}),
		bytes:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "gitcache_mirror_bytes", Help: "Bytes used by mirrors."}),
		fsUsedRatio: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gitcache_filesystem_used_ratio", Help: "Used fraction of the filesystem holding the mirrors."}),
		evictions:    prometheus.NewCounter(prometheus.CounterOpts{Name: "gitcache_evictions_total", Help: "Repositories evicted to free space."}),
		evictedBytes: prometheus.NewCounter(prometheus.CounterOpts{Name: "gitcache_evicted_bytes_total", Help: "Bytes freed by eviction."}),
	}
	m.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.requests, m.authChecks, m.forwarded, m.syncDuration, m.syncErrors, m.upstreamUp, m.repos, m.bytes, m.fsUsedRatio, m.evictions, m.evictedBytes)
	return m
}

func (m *metrics) handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
