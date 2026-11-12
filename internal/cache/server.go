// Package cache implements a read-only pull-through cache for git over smart
// HTTP. Requests are authorised against the upstream, then served from a local
// bare mirror that is refreshed on demand. If the upstream is unreachable,
// recently authorised callers are served from the mirror as-is.
package cache

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Upstream        *url.URL      // e.g. https://ghe.example.com
	DataDir         string        // where bare mirrors live
	CAFile          string        // optional PEM bundle for the upstream
	AuthTTL         time.Duration // how long a successful upstream auth check is trusted
	OutageAuthTTL   time.Duration // how long a past success authorises access when upstream is down
	RefreshInterval time.Duration // minimum spacing between ls-refs-triggered syncs
	EvictHigh       float64       // start evicting above this used fraction (0 < low < high <= 1)
	EvictLow        float64       // evict down to this used fraction
	MaxSize         int64         // optional cap on total mirror bytes; 0 = filesystem watermarks only
	JanitorInterval time.Duration
	Logger          *slog.Logger

	// Sharding: Peers lists every replica's base URL by ordinal (including
	// this one). With more than one peer, repos are owned by one replica
	// (rendezvous hash) and requests to the others are proxied to it.
	Peers      []string
	ShardIndex int // this replica's position in Peers
}

type Server struct {
	cfg    Config
	log    *slog.Logger
	client *http.Client
	m      *mirrors
	auth   *authCache

	downUntil  atomic.Int64 // unix nanos: skip upstream checks until then (circuit breaker)
	shard      *sharder     // nil when not sharded
	metrics    *metrics
	upstreamUp atomic.Bool
	fsUsage    func(path string) (used, total int64, err error) // swappable in tests
}

func New(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// Fail fast when the upstream is down: a blackholed connect would otherwise
	// hold every request for the client timeout.
	tr.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	tr.TLSHandshakeTimeout = 5 * time.Second
	tr.ResponseHeaderTimeout = 15 * time.Second
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in %s", cfg.CAFile)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool}
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	if cfg.EvictHigh == 0 {
		cfg.EvictHigh = 0.85
	}
	if cfg.EvictLow == 0 {
		cfg.EvictLow = 0.70
	}
	if cfg.EvictLow >= cfg.EvictHigh || cfg.EvictHigh > 1 {
		return nil, fmt.Errorf("need 0 < evict-low (%v) < evict-high (%v) <= 1", cfg.EvictLow, cfg.EvictHigh)
	}
	if cfg.JanitorInterval == 0 {
		cfg.JanitorInterval = 2 * time.Minute
	}
	mt := newMetrics()
	s := &Server{
		cfg:     cfg,
		log:     cfg.Logger,
		client:  &http.Client{Transport: tr, Timeout: 30 * time.Second},
		m:       newMirrors(cfg.DataDir, cfg.Upstream, cfg.CAFile, cfg.RefreshInterval, mt),
		auth:    &authCache{m: map[[32]byte]time.Time{}},
		metrics: mt,
		fsUsage: fsUsage,
	}
	s.upstreamUp.Store(true)
	mt.upstreamUp.Set(1)
	if len(cfg.Peers) > 1 {
		sh, err := newSharder(s, cfg.ShardIndex, cfg.Peers)
		if err != nil {
			return nil, err
		}
		s.shard = sh
	}
	return s, nil
}

// MetricsHandler serves Prometheus metrics; mount it on a non-public port.
func (s *Server) MetricsHandler() http.Handler { return s.metrics.handler() }

func (s *Server) setUpstreamUp(up bool) {
	s.upstreamUp.Store(up)
	if up {
		s.metrics.upstreamUp.Set(1)
	} else {
		s.metrics.upstreamUp.Set(0)
	}
}

var (
	routeRE = regexp.MustCompile(`^/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+?)(?:\.git)?/(info/refs|git-upload-pack|git-receive-pack)$`)
	protoRE = regexp.MustCompile(`^[A-Za-z0-9=:,._-]*$`)
)

const (
	maxBody     = 256 << 20
	downBackoff = 10 * time.Second // how long a failed upstream check is trusted
)

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		fmt.Fprintln(w, "ok")
		return
	}
	m := routeRE.FindStringSubmatch(r.URL.Path)
	if m == nil || strings.Trim(m[1], ".") == "" || strings.Trim(m[2], ".") == "" {
		http.NotFound(w, r)
		return
	}
	ref, op := repoRef{m[1], m[2]}, m[3]
	service := r.URL.Query().Get("service")

	switch {
	case op == "git-receive-pack" || service == "git-receive-pack":
		http.Error(w, "read-only mirror: pushes must go to the upstream", http.StatusForbidden)
	case op == "info/refs" && r.Method == http.MethodGet && service == "git-upload-pack":
		if s.forward(w, r, ref) {
			return
		}
		s.handleAdvertise(w, r, ref)
	case op == "git-upload-pack" && r.Method == http.MethodPost:
		if s.forward(w, r, ref) {
			return
		}
		s.handleUploadPack(w, r, ref)
	case op == "info/refs":
		http.Error(w, "only the smart HTTP protocol is supported", http.StatusForbidden)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleAdvertise(w http.ResponseWriter, r *http.Request, ref repoRef) {
	authHdr := r.Header.Get("Authorization")
	a := s.authorize(r.Context(), ref, authHdr)
	if !a.ok {
		s.metrics.requests.WithLabelValues("advertise", "denied").Inc()
		s.deny(w, a, authHdr == "")
		return
	}
	defer s.m.acquire(ref)()
	proto := cleanProto(r.Header.Get("Git-Protocol"))
	v2 := strings.Contains(proto, "version=2")

	// v2 advertises capabilities only, so it needs no fresh refs; v1 does.
	// Either way an unsynced mirror must be populated first.
	result := "hit"
	if !v2 || !s.m.Synced(ref) {
		result = "stale"
		if a.upstreamUp {
			if err := s.m.Sync(r.Context(), ref, authHdr, false); err != nil {
				s.log.Warn("sync failed", "repo", ref.key(), "err", err)
			} else {
				result = "synced"
			}
		}
	}
	if !s.m.Synced(ref) {
		s.metrics.requests.WithLabelValues("advertise", "unavailable").Inc()
		http.Error(w, "upstream unavailable and no cached copy of this repository", http.StatusServiceUnavailable)
		return
	}
	s.metrics.requests.WithLabelValues("advertise", result).Inc()
	w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
	w.Header().Set("Cache-Control", "no-cache")
	if !v2 {
		io.WriteString(w, pktLine("# service=git-upload-pack\n")+"0000")
	}
	if err := s.m.UploadPack(r.Context(), ref, proto, true, nil, w); err != nil {
		s.log.Error("advertise failed", "repo", ref.key(), "err", err)
	}
}

func (s *Server) handleUploadPack(w http.ResponseWriter, r *http.Request, ref repoRef) {
	authHdr := r.Header.Get("Authorization")
	a := s.authorize(r.Context(), ref, authHdr)
	if !a.ok {
		s.metrics.requests.WithLabelValues("upload-pack", "denied").Inc()
		s.deny(w, a, authHdr == "")
		return
	}
	defer s.m.acquire(ref)()

	var body io.Reader = http.MaxBytesReader(w, r.Body, maxBody)
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(body)
		if err != nil {
			http.Error(w, "bad gzip body", http.StatusBadRequest)
			return
		}
		body = zr
	}
	raw, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}
	req := parseRequest(raw)
	proto := cleanProto(r.Header.Get("Git-Protocol"))

	// Decide whether the mirror is good enough for this request.
	var needSync, force bool
	switch {
	case req.command == "ls-refs":
		needSync = true // refs must be fresh, else builds silently use old commits
	case req.command == "" || req.command == "fetch":
		if req.wantRefs || !s.m.Synced(ref) || !s.m.HasObjects(r.Context(), ref, req.wants) {
			needSync, force = true, true
		}
	}
	outcome := "hit"
	if needSync && a.upstreamUp {
		if err := s.m.Sync(r.Context(), ref, authHdr, force); err != nil {
			s.log.Warn("sync failed", "repo", ref.key(), "err", err)
			outcome = "stale"
		} else {
			outcome = "synced"
		}
	} else if needSync {
		outcome = "stale"
	}
	if !s.m.Synced(ref) || (outcome == "stale" && force && !s.m.HasObjects(r.Context(), ref, req.wants)) {
		s.metrics.requests.WithLabelValues(opLabel(req), "unavailable").Inc()
		http.Error(w, "upstream unavailable and the cache lacks the requested objects", http.StatusServiceUnavailable)
		return
	}
	s.metrics.requests.WithLabelValues(opLabel(req), outcome).Inc()
	s.log.Info("upload-pack", "repo", ref.key(), "command", req.command, "wants", len(req.wants), "cache", outcome)

	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.Header().Set("Cache-Control", "no-cache")
	if err := s.m.UploadPack(r.Context(), ref, proto, false, bytes.NewReader(raw), w); err != nil {
		s.log.Error("upload-pack failed", "repo", ref.key(), "err", err)
	}
}

func opLabel(r request) string {
	switch r.command {
	case "ls-refs", "fetch":
		return r.command
	case "":
		return "fetch-v1"
	}
	return "other"
}

func cleanProto(p string) string {
	if !protoRE.MatchString(p) {
		return ""
	}
	return p
}

// authResult is the outcome of checking a caller's credentials upstream.
type authResult struct {
	ok         bool
	upstreamUp bool
	status     int // upstream status to relay when !ok; 0 means upstream unavailable
	wwwAuth    string
}

// authorize asks the upstream whether these credentials may read the repo.
// This is what keeps the cache from leaking private repos: every request is
// gated on a (briefly cached) upstream decision.
func (s *Server) authorize(ctx context.Context, ref repoRef, authHdr string) authResult {
	key := sha256.Sum256([]byte(authHdr + "\x00" + ref.key()))
	if t, ok := s.auth.get(key); ok && time.Since(t) < s.cfg.AuthTTL {
		s.metrics.authChecks.WithLabelValues("cached").Inc()
		return authResult{ok: true, upstreamUp: true}
	}

	// outage answers from the authorisation cache alone.
	outage := func() authResult {
		s.setUpstreamUp(false)
		if t, ok := s.auth.get(key); ok && time.Since(t) < s.cfg.OutageAuthTTL {
			s.metrics.authChecks.WithLabelValues("outage_allowed").Inc()
			return authResult{ok: true, upstreamUp: false}
		}
		s.metrics.authChecks.WithLabelValues("outage_denied").Inc()
		return authResult{}
	}
	// After a failure, don't make every request wait on a dead upstream.
	if time.Now().UnixNano() < s.downUntil.Load() {
		return outage()
	}

	u := s.cfg.Upstream.JoinPath(ref.urlPath(), "info", "refs")
	u.RawQuery = "service=git-upload-pack"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	req.Header.Set("Git-Protocol", "version=2")
	req.Header.Set("User-Agent", "git-pull-thru-cache")
	if authHdr != "" {
		req.Header.Set("Authorization", authHdr)
	}
	resp, err := s.client.Do(req)
	if resp != nil {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
	}
	if err != nil || resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		if err != nil {
			s.log.Warn("upstream unreachable", "repo", ref.key(), "err", err)
		}
		s.downUntil.Store(time.Now().Add(downBackoff).UnixNano())
		return outage()
	}
	s.downUntil.Store(0)
	s.setUpstreamUp(true)
	if resp.StatusCode == http.StatusOK {
		s.metrics.authChecks.WithLabelValues("allowed").Inc()
		s.auth.put(key, s.cfg.OutageAuthTTL)
		return authResult{ok: true, upstreamUp: true}
	}
	s.metrics.authChecks.WithLabelValues("denied").Inc()
	s.auth.del(key)
	return authResult{status: resp.StatusCode, wwwAuth: resp.Header.Get("WWW-Authenticate"), upstreamUp: true}
}

func (s *Server) deny(w http.ResponseWriter, a authResult, noCreds bool) {
	switch {
	case a.status == 0 && noCreds:
		// Upstream is down and we can't vouch for an anonymous caller. Ask for
		// credentials so git retries with them rather than giving up on a 503.
		w.Header().Set("WWW-Authenticate", `Basic realm="git-pull-thru-cache"`)
		http.Error(w, "authentication required", http.StatusUnauthorized)
	case a.status == 0:
		http.Error(w, "upstream unavailable and no recent authorisation for these credentials", http.StatusServiceUnavailable)
	default:
		if a.wwwAuth != "" {
			w.Header().Set("WWW-Authenticate", a.wwwAuth)
		}
		status := a.status
		if status < 400 {
			status = http.StatusBadGateway
		}
		http.Error(w, http.StatusText(status), status)
	}
}

// authCache remembers when a (credential, repo) pair last passed upstream.
type authCache struct {
	mu sync.Mutex
	m  map[[32]byte]time.Time
}

func (a *authCache) get(k [32]byte) (time.Time, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.m[k]
	return t, ok
}

func (a *authCache) put(k [32]byte, retain time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	a.m[k] = now
	if len(a.m) > 10000 {
		for kk, t := range a.m {
			if now.Sub(t) > retain {
				delete(a.m, kk)
			}
		}
	}
}

func (a *authCache) del(k [32]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.m, k)
}
