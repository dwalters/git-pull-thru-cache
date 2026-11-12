package cache

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"
)

// forwardedHeader marks a request already forwarded by a peer. Such requests
// are always served locally, so replicas that disagree about the shard count
// (mid-rollout) can't bounce a request between them.
const forwardedHeader = "X-Gitcache-Forwarded"

// rendezvousOwner picks the shard owning key. Rendezvous (highest random
// weight) hashing means that growing from N to N+1 shards only moves keys to
// the new shard, and shrinking only moves the removed shard's keys.
func rendezvousOwner(key string, n int) int {
	best, bestScore := 0, uint64(0)
	for i := 0; i < n; i++ {
		h := sha256.Sum256([]byte(key + "\x00" + strconv.Itoa(i)))
		if s := binary.BigEndian.Uint64(h[:8]); i == 0 || s > bestScore {
			best, bestScore = i, s
		}
	}
	return best
}

type sharder struct {
	self    int
	count   int
	proxies []*httputil.ReverseProxy // by shard index; nil for self
}

func newSharder(s *Server, self int, peers []string) (*sharder, error) {
	n := len(peers)
	if self < 0 || self >= n {
		return nil, fmt.Errorf("shard index %d out of range for %d peers", self, n)
	}
	tr := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
	}
	sh := &sharder{self: self, count: n, proxies: make([]*httputil.ReverseProxy, n)}
	for i, p := range peers {
		if i == self {
			continue
		}
		u, err := url.Parse(p)
		if err != nil || u.Host == "" {
			return nil, fmt.Errorf("bad peer URL %q", p)
		}
		rp := httputil.NewSingleHostReverseProxy(u)
		rp.Transport = tr
		rp.FlushInterval = -1 // stream pack data
		rp.ModifyResponse = func(*http.Response) error {
			s.metrics.forwarded.WithLabelValues("ok").Inc()
			return nil
		}
		shard := i
		rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			s.metrics.forwarded.WithLabelValues("error").Inc()
			s.log.Warn("shard owner unreachable", "shard", shard, "err", err)
			http.Error(w, "the cache replica that owns this repository is unavailable", http.StatusServiceUnavailable)
		}
		sh.proxies[i] = rp
	}
	return sh, nil
}

// forward proxies the request to the owning replica. It returns true if it
// handled the request.
func (s *Server) forward(w http.ResponseWriter, r *http.Request, ref repoRef) bool {
	if s.shard == nil || r.Header.Get(forwardedHeader) != "" {
		return false
	}
	owner := rendezvousOwner(ref.key(), s.shard.count)
	if owner == s.shard.self {
		return false
	}
	r.Header.Set(forwardedHeader, "1")
	s.shard.proxies[owner].ServeHTTP(w, r)
	return true
}
