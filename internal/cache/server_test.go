package cache

import (
	"fmt"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func git(t *testing.T, dir string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0",
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func mustGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := git(t, dir, args...)
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return out
}

// upstream is a stand-in GHE: git-http-backend behind basic auth.
func startUpstream(t *testing.T, root string) *httptest.Server {
	t.Helper()
	execPath := strings.TrimSpace(mustGit(t, "", "--exec-path"))
	backend := filepath.Join(execPath, "git-http-backend")
	if _, err := os.Stat(backend); err != nil {
		t.Skip("git-http-backend not available")
	}
	h := &cgi.Handler{Path: backend, Root: "/", InheritEnv: []string{"PATH", "HOME"},
		Env: []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1"}}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pw, ok := r.BasicAuth(); !ok || pw != "secret" {
			w.Header().Set("WWW-Authenticate", `Basic realm="ghe"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	}))
}

func TestPullThrough(t *testing.T) {
	tmp := t.TempDir()
	originRoot := filepath.Join(tmp, "origin")
	bare := filepath.Join(originRoot, "acme", "widgets.git")
	mustGit(t, "", "init", "--bare", "-b", "main", bare)
	work := filepath.Join(tmp, "work")
	mustGit(t, "", "clone", bare, work)
	mustGit(t, work, "checkout", "-b", "main")
	commit := func(file, content string) {
		os.WriteFile(filepath.Join(work, file), []byte(content), 0o644)
		mustGit(t, work, "add", ".")
		mustGit(t, work, "commit", "-m", "add "+file)
		mustGit(t, work, "push", "origin", "main")
	}
	commit("a.txt", "one")

	up := startUpstream(t, originRoot)
	defer up.Close()
	upURL, _ := url.Parse(up.URL)

	srv, err := New(Config{Upstream: upURL, DataDir: filepath.Join(tmp, "data"),
		AuthTTL: 0, OutageAuthTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	cs := httptest.NewServer(srv)
	defer cs.Close()
	cacheURL := func(pw string) string {
		return strings.Replace(cs.URL, "http://", "http://x-access-token:"+pw+"@", 1) + "/acme/widgets.git"
	}

	// Cold clone populates the mirror.
	c1 := filepath.Join(tmp, "c1")
	mustGit(t, "", "clone", cacheURL("secret"), c1)
	if b, _ := os.ReadFile(filepath.Join(c1, "a.txt")); string(b) != "one" {
		t.Fatalf("a.txt = %q", b)
	}

	// Default branch comes through.
	if out := mustGit(t, c1, "rev-parse", "--abbrev-ref", "HEAD"); strings.TrimSpace(out) != "main" {
		t.Fatalf("HEAD = %q", out)
	}

	// Bad credentials are rejected, not served from cache.
	if out, err := git(t, "", "clone", cacheURL("wrong"), filepath.Join(tmp, "bad")); err == nil {
		t.Fatalf("clone with bad creds succeeded:\n%s", out)
	}

	// New upstream commits show up on an incremental fetch.
	commit("b.txt", "two")
	mustGit(t, c1, "pull", "-q")
	if b, _ := os.ReadFile(filepath.Join(c1, "b.txt")); string(b) != "two" {
		t.Fatalf("b.txt = %q", b)
	}

	// Pushes are refused.
	resp, err := http.Get(cs.URL + "/acme/widgets.git/info/refs?service=git-receive-pack")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("receive-pack advertise status = %d", resp.StatusCode)
	}

	// Upstream outage: fresh clones and incremental fetches keep working.
	up.Close()
	c2 := filepath.Join(tmp, "c2")
	mustGit(t, "", "clone", cacheURL("secret"), c2)
	if b, _ := os.ReadFile(filepath.Join(c2, "b.txt")); string(b) != "two" {
		t.Fatalf("outage clone b.txt = %q", b)
	}
	mustGit(t, c1, "pull", "-q")

	// Credentials never seen succeeding are not honoured during the outage.
	if out, err := git(t, "", "clone", cacheURL("wrong"), filepath.Join(tmp, "bad2")); err == nil {
		t.Fatalf("outage clone with unseen creds succeeded:\n%s", out)
	}
}

func TestParseRequest(t *testing.T) {
	oid := strings.Repeat("a", 40)
	body := pktLine("command=fetch\n") + "0001" + pktLine("want "+oid+"\n") + pktLine("want-ref refs/heads/x\n") + pktLine("done\n") + "0000"
	r := parseRequest([]byte(body))
	if r.command != "fetch" || len(r.wants) != 1 || r.wants[0] != oid || !r.wantRefs {
		t.Fatalf("got %+v", r)
	}
	v1 := parseRequest([]byte(pktLine("want "+oid+" multi_ack side-band-64k\n") + "0000" + pktLine("done\n")))
	if v1.command != "" || len(v1.wants) != 1 || v1.wants[0] != oid {
		t.Fatalf("v1 got %+v", v1)
	}
}

func evictionFixture(t *testing.T, low float64) (*Server, string, func(string) bool) {
	t.Helper()
	root := t.TempDir()
	srv, err := New(Config{Upstream: &url.URL{Scheme: "http", Host: "x"}, DataDir: root, MaxSize: 1000, EvictHigh: 0.9, EvictLow: low})
	if err != nil {
		t.Fatal(err)
	}
	srv.fsUsage = func(string) (int64, int64, error) { return 0, 0, nil } // only the size cap applies
	now := time.Now()
	for i, name := range []string{"old", "mid", "new"} {
		dir := filepath.Join(root, "acme", name+".git")
		os.MkdirAll(dir, 0o755)
		os.WriteFile(filepath.Join(dir, "blob"), make([]byte, 400), 0o644)
		os.WriteFile(filepath.Join(dir, "pull-thru-synced"), nil, 0o644)
		ts := now.Add(time.Duration(i-3) * time.Hour)
		os.Chtimes(filepath.Join(dir, "pull-thru-synced"), ts, ts)
	}
	exists := func(n string) bool { _, err := os.Stat(filepath.Join(root, "acme", n+".git")); return err == nil }
	return srv, root, exists
}

func TestEvictionLRU(t *testing.T) {
	// 1200 bytes vs 1000 cap, target 800: free >=400, so only the oldest goes.
	srv, _, exists := evictionFixture(t, 0.8)
	srv.janitorPass()
	if exists("old") || !exists("mid") || !exists("new") {
		t.Fatalf("old=%v mid=%v new=%v", exists("old"), exists("mid"), exists("new"))
	}
}

func TestEvictionSkipsReposInUse(t *testing.T) {
	// Target 500: must free >=700. mid is in use, so old and new go instead.
	srv, _, exists := evictionFixture(t, 0.5)
	release := srv.m.acquire(repoRef{"acme", "mid"})
	srv.janitorPass()
	release()
	if exists("old") || !exists("mid") || exists("new") {
		t.Fatalf("old=%v mid=%v new=%v", exists("old"), exists("mid"), exists("new"))
	}
}

func TestMetricsExposed(t *testing.T) {
	srv, err := New(Config{Upstream: &url.URL{Scheme: "http", Host: "x"}, DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	srv.metrics.requests.WithLabelValues("fetch", "hit").Inc()
	rec := httptest.NewRecorder()
	srv.MetricsHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(rec.Body.String(), `gitcache_requests_total{op="fetch",result="hit"} 1`) {
		t.Fatal(fmt.Sprint("missing metric:\n", rec.Body.String()))
	}
}

func TestRendezvousProperties(t *testing.T) {
	const keys = 3000
	counts := make([]int, 4)
	moved := 0
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("acme/repo-%d", i)
		o3, o4 := rendezvousOwner(k, 3), rendezvousOwner(k, 4)
		counts[o3]++
		if o3 != o4 {
			moved++
			if o4 != 3 {
				t.Fatalf("%s moved %d -> %d; growth must only move keys to the new shard", k, o3, o4)
			}
		}
	}
	for i := 0; i < 3; i++ {
		if counts[i] < keys*25/100 || counts[i] > keys*42/100 {
			t.Fatalf("uneven distribution: %v", counts[:3])
		}
	}
	if moved < keys*18/100 || moved > keys*32/100 {
		t.Fatalf("expected ~1/4 of keys to move on 3->4, got %d/%d", moved, keys)
	}
}

func TestShardedForwarding(t *testing.T) {
	tmp := t.TempDir()
	originRoot := filepath.Join(tmp, "origin")
	bare := filepath.Join(originRoot, "acme", "widgets.git")
	mustGit(t, "", "init", "--bare", "-b", "main", bare)
	work := filepath.Join(tmp, "work")
	mustGit(t, "", "clone", bare, work)
	mustGit(t, work, "checkout", "-b", "main")
	os.WriteFile(filepath.Join(work, "a.txt"), []byte("one"), 0o644)
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "a")
	mustGit(t, work, "push", "origin", "main")

	up := startUpstream(t, originRoot)
	defer up.Close()
	upURL, _ := url.Parse(up.URL)

	// Two replicas; start the listeners first so each knows the other's URL.
	const n = 2
	servers := make([]*httptest.Server, n)
	peers := make([]string, n)
	for i := range servers {
		servers[i] = httptest.NewUnstartedServer(nil)
		peers[i] = "http://" + servers[i].Listener.Addr().String()
	}
	dirs := make([]string, n)
	for i := range servers {
		dirs[i] = filepath.Join(tmp, fmt.Sprintf("data%d", i))
		srv, err := New(Config{Upstream: upURL, DataDir: dirs[i], OutageAuthTTL: time.Hour, Peers: peers, ShardIndex: i})
		if err != nil {
			t.Fatal(err)
		}
		servers[i].Config.Handler = srv
		servers[i].Start()
		defer servers[i].Close()
	}
	owner := rendezvousOwner("acme/widgets", n)
	other := 1 - owner
	cloneURL := func(i int) string {
		return strings.Replace(servers[i].URL, "http://", "http://x-access-token:secret@", 1) + "/acme/widgets.git"
	}
	has := func(i int) bool { _, err := os.Stat(filepath.Join(dirs[i], "acme", "widgets.git")); return err == nil }

	// Clone via the non-owner: it must proxy, and only the owner holds the mirror.
	c1 := filepath.Join(tmp, "c1")
	mustGit(t, "", "clone", cloneURL(other), c1)
	if b, _ := os.ReadFile(filepath.Join(c1, "a.txt")); string(b) != "one" {
		t.Fatalf("a.txt = %q", b)
	}
	if !has(owner) || has(other) {
		t.Fatalf("mirror placement: owner(%d)=%v other(%d)=%v", owner, has(owner), other, has(other))
	}
	// Same via the owner directly.
	mustGit(t, "", "clone", cloneURL(owner), filepath.Join(tmp, "c2"))

	// Owner down: the other replica reports unavailability rather than making a second copy.
	servers[owner].Close()
	if out, err := git(t, "", "clone", cloneURL(other), filepath.Join(tmp, "c3")); err == nil {
		t.Fatalf("clone succeeded with owner down:\n%s", out)
	}
	if has(other) {
		t.Fatal("non-owner created a mirror")
	}
}

func TestCircuitBreakerSkipsUpstream(t *testing.T) {
	hits := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	srv, err := New(Config{Upstream: u, DataDir: t.TempDir(), AuthTTL: time.Minute, OutageAuthTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ref := repoRef{"acme", "widgets"}
	srv.downUntil.Store(time.Now().Add(time.Minute).UnixNano())
	if a := srv.authorize(t.Context(), ref, "Basic x"); a.ok || a.status != 0 || hits != 0 {
		t.Fatalf("breaker open: got %+v, upstream hits %d", a, hits)
	}
	srv.downUntil.Store(0)
	if a := srv.authorize(t.Context(), ref, "Basic x"); !a.ok || hits != 1 {
		t.Fatalf("breaker closed: got %+v, upstream hits %d", a, hits)
	}
}
