package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// repoRef identifies a repository on the upstream.
type repoRef struct{ org, name string }

func (r repoRef) key() string     { return strings.ToLower(r.org + "/" + r.name) }
func (r repoRef) urlPath() string { return r.org + "/" + r.name + ".git" }

var oidRE = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)

const headRefresh = 5 * time.Minute

// mirrors manages a directory of bare repositories that mirror upstream repos.
type mirrors struct {
	root       string
	upstream   *url.URL
	baseEnv    []string
	refreshMin time.Duration
	metrics    *metrics

	mu     sync.Mutex
	states map[string]*repoState
}

type repoState struct {
	use       sync.RWMutex // held shared by requests, exclusively by eviction
	lastTouch atomic.Int64
	mu        sync.Mutex
	lastStart time.Time
	lastEnd   time.Time
	lastErr   error
	lastHead  time.Time
}

func newMirrors(root string, upstream *url.URL, caFile string, refreshMin time.Duration, mt *metrics) *mirrors {
	env := append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		// Arbitrary-UID containers (OpenShift) have no passwd entry, so git cannot
		// look up an identity or a home directory on its own.
		"HOME=/tmp",
		"GIT_AUTHOR_NAME=git-pull-thru-cache", "GIT_AUTHOR_EMAIL=noreply@localhost",
		"GIT_COMMITTER_NAME=git-pull-thru-cache", "GIT_COMMITTER_EMAIL=noreply@localhost",
	)
	if caFile != "" {
		env = append(env, "GIT_SSL_CAINFO="+caFile)
	}
	return &mirrors{root: root, upstream: upstream, baseEnv: env, refreshMin: refreshMin, metrics: mt, states: map[string]*repoState{}}
}

func (m *mirrors) dir(r repoRef) string {
	return filepath.Join(m.root, strings.ToLower(r.org), strings.ToLower(r.name)+".git")
}

func (m *mirrors) state(key string) *repoState {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.states[key]
	if st == nil {
		st = &repoState{}
		m.states[key] = st
	}
	return st
}

// Synced reports whether the mirror has completed at least one full sync.
// Until then it must never serve refs: an empty mirror would look like an
// empty repository to clients.
func (m *mirrors) Synced(r repoRef) bool {
	_, err := os.Stat(filepath.Join(m.dir(r), "pull-thru-synced"))
	return err == nil
}

func (m *mirrors) run(ctx context.Context, dir string, extraEnv []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(append([]string{}, m.baseEnv...), extraEnv...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func authEnv(authHeader string) []string {
	if authHeader == "" {
		return nil
	}
	// Env rather than -c so the credential never shows up in `ps`.
	return []string{
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=http.extraHeader",
		"GIT_CONFIG_VALUE_0=Authorization: " + authHeader,
	}
}

// Sync brings the mirror up to date with upstream using the caller's
// credentials. Concurrent callers coalesce: a sync that started after a
// caller asked is good enough for that caller. Unless force is set, a sync
// that finished within refreshMin is also reused.
func (m *mirrors) Sync(ctx context.Context, r repoRef, authHeader string, force bool) error {
	st := m.state(r.key())
	requested := time.Now()
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.lastStart.After(requested) {
		return st.lastErr
	}
	if !force && st.lastErr == nil && time.Since(st.lastEnd) < m.refreshMin && m.Synced(r) {
		return nil
	}
	st.lastStart = time.Now()
	err := m.doSync(ctx, st, r, authHeader)
	st.lastEnd, st.lastErr = time.Now(), err
	m.metrics.syncDuration.Observe(time.Since(st.lastStart).Seconds())
	if err != nil {
		m.metrics.syncErrors.Inc()
	}
	return err
}

func (m *mirrors) doSync(ctx context.Context, st *repoState, r repoRef, authHeader string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	dir := m.dir(r)
	if _, err := os.Stat(filepath.Join(dir, "HEAD")); err != nil {
		if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
			return err
		}
		if _, err := m.run(ctx, "", nil, "init", "--bare", "-q", dir); err != nil {
			return err
		}
	}
	up := m.upstream.JoinPath(r.urlPath()).String()
	env := authEnv(authHeader)

	if _, err := m.run(ctx, dir, env, "fetch", "--quiet", "--prune", "--force", "--no-tags", up, "+refs/*:refs/*"); err != nil {
		return err
	}

	// Plain refs/* fetches don't carry HEAD's symref; mirror it so clones
	// get the right default branch.
	_, headErr := m.run(ctx, dir, nil, "rev-parse", "-q", "--verify", "HEAD^{commit}")
	if headErr != nil || time.Since(st.lastHead) > headRefresh {
		if out, err := m.run(ctx, dir, env, "ls-remote", "--symref", up, "HEAD"); err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				if t, ok := strings.CutPrefix(line, "ref: "); ok {
					if target, _, ok := strings.Cut(t, "\t"); ok {
						if _, err := m.run(ctx, dir, nil, "symbolic-ref", "HEAD", target); err == nil {
							st.lastHead = time.Now()
						}
					}
				}
			}
		}
	}
	return os.WriteFile(filepath.Join(dir, "pull-thru-synced"), []byte(time.Now().UTC().Format(time.RFC3339)), 0o644)
}

// HasObjects reports whether every oid is present in the mirror.
func (m *mirrors) HasObjects(ctx context.Context, r repoRef, oids []string) bool {
	if len(oids) == 0 {
		return true
	}
	var in strings.Builder
	for _, o := range oids {
		if !oidRE.MatchString(o) {
			return false
		}
		in.WriteString(o + "\n")
	}
	cmd := exec.CommandContext(ctx, "git", "cat-file", "--batch-check")
	cmd.Dir = m.dir(r)
	cmd.Env = m.baseEnv
	cmd.Stdin = strings.NewReader(in.String())
	out, err := cmd.Output()
	return err == nil && !bytes.Contains(out, []byte(" missing"))
}

// UploadPack runs git upload-pack in stateless-rpc mode against the mirror.
func (m *mirrors) UploadPack(ctx context.Context, r repoRef, proto string, advertise bool, in io.Reader, out io.Writer) error {
	args := []string{
		"-c", "uploadpack.allowFilter=true",
		"-c", "uploadpack.allowRefInWant=true",
		"upload-pack", "--stateless-rpc",
	}
	if advertise {
		args = append(args, "--advertise-refs")
	}
	args = append(args, m.dir(r))
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = append(append([]string{}, m.baseEnv...), "GIT_PROTOCOL="+proto)
	cmd.Stdin, cmd.Stdout = in, out
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("upload-pack: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}
