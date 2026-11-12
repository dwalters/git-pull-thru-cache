package cache

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const usedFile = "pull-thru-used"

// repoInfo describes one mirror on disk.
type repoInfo struct {
	key      string
	dir      string
	size     int64
	lastUsed time.Time
}

// touch records that a repo was just used, for LRU eviction. The mtime of a
// marker file is the record, so it survives restarts. Throttled per repo.
func (m *mirrors) touch(r repoRef, st *repoState) {
	now := time.Now()
	if last := st.lastTouch.Load(); now.UnixNano()-last < int64(30*time.Second) {
		return
	}
	st.lastTouch.Store(now.UnixNano())
	p := filepath.Join(m.dir(r), usedFile)
	if err := os.Chtimes(p, now, now); err != nil {
		if _, serr := os.Stat(m.dir(r)); serr == nil { // repo exists: create marker
			os.WriteFile(p, nil, 0o644)
		}
	}
}

// acquire marks a repo in use until the returned func is called. Eviction
// never removes a repo that is acquired.
func (m *mirrors) acquire(r repoRef) func() {
	st := m.state(r.key())
	st.use.RLock()
	m.touch(r, st)
	return st.use.RUnlock
}

// scan lists mirrors on disk with their sizes and last use.
func (m *mirrors) scan() []repoInfo {
	var out []repoInfo
	orgs, _ := os.ReadDir(m.root)
	for _, o := range orgs {
		if !o.IsDir() {
			continue
		}
		repos, _ := os.ReadDir(filepath.Join(m.root, o.Name()))
		for _, r := range repos {
			if !r.IsDir() || !strings.HasSuffix(r.Name(), ".git") {
				continue
			}
			dir := filepath.Join(m.root, o.Name(), r.Name())
			ri := repoInfo{key: o.Name() + "/" + strings.TrimSuffix(r.Name(), ".git"), dir: dir}
			filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
				if err == nil && !d.IsDir() {
					if fi, err := d.Info(); err == nil {
						ri.size += fi.Size()
					}
				}
				return nil
			})
			for _, f := range []string{usedFile, "pull-thru-synced"} {
				if fi, err := os.Stat(filepath.Join(dir, f)); err == nil && fi.ModTime().After(ri.lastUsed) {
					ri.lastUsed = fi.ModTime()
				}
			}
			out = append(out, ri)
		}
	}
	return out
}

// evict removes a repo unless a request is using it.
func (m *mirrors) evict(ri repoInfo) bool {
	st := m.state(ri.key)
	if !st.use.TryLock() {
		return false
	}
	defer st.use.Unlock()
	// Drop the marker first so a half-deleted repo is never served.
	os.Remove(filepath.Join(ri.dir, "pull-thru-synced"))
	return os.RemoveAll(ri.dir) == nil
}

// bytesToFree returns how much must be freed so that used drops to low*capacity,
// or 0 if used is still under high*capacity.
func bytesToFree(used, capacity int64, high, low float64) int64 {
	if capacity <= 0 || float64(used) <= high*float64(capacity) {
		return 0
	}
	return used - int64(low*float64(capacity))
}

func fsUsage(path string) (used, total int64, err error) {
	var st syscall.Statfs_t
	if err = syscall.Statfs(path, &st); err != nil {
		return
	}
	total = int64(st.Blocks) * int64(st.Bsize)
	used = total - int64(st.Bavail)*int64(st.Bsize)
	return
}

// janitorPass refreshes storage metrics and, under pressure, evicts the least
// recently used repositories. Eviction is skipped while the upstream is down:
// the mirror is then the only copy anyone can read.
func (s *Server) janitorPass() {
	repos := s.m.scan()
	var total int64
	for _, r := range repos {
		total += r.size
	}
	s.metrics.repos.Set(float64(len(repos)))
	s.metrics.bytes.Set(float64(total))

	var need int64
	if s.cfg.MaxSize > 0 {
		need = bytesToFree(total, s.cfg.MaxSize, s.cfg.EvictHigh, s.cfg.EvictLow)
	}
	if used, fsTotal, err := s.fsUsage(s.cfg.DataDir); err == nil && fsTotal > 0 {
		s.metrics.fsUsedRatio.Set(float64(used) / float64(fsTotal))
		if n := bytesToFree(used, fsTotal, s.cfg.EvictHigh, s.cfg.EvictLow); n > need {
			need = n
		}
	}
	if need <= 0 {
		return
	}
	if !s.upstreamUp.Load() {
		s.log.Warn("storage pressure but upstream is down; not evicting", "needBytes", need)
		return
	}
	sort.Slice(repos, func(i, j int) bool { return repos[i].lastUsed.Before(repos[j].lastUsed) })
	var freed int64
	for _, r := range repos {
		if freed >= need {
			break
		}
		if s.m.evict(r) {
			freed += r.size
			s.metrics.evictions.Inc()
			s.metrics.evictedBytes.Add(float64(r.size))
			s.log.Info("evicted repository", "repo", r.key, "bytes", r.size, "lastUsed", r.lastUsed)
		}
	}
	if freed < need {
		s.log.Warn("could not free enough space", "needBytes", need, "freedBytes", freed)
	}
}

// RunJanitor runs storage accounting and eviction until ctx is done.
func (s *Server) RunJanitor(ctx context.Context) {
	t := time.NewTicker(s.cfg.JanitorInterval)
	defer t.Stop()
	for {
		s.janitorPass()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
