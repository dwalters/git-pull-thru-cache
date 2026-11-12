# git-pull-thru-cache

Read-only pull-through cache for git over smart HTTP (built for GHE). Serves
clones and fetches from a local bare mirror and keeps serving them when the
upstream is down.

    git-pull-thru-cache -upstream https://ghe.example.com -data-dir /data

Client side (PATs and GitHub App installation tokens both work, since
credentials are passed straight through):

    git config --global url."https://cache.internal/".insteadOf https://ghe.example.com/

## Behaviour

- Every request is authorised against the upstream (`info/refs`, result trusted
  for `-auth-ttl`). Credentials that passed within `-outage-auth-ttl` are still
  honoured while the upstream is unreachable. Unknown credentials are never served.
- `ls-refs` / v1 ref advertisement: mirror is synced first (coalesced per repo).
- `fetch`: served from the mirror if it has every wanted object, otherwise synced first.
- Upstream down: served from the mirror as-is (stale refs). Repos never synced return 503.
- Pushes (`git-receive-pack`) get 403.

## Storage

Mirrors live on the PVC. A janitor (`-janitor-interval`) tracks size and, when the
filesystem passes `-evict-high` (default 85%), evicts least-recently-used repos
until it is under `-evict-low` (70%). `-max-size-bytes` adds an optional cap on
mirror bytes. Repos in use are skipped, and nothing is evicted while the upstream
is down (the mirror may be the only copy). An evicted repo is simply re-fetched
on next use.

## Metrics

`:9090/metrics` (`-metrics-listen`): `gitcache_requests_total{op,result}`,
`gitcache_auth_checks_total`, `gitcache_sync_duration_seconds`,
`gitcache_sync_errors_total`, `gitcache_upstream_up`, `gitcache_mirror_bytes`,
`gitcache_mirror_repositories`, `gitcache_filesystem_used_ratio`,
`gitcache_evictions_total`.

## Deploy

    helm install gitcache charts/git-pull-thru-cache --set upstream=https://ghe.example.com

## Sharding

With `replicaCount > 1` each repo is owned by one pod (rendezvous hash of
`org/repo`); a request arriving at any other pod is reverse-proxied to the owner
over the headless Service, so clients (in or out of the cluster) just use the
normal Service or Route. One copy per repo: if the owner is down its repos
return 503. Changing `replicaCount` moves ~1/N of repos; they are re-fetched on
first use and the old copies age out via eviction. Don't scale during an
upstream outage. Forwarded requests carry `X-Gitcache-Forwarded` and are always
served locally by the receiver, so mismatched replica counts mid-rollout can't loop.

## Limits / TODO

- Per-repo locking is in-process, which is why repos have exactly one owner pod. Mirrors need a persistent volume.
- First request for an uncached repo blocks on a full mirror fetch.
- Mirror holds all upstream refs, including `refs/pull/*`.
- Sharded mode keeps one copy per repo, so a down pod makes its repos unavailable.
