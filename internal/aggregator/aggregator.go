package aggregator

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"trending/internal/config"
	"trending/internal/contract"
	"trending/internal/normalize"
	"trending/internal/stoplist"
)

type QueryCount struct {
	Query string `json:"query"`
	Count int64  `json:"count"`
}

type TopResponse struct {
	WindowSeconds int          `json:"window_seconds"`
	GeneratedAt   time.Time    `json:"generated_at"`
	Items         []QueryCount `json:"items"`
}

type bucket struct {
	counts map[string]int64
	actors map[string]map[string]int
}

type cachedTop struct {
	at    time.Time
	items []QueryCount
}

type Aggregator struct {
	cfg      config.Config
	stoplist *stoplist.List

	mu           sync.RWMutex
	buckets      []bucket
	currentIndex int
	lastRotate   time.Time
	bucketDur    time.Duration

	topMu    sync.RWMutex
	topCache cachedTop
}

func New(cfg config.Config, sl *stoplist.List) *Aggregator {
	if sl == nil {
		sl = stoplist.New()
	}
	buckets := make([]bucket, cfg.BucketCount)
	for i := range buckets {
		buckets[i] = newBucket()
	}
	return &Aggregator{
		cfg:          cfg,
		stoplist:     sl,
		buckets:      buckets,
		currentIndex: 0,
		lastRotate:   time.Now().UTC(),
		bucketDur:    cfg.WindowDuration / time.Duration(cfg.BucketCount),
	}
}

func newBucket() bucket {
	return bucket{
		counts: make(map[string]int64),
		actors: make(map[string]map[string]int),
	}
}

func (a *Aggregator) Start(ctx context.Context) {
	ticker := time.NewTicker(a.bucketDur)
	defer ticker.Stop()

	cacheTicker := time.NewTicker(a.cfg.TopCacheRefresh)
	defer cacheTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.rotateBucket()
		case <-cacheTicker.C:
			a.refreshTopCache(a.cfg.MaxTopN)
		}
	}
}

func (a *Aggregator) rotateBucket() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.currentIndex = (a.currentIndex + 1) % len(a.buckets)
	a.buckets[a.currentIndex] = newBucket()
	a.lastRotate = time.Now().UTC()
}

func actorKey(sessionID string) string {
	if sessionID == "" {
		return "anon"
	}
	return "s:" + sessionID
}

func (a *Aggregator) Record(ev contract.SearchEvent) bool {
	query := normalize.Query(ev.Query)
	if query == "" {
		return false
	}
	if a.stoplist.Contains(query) {
		return false
	}

	ts := ev.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.rotateToTimeLocked(time.Now().UTC())

	age := a.lastRotate.Sub(ts)
	if age >= a.cfg.WindowDuration {
		return false
	}
	if age < 0 {
		ts = a.lastRotate
	}

	idx := a.bucketIndexForTimeLocked(ts)
	if idx < 0 {
		return false
	}
	b := &a.buckets[idx]

	actor := actorKey(ev.SessionID)

	actorCounts, ok := b.actors[query]
	if !ok {
		actorCounts = make(map[string]int)
		b.actors[query] = actorCounts
	}

	current := actorCounts[actor]
	if current >= a.cfg.MaxHitsPerActor {
		return false
	}
	actorCounts[actor] = current + 1
	b.counts[query]++
	return true
}

func (a *Aggregator) rotateToTimeLocked(now time.Time) {
	if now.Before(a.lastRotate) {
		return
	}
	steps := int(now.Sub(a.lastRotate) / a.bucketDur)
	if steps <= 0 {
		return
	}
	if steps >= len(a.buckets) {
		for i := range a.buckets {
			a.buckets[i] = newBucket()
		}
		a.currentIndex = 0
		a.lastRotate = now
		return
	}
	for i := 0; i < steps; i++ {
		a.currentIndex = (a.currentIndex + 1) % len(a.buckets)
		a.buckets[a.currentIndex] = newBucket()
	}
	a.lastRotate = a.lastRotate.Add(time.Duration(steps) * a.bucketDur)
}

func (a *Aggregator) bucketIndexForTimeLocked(ts time.Time) int {
	age := a.lastRotate.Sub(ts)
	if age < 0 {
		return a.currentIndex
	}
	offset := int(age / a.bucketDur)
	if offset >= len(a.buckets) {
		return -1
	}
	idx := a.currentIndex - offset
	if idx < 0 {
		idx += len(a.buckets)
	}
	return idx
}

func (a *Aggregator) aggregateCountsLocked() map[string]int64 {
	totals := make(map[string]int64)
	for _, b := range a.buckets {
		for q, c := range b.counts {
			totals[q] += c
		}
	}
	return totals
}

func (a *Aggregator) Top(n int) TopResponse {
	if n <= 0 {
		n = a.cfg.DefaultTopN
	}
	if n > a.cfg.MaxTopN {
		n = a.cfg.MaxTopN
	}

	a.topMu.RLock()
	cache := a.topCache
	a.topMu.RUnlock()

	var items []QueryCount
	if len(cache.items) >= n && time.Since(cache.at) < a.cfg.TopCacheRefresh*2 {
		items = append(items, cache.items[:n]...)
	} else {
		items = a.computeTop(n)
	}

	return TopResponse{
		WindowSeconds: int(a.cfg.WindowDuration.Seconds()),
		GeneratedAt:   time.Now().UTC(),
		Items:         items,
	}
}

func (a *Aggregator) refreshTopCache(n int) {
	items := a.computeTop(n)
	a.topMu.Lock()
	a.topCache = cachedTop{at: time.Now().UTC(), items: items}
	a.topMu.Unlock()
}

func (a *Aggregator) computeTop(n int) []QueryCount {
	a.mu.RLock()
	totals := a.aggregateCountsLocked()
	a.mu.RUnlock()

	filtered := make(map[string]int64, len(totals))
	for q, c := range totals {
		if !a.stoplist.Contains(q) {
			filtered[q] = c
		}
	}

	return topN(filtered, n)
}

func topN(counts map[string]int64, n int) []QueryCount {
	if n <= 0 || len(counts) == 0 {
		return nil
	}

	h := &minHeap{}
	heap.Init(h)

	for q, c := range counts {
		if h.Len() < n {
			heap.Push(h, QueryCount{Query: q, Count: c})
			continue
		}
		if c > (*h)[0].Count {
			(*h)[0] = QueryCount{Query: q, Count: c}
			heap.Fix(h, 0)
		}
	}

	items := make([]QueryCount, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		items[i] = heap.Pop(h).(QueryCount)
	}
	return items
}

type minHeap []QueryCount

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].Count < h[j].Count }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x any) {
	*h = append(*h, x.(QueryCount))
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (a *Aggregator) SnapshotCounts() map[string]int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.aggregateCountsLocked()
}
