package aggregator

import (
	"testing"
	"time"

	"trending/internal/config"
	"trending/internal/contract"
	"trending/internal/stoplist"
)

func testConfig() config.Config {
	return config.Config{
		WindowDuration:  5 * time.Minute,
		BucketCount:     5,
		MaxHitsPerActor: 2,
		DefaultTopN:     10,
		MaxTopN:         100,
		TopCacheRefresh: time.Hour,
	}
}

func TestRecordAndTop(t *testing.T) {
	cfg := testConfig()
	cfg.MaxHitsPerActor = 100
	agg := New(cfg, stoplist.New())
	now := time.Now().UTC()

	for i := 0; i < 5; i++ {
		agg.Record(contract.SearchEvent{Query: "iphone", Timestamp: now, SessionID: "s1"})
	}
	for i := 0; i < 3; i++ {
		agg.Record(contract.SearchEvent{Query: "samsung", Timestamp: now, SessionID: "s2"})
	}

	top := agg.Top(2)
	if len(top.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(top.Items))
	}
	if top.Items[0].Query != "iphone" || top.Items[0].Count != 5 {
		t.Fatalf("unexpected top[0]: %+v", top.Items[0])
	}
	if top.Items[1].Query != "samsung" || top.Items[1].Count != 3 {
		t.Fatalf("unexpected top[1]: %+v", top.Items[1])
	}
}

func TestMaxHitsPerSessionCap(t *testing.T) {
	agg := New(testConfig(), stoplist.New())
	now := time.Now().UTC()

	for i := 0; i < 10; i++ {
		agg.Record(contract.SearchEvent{Query: "boost", Timestamp: now, SessionID: "bot"})
	}

	counts := agg.SnapshotCounts()
	if counts["boost"] != 2 {
		t.Fatalf("expected cap at 2 hits per actor per bucket, got %d", counts["boost"])
	}
}

func TestStoplistExcludesFromTop(t *testing.T) {
	sl := stoplist.New("secret")
	agg := New(testConfig(), sl)
	now := time.Now().UTC()

	agg.Record(contract.SearchEvent{Query: "secret deal", Timestamp: now, SessionID: "a"})
	agg.Record(contract.SearchEvent{Query: "iphone", Timestamp: now, SessionID: "b"})
	agg.Record(contract.SearchEvent{Query: "iphone", Timestamp: now, SessionID: "c"})

	top := agg.Top(5)
	if len(top.Items) != 1 || top.Items[0].Query != "iphone" {
		t.Fatalf("stoplist query must be excluded, got %+v", top.Items)
	}
}

func TestOldEventsOutsideWindow(t *testing.T) {
	cfg := testConfig()
	cfg.BucketCount = 2
	agg := New(cfg, stoplist.New())

	old := time.Now().UTC().Add(-10 * time.Minute)
	agg.Record(contract.SearchEvent{Query: "old", Timestamp: old, SessionID: "x"})
	agg.Record(contract.SearchEvent{Query: "fresh", Timestamp: time.Now().UTC(), SessionID: "y"})

	counts := agg.SnapshotCounts()
	if counts["old"] != 0 {
		t.Fatalf("old event should not contribute, got %d", counts["old"])
	}
	if counts["fresh"] != 1 {
		t.Fatalf("fresh event should contribute, got %d", counts["fresh"])
	}
}

func TestTopNHeap(t *testing.T) {
	items := topN(map[string]int64{
		"a": 1, "b": 5, "c": 3, "d": 2,
	}, 2)
	if len(items) != 2 || items[0].Query != "b" || items[1].Query != "c" {
		t.Fatalf("unexpected topN: %+v", items)
	}
}
