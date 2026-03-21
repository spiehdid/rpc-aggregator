package cache

import (
	"testing"
	"time"
)

func TestCache_SetAndGet(t *testing.T) {
	c := New(100)
	c.Set("key1", []byte(`{"result":"ok"}`), 5*time.Second)
	val, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(val) != `{"result":"ok"}` {
		t.Errorf("got %q, want %q", string(val), `{"result":"ok"}`)
	}
}

func TestCache_Miss(t *testing.T) {
	c := New(100)
	_, ok := c.Get("nonexistent")
	if ok {
		t.Error("expected cache miss")
	}
}

func TestCache_TTLExpiry(t *testing.T) {
	c := New(100)
	c.Set("key1", []byte("data"), 50*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	_, ok := c.Get("key1")
	if ok {
		t.Error("expected cache miss after TTL expiry")
	}
}

func TestCache_ZeroTTL_NeverExpires(t *testing.T) {
	c := New(100)
	c.Set("key1", []byte("eternal"), 0)
	time.Sleep(10 * time.Millisecond)
	val, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit for zero TTL (eternal)")
	}
	if string(val) != "eternal" {
		t.Errorf("got %q, want %q", string(val), "eternal")
	}
}

func TestCache_LRUEviction(t *testing.T) {
	c := New(2)
	c.Set("a", []byte("1"), 5*time.Second)
	c.Set("b", []byte("2"), 5*time.Second)
	c.Set("c", []byte("3"), 5*time.Second)
	_, ok := c.Get("a")
	if ok {
		t.Error("expected 'a' to be evicted")
	}
	_, ok = c.Get("b")
	if !ok {
		t.Error("expected 'b' to still exist")
	}
	_, ok = c.Get("c")
	if !ok {
		t.Error("expected 'c' to still exist")
	}
}

func TestCache_LRUAccessRefreshes(t *testing.T) {
	c := New(2)
	c.Set("a", []byte("1"), 5*time.Second)
	c.Set("b", []byte("2"), 5*time.Second)
	c.Get("a")
	c.Set("c", []byte("3"), 5*time.Second)
	_, ok := c.Get("a")
	if !ok {
		t.Error("expected 'a' to still exist after access refresh")
	}
	_, ok = c.Get("b")
	if ok {
		t.Error("expected 'b' to be evicted")
	}
}

func TestCache_Overwrite(t *testing.T) {
	c := New(100)
	c.Set("key", []byte("old"), 5*time.Second)
	c.Set("key", []byte("new"), 5*time.Second)
	val, ok := c.Get("key")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(val) != "new" {
		t.Errorf("got %q, want %q", string(val), "new")
	}
}

func TestCache_Stats(t *testing.T) {
	c := New(100)
	c.Set("key", []byte("val"), 5*time.Second)
	c.Get("key")
	c.Get("missing")
	stats := c.Stats()
	if stats.Entries != 1 {
		t.Errorf("Entries = %d, want 1", stats.Entries)
	}
	if stats.Hits != 1 {
		t.Errorf("Hits = %d, want 1", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Misses = %d, want 1", stats.Misses)
	}
}
