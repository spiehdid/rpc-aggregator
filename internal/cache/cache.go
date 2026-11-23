package cache

import (
	"container/list"
	"sync"
	"time"
)

type entry struct {
	key       string
	value     []byte
	expiresAt time.Time
	ttl       time.Duration
}

type Stats struct {
	Entries int
	Bytes   int64
	Hits    uint64
	Misses  uint64
}

type Cache struct {
	mu         sync.Mutex
	maxEntries int
	// maxBytes bounds the summed size of cached values (0 = no byte limit). Entries alone are not
	// enough: historical eth_getLogs responses can be megabytes each.
	maxBytes  int64
	curBytes  int64
	items     map[string]*list.Element
	evictList *list.List
	hits      uint64
	misses    uint64
}

func New(maxEntries int) *Cache {
	return &Cache{
		maxEntries: maxEntries,
		items:      make(map[string]*list.Element),
		evictList:  list.New(),
	}
}

// SetMaxBytes bounds the summed size of cached values; values larger than the bound are not cached.
func (c *Cache) SetMaxBytes(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxBytes = n
	c.evictOverBytes()
}

func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, false
	}
	e := el.Value.(*entry)
	if e.ttl > 0 && time.Now().After(e.expiresAt) {
		c.removeElement(el)
		c.misses++
		return nil, false
	}
	c.evictList.MoveToFront(el)
	c.hits++
	return e.value, true
}

func (c *Cache) Set(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.maxBytes > 0 && int64(len(value)) > c.maxBytes {
		if el, ok := c.items[key]; ok {
			c.removeElement(el)
		}
		return
	}
	if el, ok := c.items[key]; ok {
		c.evictList.MoveToFront(el)
		e := el.Value.(*entry)
		c.curBytes += int64(len(value)) - int64(len(e.value))
		e.value = value
		e.ttl = ttl
		if ttl > 0 {
			e.expiresAt = time.Now().Add(ttl)
		}
		c.evictOverBytes()
		return
	}
	e := &entry{key: key, value: value, ttl: ttl}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	el := c.evictList.PushFront(e)
	c.items[key] = el
	c.curBytes += int64(len(value))
	if c.maxEntries > 0 && c.evictList.Len() > c.maxEntries {
		c.removeOldest()
	}
	c.evictOverBytes()
}

// evictOverBytes drops least-recently-used entries until the byte bound holds. Caller holds mu.
func (c *Cache) evictOverBytes() {
	for c.maxBytes > 0 && c.curBytes > c.maxBytes && c.evictList.Len() > 0 {
		c.removeOldest()
	}
}

func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{Entries: len(c.items), Bytes: c.curBytes, Hits: c.hits, Misses: c.misses}
}

func (c *Cache) removeOldest() {
	el := c.evictList.Back()
	if el != nil {
		c.removeElement(el)
	}
}

func (c *Cache) removeElement(el *list.Element) {
	c.evictList.Remove(el)
	e := el.Value.(*entry)
	delete(c.items, e.key)
	c.curBytes -= int64(len(e.value))
}
