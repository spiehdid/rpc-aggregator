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
	Hits    uint64
	Misses  uint64
}

type Cache struct {
	mu         sync.Mutex
	maxEntries int
	items      map[string]*list.Element
	evictList  *list.List
	hits       uint64
	misses     uint64
}

func New(maxEntries int) *Cache {
	return &Cache{
		maxEntries: maxEntries,
		items:      make(map[string]*list.Element),
		evictList:  list.New(),
	}
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
	if el, ok := c.items[key]; ok {
		c.evictList.MoveToFront(el)
		e := el.Value.(*entry)
		e.value = value
		e.ttl = ttl
		if ttl > 0 {
			e.expiresAt = time.Now().Add(ttl)
		}
		return
	}
	e := &entry{key: key, value: value, ttl: ttl}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	el := c.evictList.PushFront(e)
	c.items[key] = el
	if c.maxEntries > 0 && c.evictList.Len() > c.maxEntries {
		c.removeOldest()
	}
}

func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{Entries: len(c.items), Hits: c.hits, Misses: c.misses}
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
}
