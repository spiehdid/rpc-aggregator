package cache

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkCache_Get_Hit(b *testing.B) {
	c := New(10000)
	c.Set("key", []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`), 5*time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("key")
	}
}

func BenchmarkCache_Get_Miss(b *testing.B) {
	c := New(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("missing")
	}
}

func BenchmarkCache_Set(b *testing.B) {
	c := New(10000)
	val := []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(fmt.Sprintf("key-%d", i%10000), val, 5*time.Second)
	}
}

func BenchmarkCache_Set_Eviction(b *testing.B) {
	c := New(100) // small cache, forces eviction
	val := []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(fmt.Sprintf("key-%d", i), val, 5*time.Second)
	}
}
