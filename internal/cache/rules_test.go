package cache

import (
	"testing"
	"time"
)

func TestRuleMatcher_FindsMatchingRule(t *testing.T) {
	rules := []Rule{
		{Method: "eth_chainId", TTL: 0},
		{Method: "eth_blockNumber", TTL: 1 * time.Second},
		{Method: "eth_gasPrice", TTL: 3 * time.Second},
	}
	rm := NewRuleMatcher(rules, 5*time.Second)
	ttl, ok := rm.Match("eth_blockNumber", "")
	if !ok {
		t.Fatal("expected match for eth_blockNumber")
	}
	if ttl != 1*time.Second {
		t.Errorf("TTL = %v, want 1s", ttl)
	}
}

func TestRuleMatcher_ZeroTTLMeansForever(t *testing.T) {
	rules := []Rule{{Method: "eth_chainId", TTL: 0}}
	rm := NewRuleMatcher(rules, 5*time.Second)
	ttl, ok := rm.Match("eth_chainId", "")
	if !ok {
		t.Fatal("expected match")
	}
	if ttl != 0 {
		t.Errorf("TTL = %v, want 0 (forever)", ttl)
	}
}

func TestRuleMatcher_ConditionMatches(t *testing.T) {
	rules := []Rule{{Method: "eth_getBlockByNumber", TTL: 0, Condition: "finalized"}}
	rm := NewRuleMatcher(rules, 5*time.Second)
	ttl, ok := rm.Match("eth_getBlockByNumber", "finalized")
	if !ok {
		t.Fatal("expected match with condition")
	}
	if ttl != 0 {
		t.Errorf("TTL = %v, want 0", ttl)
	}
}

func TestRuleMatcher_ConditionMismatch(t *testing.T) {
	rules := []Rule{{Method: "eth_getBlockByNumber", TTL: 0, Condition: "finalized"}}
	rm := NewRuleMatcher(rules, 5*time.Second)
	_, ok := rm.Match("eth_getBlockByNumber", "latest")
	if ok {
		t.Error("expected no match when condition doesn't match")
	}
}

func TestRuleMatcher_NoMatchReturnsDefault(t *testing.T) {
	rules := []Rule{{Method: "eth_chainId", TTL: 0}}
	rm := NewRuleMatcher(rules, 5*time.Second)
	_, ok := rm.Match("eth_unknownMethod", "")
	if ok {
		t.Error("expected no match for unknown method")
	}
}
