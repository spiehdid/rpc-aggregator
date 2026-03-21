package ws

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
)

type consumerSub struct {
	ConsumerID  string
	ConsumerSub string
	ShareKey    string
}

// OrphanedSub describes an upstream subscription that has no remaining
// consumers and should be unsubscribed from the upstream node.
type OrphanedSub struct {
	UpstreamSubID     string
	NodeID            string
	UnsubscribeMethod string
}

// SubInfo carries chain and method labels for metrics tracking.
type SubInfo struct {
	Chain  string
	Method string
}

type sharedSub struct {
	ShareKey    string
	Chain       string
	Method      string
	Params      string
	UpstreamSub string
	NodeID      string            // which upstream node this sub is on
	Consumers   map[string]string // consumerID → consumerSubID
}

type SubscriptionManager struct {
	mu               sync.RWMutex
	maxSubsPerConn   int
	maxTotalSubs     int
	byConsumerSub    map[string]*sharedSub
	byUpstreamSub    map[string]*sharedSub
	byShareKey       map[string]*sharedSub
	consumerSubs     map[string][]string // consumerID → []consumerSubID
	consumerSubCount map[string]int
}

func NewSubscriptionManager(maxSubsPerConn, maxTotalSubs int) *SubscriptionManager {
	return &SubscriptionManager{
		maxSubsPerConn: maxSubsPerConn, maxTotalSubs: maxTotalSubs,
		byConsumerSub: make(map[string]*sharedSub), byUpstreamSub: make(map[string]*sharedSub),
		byShareKey: make(map[string]*sharedSub), consumerSubs: make(map[string][]string),
		consumerSubCount: make(map[string]int),
	}
}

func (sm *SubscriptionManager) Subscribe(consumerID, chain, method, params, upstreamSubID, nodeID string) (string, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.consumerSubCount[consumerID] >= sm.maxSubsPerConn {
		return "", fmt.Errorf("max subscriptions per connection exceeded (%d)", sm.maxSubsPerConn)
	}
	if len(sm.byConsumerSub) >= sm.maxTotalSubs {
		return "", fmt.Errorf("max total subscriptions exceeded (%d)", sm.maxTotalSubs)
	}
	shareKey := chain + ":" + method + ":" + params
	consumerSubID := generateSubID()
	ss, exists := sm.byShareKey[shareKey]
	if !exists {
		ss = &sharedSub{ShareKey: shareKey, Chain: chain, Method: method, Params: params, UpstreamSub: upstreamSubID, NodeID: nodeID, Consumers: make(map[string]string)}
		sm.byShareKey[shareKey] = ss
		sm.byUpstreamSub[upstreamSubID] = ss
	}
	ss.Consumers[consumerID] = consumerSubID
	sm.byConsumerSub[consumerSubID] = ss
	sm.consumerSubs[consumerID] = append(sm.consumerSubs[consumerID], consumerSubID)
	sm.consumerSubCount[consumerID]++
	return consumerSubID, nil
}

func (sm *SubscriptionManager) SubscribeShared(consumerID, chain, method, params string) (string, bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.consumerSubCount[consumerID] >= sm.maxSubsPerConn {
		return "", false, fmt.Errorf("max subscriptions per connection exceeded (%d)", sm.maxSubsPerConn)
	}
	if len(sm.byConsumerSub) >= sm.maxTotalSubs {
		return "", false, fmt.Errorf("max total subscriptions exceeded (%d)", sm.maxTotalSubs)
	}
	shareKey := chain + ":" + method + ":" + params
	ss, exists := sm.byShareKey[shareKey]
	if !exists {
		return "", false, nil
	}
	consumerSubID := generateSubID()
	ss.Consumers[consumerID] = consumerSubID
	sm.byConsumerSub[consumerSubID] = ss
	sm.consumerSubs[consumerID] = append(sm.consumerSubs[consumerID], consumerSubID)
	sm.consumerSubCount[consumerID]++
	return consumerSubID, true, nil
}

func (sm *SubscriptionManager) Unsubscribe(consumerID, consumerSubID string) (info *SubInfo, orphaned *OrphanedSub) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ss, ok := sm.byConsumerSub[consumerSubID]
	if !ok {
		return nil, nil
	}
	info = &SubInfo{Chain: ss.Chain, Method: ss.Method}
	delete(ss.Consumers, consumerID)
	delete(sm.byConsumerSub, consumerSubID)
	subs := sm.consumerSubs[consumerID]
	for i, s := range subs {
		if s == consumerSubID {
			sm.consumerSubs[consumerID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	sm.consumerSubCount[consumerID] = len(sm.consumerSubs[consumerID])
	if len(ss.Consumers) == 0 {
		delete(sm.byShareKey, ss.ShareKey)
		delete(sm.byUpstreamSub, ss.UpstreamSub)
		return info, &OrphanedSub{
			UpstreamSubID:     ss.UpstreamSub,
			NodeID:            ss.NodeID,
			UnsubscribeMethod: strings.Replace(ss.Method, "Subscribe", "Unsubscribe", 1),
		}
	}
	return info, nil
}

func (sm *SubscriptionManager) RemoveConsumer(consumerID string) []OrphanedSub {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var orphaned []OrphanedSub
	for _, subID := range sm.consumerSubs[consumerID] {
		ss, ok := sm.byConsumerSub[subID]
		if !ok {
			continue
		}
		delete(ss.Consumers, consumerID)
		delete(sm.byConsumerSub, subID)
		if len(ss.Consumers) == 0 {
			orphaned = append(orphaned, OrphanedSub{
				UpstreamSubID:     ss.UpstreamSub,
				NodeID:            ss.NodeID,
				UnsubscribeMethod: strings.Replace(ss.Method, "Subscribe", "Unsubscribe", 1),
			})
			delete(sm.byShareKey, ss.ShareKey)
			delete(sm.byUpstreamSub, ss.UpstreamSub)
		}
	}
	delete(sm.consumerSubs, consumerID)
	delete(sm.consumerSubCount, consumerID)
	return orphaned
}

func (sm *SubscriptionManager) GetUpstreamSubInfo(upstreamSubID string) *SubInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ss, ok := sm.byUpstreamSub[upstreamSubID]
	if !ok {
		return nil
	}
	return &SubInfo{Chain: ss.Chain, Method: ss.Method}
}

func (sm *SubscriptionManager) GetConsumersForUpstream(upstreamSubID string) map[string]string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ss, ok := sm.byUpstreamSub[upstreamSubID]
	if !ok {
		return nil
	}
	result := make(map[string]string, len(ss.Consumers))
	for k, v := range ss.Consumers {
		result[k] = v
	}
	return result
}

func (sm *SubscriptionManager) GetConsumerSubs(consumerID string) []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	result := make([]string, len(sm.consumerSubs[consumerID]))
	copy(result, sm.consumerSubs[consumerID])
	return result
}

func (sm *SubscriptionManager) InvalidateUpstream(upstreamSubID string) []string {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ss, ok := sm.byUpstreamSub[upstreamSubID]
	if !ok {
		return nil
	}
	shareKey := ss.ShareKey
	delete(sm.byUpstreamSub, upstreamSubID)
	ss.UpstreamSub = ""
	return []string{shareKey}
}

func (sm *SubscriptionManager) ReattachUpstream(shareKey, newUpstreamSubID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ss, ok := sm.byShareKey[shareKey]
	if !ok {
		return
	}
	ss.UpstreamSub = newUpstreamSubID
	sm.byUpstreamSub[newUpstreamSubID] = ss
}

// SubResubInfo carries the details needed to re-subscribe after a reconnect.
type SubResubInfo struct {
	ShareKey       string
	OldUpstreamSub string
	Method         string
	Params         string
	Chain          string
}

func (sm *SubscriptionManager) GetSubsByNodeID(nodeID string) []SubResubInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	var subs []SubResubInfo
	for _, ss := range sm.byShareKey {
		if ss.NodeID == nodeID && ss.UpstreamSub != "" {
			subs = append(subs, SubResubInfo{
				ShareKey:       ss.ShareKey,
				OldUpstreamSub: ss.UpstreamSub,
				Method:         ss.Method,
				Params:         ss.Params,
				Chain:          ss.Chain,
			})
		}
	}
	return subs
}

func generateSubID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return "0x" + hex.EncodeToString(b)
}
