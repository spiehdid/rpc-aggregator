package cache

import "time"

type Rule struct {
	Method    string
	TTL       time.Duration
	Condition string
}

type RuleMatcher struct {
	rules      []Rule
	defaultTTL time.Duration
}

func NewRuleMatcher(rules []Rule, defaultTTL time.Duration) *RuleMatcher {
	return &RuleMatcher{rules: rules, defaultTTL: defaultTTL}
}

func (rm *RuleMatcher) Match(method, condition string) (ttl time.Duration, matched bool) {
	for _, r := range rm.rules {
		if r.Method != method {
			continue
		}
		if r.Condition != "" && r.Condition != condition {
			continue
		}
		return r.TTL, true
	}
	return 0, false
}

func (rm *RuleMatcher) DefaultTTL() time.Duration { return rm.defaultTTL }
