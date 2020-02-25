/*
Copyright (c) 2018 Dropbox, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scorecard

import (
	"hash/fnv"
	"regexp"
	"strings"
	"sync"
)

// Implementation of the interfaces in scorecard.go.

const (
	numBuckets = 16 // Picked arbitrarily. Ideally should be tuned.

	// Rule patterns have wild cards (*). We use regex matching to have better
	// performance and the below string is used to replace the wild card pattern in
	// the rule while performing regex compilation.
	//
	// Note: We use * at the end instead of + because in certain cases it's possible due to bugs
	// that we don't emit a value for a tag but we'd still like it to match with the existing rules.
	// Eg: Rule: op:*;source:foo will be compiled using ->  regexp.Compile("^op:[a-z|A-Z|0-9|_|-]*;source:foo$")
	// This will match the tag: op:;source:foo" whereas if we had used a + it wouldn't match op:;source:foo.
	//
	// Note: We can't blindly replace * in the rule with .* because it'll end up matching more than we desire.
	// For eg if we replace * with .* in the regex then the rule: op:*;source:* will have the regex: op:.*;source:.*
	// This will end up matching tags that look like: op:read_gid2;rpc:read;source:file_system. We don't want
	// the regex to match this because it has an additional tag, value pair namely rpc:read in the middle.
	// Instead we simply match everything that's not a rule delimeter a.k.a ";"
	starExpand = "[^;]*"
)

type scorecardImpl struct {
	// rulesMu protects rules and ctg.
	// Those are pointers and fast to read and write, but under heavy concurrency
	// contention becomes a problem, therefore RWMutex.
	rulesMu sync.RWMutex
	rules   []*fastMatchRule
	ctg     *compoundTagGenerator

	// To reduce lock contention, every `Tag` is mapped to one of `numBuckets` buckets which has
	// its own lock.
	tagScoresBuckets [numBuckets]*tagScores
}

func getFastMatchRuleFromRule(rule Rule) *fastMatchRule {
	ruleRegex, err := regexp.Compile("^" + strings.Replace(rule.Pattern, "*", starExpand, -1) + "$")
	if err != nil {
		panic(err)
	}
	return &fastMatchRule{Rule: rule, regex: ruleRegex}
}

func getRulesAndTagGenerator(rules []Rule) ([]*fastMatchRule, *compoundTagGenerator) {
	// Dedup the rules
	dedupedRules := make([]*fastMatchRule, 0, len(rules))
	ruleMap := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		// Add the rule if we have not seen it before
		if _, ok := ruleMap[rule.Pattern]; !ok {
			dedupedRules = append(dedupedRules, getFastMatchRuleFromRule(rule))
			ruleMap[rule.Pattern] = struct{}{}
		}
	}

	return dedupedRules, newCompoundTagGenerator(dedupedRules)
}

func newScorecard(rules []Rule) Scorecard {
	rule, ctg := getRulesAndTagGenerator(rules)
	var tagScoresBuckets [numBuckets]*tagScores
	for i := range tagScoresBuckets {
		tagScoresBuckets[i] = &tagScores{tagToScore: make(map[Tag]uint)}
	}
	return &scorecardImpl{
		rules:            rule,
		ctg:              ctg,
		tagScoresBuckets: tagScoresBuckets,
	}
}

func (s *scorecardImpl) Rules() []Rule {
	s.rulesMu.RLock()
	defer s.rulesMu.RUnlock()
	rules := make([]Rule, len(s.rules))
	for idx, rule := range s.rules {
		rules[idx] = rule.Rule
	}
	return rules
}

func (r *Rule) isDefaultValue() bool {
	return r.Pattern == "" && r.Capacity == 0
}

func ruleFor(rules []*fastMatchRule, tag Tag) Rule {
	for _, rule := range rules {
		if FastMatchCompoundRule(tag, rule) {
			return rule.Rule
		}
	}
	return Rule{}
}

func (s *scorecardImpl) TrackRequest(tags []Tag) *TrackingInfo {
	s.rulesMu.RLock()
	ctg := s.ctg
	rules := s.rules
	s.rulesMu.RUnlock()

	var rule Rule
	expanded := ctg.combine(tags)
	expanded = append(expanded, tags...)
	for idx, tag := range expanded {
		rule = ruleFor(rules, tag)
		if s.shouldIsolateTag(tag, rule) {
			// shouldIsolate tracks rules in the scorecard. For now, it only tracks when the rule
			// isn't violated (i.e. doesn't hit this branch). That means we will want to untrack the
			// previous idx rules, so we take expanded[:idx] (end range is EXCLUSIVE). If
			// shouldIsolate changes to add in either case, then we should make sure to unset
			// expanded[idx] as well.
			s.rawUntrackRequest(expanded[:idx])
			return &TrackingInfo{
				Tracked:  false,
				Violated: rule,
				Value:    rule.Capacity,
				Tag:      tag,
			}
		}
	}
	return &TrackingInfo{
		Tracked:     true,
		trackedTags: expanded,
		scorecard:   s,
		callback:    rawUntrackCallback,
	}
}

func (s *scorecardImpl) Reconfigure(rules []Rule) {
	rulesForFastMatching, ctg := getRulesAndTagGenerator(rules)
	s.rulesMu.Lock()
	s.rules = rulesForFastMatching
	s.ctg = ctg
	s.rulesMu.Unlock()
}

func rawUntrackCallback(s Scorecard, t []Tag) {
	s.(*scorecardImpl).rawUntrackRequest(t)
}

func (s *scorecardImpl) rawUntrackRequest(tags []Tag) {
	for _, tag := range tags {
		s.removeReference(tag)
	}
}

func (s *scorecardImpl) shouldIsolateTag(tag Tag, rule Rule) bool {
	return s.bucket(tag).shouldIsolateTag(tag, rule)
}

func (s *scorecardImpl) removeReference(tag Tag) {
	s.bucket(tag).removeReference(tag)
}

func (s *scorecardImpl) bucket(tag Tag) *tagScores {
	// This hash was picked arbitrarily. The fastest, crudest, hash is probably ideal.
	h := fnv.New32a()
	_, _ = h.Write([]byte(tag))
	return s.tagScoresBuckets[h.Sum32()%numBuckets]
}

func (s *scorecardImpl) Inspect() map[Tag]uint {
	ret := make(map[Tag]uint)
	for _, bucket := range s.tagScoresBuckets {
		bucket.lock.Lock()
		for t, v := range bucket.tagToScore {
			ret[t] = v
		}
		bucket.lock.Unlock()
	}
	return ret
}

type tagScores struct {
	lock       sync.Mutex
	tagToScore map[Tag]uint
}

func (t *tagScores) shouldIsolateTag(tag Tag, rule Rule) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	score := t.score(tag)
	isolate := !rule.isDefaultValue() && score >= rule.Capacity
	// overflow check
	if score+1 < score {
		isolate = true
	}
	if !isolate {
		t.tagToScore[tag] = score + 1
	}
	return isolate
}

func (t *tagScores) removeReference(tag Tag) {
	t.lock.Lock()
	defer t.lock.Unlock()
	score := t.score(tag)
	if score > 1 {
		t.tagToScore[tag] = score - 1
	} else if score > 0 {
		delete(t.tagToScore, tag)
	}
}

func (t *tagScores) score(tag Tag) uint {
	if x, ok := t.tagToScore[tag]; ok {
		return x
	}
	return 0
}
