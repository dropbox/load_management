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
	"sync"
)

// Implementation of the interfaces in scorecard.go.

type scorecardImpl struct {
	// rulesMu protects rules and ctg.
	// Those are pointers and fast to read and write, but under heavy concurrency
	// contention becomes a problem, therefore RWMutex.
	rulesMu sync.RWMutex
	rules   []Rule
	ctg     *CompoundTagGenerator

	// separate mutex for current to avoid contention with reading pointers above
	// for each rule.
	currentMu sync.Mutex
	current   map[Tag]uint
}

func getRulesAndTagGenerator(rules []Rule) ([]Rule, *CompoundTagGenerator) {
	// Dedup the rules
	dedupedRules := make([]Rule, 0, len(rules))
	ruleMap := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		// Add the rule if we have not seen it before
		if _, ok := ruleMap[rule.Pattern]; !ok {
			dedupedRules = append(dedupedRules, rule)
			ruleMap[rule.Pattern] = struct{}{}
		}
	}

	return dedupedRules, NewCompoundTagGenerator(dedupedRules)
}

func newScorecard(rules []Rule) Scorecard {
	rules, ctg := getRulesAndTagGenerator(rules)
	return &scorecardImpl{
		rules:   rules,
		ctg:     ctg,
		current: make(map[Tag]uint),
	}
}

func (s *scorecardImpl) Rules() []Rule {
	s.rulesMu.RLock()
	defer s.rulesMu.RUnlock()
	rules := make([]Rule, len(s.rules))
	copy(rules, s.rules)
	return rules
}

func (r *Rule) isDefaultValue() bool {
	return r.Pattern == "" && r.Capacity == 0
}

func ruleFor(rules []Rule, tag Tag) Rule {
	for _, rule := range rules {
		if TagMatchesRule(tag, rule) {
			return rule
		}
	}
	return Rule{}
}

func (s *scorecardImpl) TrackRequest(tags []Tag) *TrackingInfo {
	s.rulesMu.RLock()
	rules := s.rules
	ctg := s.ctg
	s.rulesMu.RUnlock()
	allTags := ctg.Generate(tags)
	allTags = append(allTags, tags...)
	for idx, tag := range allTags {
		rule := ruleFor(rules, tag)
		if s.shouldIsolateTag(tag, rule) {
			// shouldIsolate tracks rules in the scorecard. For now, it only tracks when the rule
			// isn't violated (i.e. doesn't hit this branch). That means we will want to untrack the
			// previous idx rules, so we take allTags[:idx] (end range is EXCLUSIVE). If
			// shouldIsolate changes to add in either case, then we should make sure to unset
			// allTags[idx] as well.
			s.rawUntrackRequest(allTags[:idx])
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
		trackedTags: allTags,
		scorecard:   s,
		callback:    rawUntrackCallback,
	}
}

func (s *scorecardImpl) Reconfigure(rules []Rule) {
	rules, ctg := getRulesAndTagGenerator(rules)
	s.rulesMu.Lock()
	s.rules = rules
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

// Perform locking in these isolated functions.  Touching the map elsewhere is
// discouraged in order to isolate the concurrency.
//
// For this lock/map design, it will have lower throughput, but also lower
// latency variance assuming the mutex can be fair (latency will grow uniformly
// across the requests).
//
// For a lockfree map design, or an otherwise scalable map, this should have
// both higher throughput and lower latency.
func (s *scorecardImpl) shouldIsolateTag(tag Tag, rule Rule) bool {
	s.currentMu.Lock()
	defer s.currentMu.Unlock()
	current := s.currentScore(tag)
	isolate := !rule.isDefaultValue() && current >= rule.Capacity
	// overflow check
	if current+1 < current {
		isolate = true
	}
	if !isolate {
		s.current[tag] = current + 1
	}
	return isolate
}

func (s *scorecardImpl) removeReference(tag Tag) {
	s.currentMu.Lock()
	defer s.currentMu.Unlock()
	current := s.currentScore(tag)
	if current > 1 {
		s.current[tag] = current - 1
	} else if current > 0 {
		delete(s.current, tag)
	}
}

func (s *scorecardImpl) currentScore(tag Tag) uint {
	if x, ok := s.current[tag]; ok {
		return x
	}
	return 0
}

func (s *scorecardImpl) Inspect() map[Tag]uint {
	ret := make(map[Tag]uint)
	s.currentMu.Lock()
	defer s.currentMu.Unlock()
	for t, v := range s.current {
		ret[t] = v
	}
	return ret
}
