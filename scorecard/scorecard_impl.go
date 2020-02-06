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
	ctg     *compoundTagGenerator

	// separate mutex for current to avoid contention with reading pointers above
	// for each rule.
	currentMu sync.Mutex
	current   map[Tag]uint
}

func getRulesAndTagGenerator(rules []Rule) ([]Rule, *compoundTagGenerator) {
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

	return dedupedRules, newCompoundTagGenerator(dedupedRules)
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
	ctg := s.ctg
	s.rulesMu.RUnlock()

	// NOTE(opaugam) - perform the cartesian product of tag X non-atomic rules. The
	// shortcuts array holds a back pointer to the corresponding rule for each output tag.
	var rule Rule
	shortcuts, expanded := ctg.combine(tags)
	expanded = append(expanded, tags...)
	for idx, tag := range expanded {
		// NOTE(opaugam) - the first n tags can by definition be mapped to their rule
		// without globbing. We still need to enforce a linear scan+glob for the rest
		// (e.g the input tags) but do it only on whatever atomic rules we have. If we
		// can't match we'll default to a Rule{} placeholder.
		if idx < len(shortcuts) {
			rule = shortcuts[idx].rule
		} else {
			rule = ruleFor(ctg.singleFragmentRules, tag)
		}

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
