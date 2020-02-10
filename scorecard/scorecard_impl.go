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
	"sync"
)

// Implementation of the interfaces in scorecard.go.

const numBuckets = 16 // Picked arbitrarily. Ideally should be tuned.

type scorecardImpl struct {
	// rulesMu protects rules and ctg.
	// Those are pointers and fast to read and write, but under heavy concurrency
	// contention becomes a problem, therefore RWMutex.
	rulesMu sync.RWMutex
	rules   []Rule
	ctg     *compoundTagGenerator

	// To reduce lock contention, every `Tag` is mapped to one of `numBuckets` buckets which has
	// its own lock.
	tagScoresBuckets [numBuckets]*tagScores
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
	var tagScoresBuckets [numBuckets]*tagScores
	for i := range tagScoresBuckets {
		tagScoresBuckets[i] = &tagScores{tagToScore: make(map[Tag]uint)}
	}
	return &scorecardImpl{
		rules:   rules,
		ctg:     ctg,
		tagScoresBuckets: tagScoresBuckets,
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

func (s *scorecardImpl) shouldIsolateTag(tag Tag, rule Rule) bool {
	return s.bucket(tag).shouldIsolateTag(tag, rule)
}

func (s *scorecardImpl) removeReference(tag Tag) {
	s.bucket(tag).removeReference(tag)
}

func (s *scorecardImpl) bucket(tag Tag) *tagScores {
	// This hash was picked arbitrarily. The fastest, crudest, hash is probably ideal.
	h := fnv.New32a()
	h.Write([]byte(tag))
	return s.tagScoresBuckets[h.Sum32() % numBuckets]
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
	lock sync.Mutex
	tagToScore   map[Tag]uint
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