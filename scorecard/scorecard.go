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

// Package scorecard allows to throttle concurrency for tagged requests.
package scorecard

import (
	"fmt"
)

// A Tag captures a single attribute of a request.
//
// By convention a tag is the string "<type>:<value>" where the ':' separates
// the type and value.  A conjunction can specified by joining the strings with
// semicolons.  For instance, to specify the request is an update to gid 42:
//      "op:update;gid:42"
type Tag string

// NoTags returns an empty list of tags.
func NoTags() []Tag {
	return []Tag{}
}

// A Rule specifies a pattern string and the number of requests matching the
// pattern that are permitted in the system before subsequent requests become
// isolated.
//
// The pattern specified by the rule set uses common shell syntax as supported
// by Go's path.Match function.  If the rule does not parse according to these
// conventions, it will be treated as an exact string match only.
type Rule struct {
	Pattern  string
	Capacity uint
}

func (r *Rule) String() string {
	return fmt.Sprintf("%s(%d)", r.Pattern, r.Capacity)
}

// NoRules generates an empty rule set
func NoRules() []Rule {
	return []Rule{}
}

// NewScorecard creates a new scorecard.
func NewScorecard(rules []Rule) Scorecard {
	return newScorecard(rules)
}

// NewDynamicScorecard generates a scorecard that track the scorecard configuration and transparently switch from one to
// the next.
func NewDynamicScorecard(rules []Rule) DynamicScorecard {
	return newScorecard(rules).(DynamicScorecard)
}

// Config describes the scorecard. For now, it includes only rules, but it can be expanded to
// support other attributes as needed.
type Config struct {
	Rules []Rule
}

// NoRulesConfig generates a config with no rules.
func NoRulesConfig() Config {
	return Config{
		Rules: NoRules(),
	}
}

// A Scorecard tracks whether requests should be isolated.
//
// The scorecard tracks, for each tag, the number of requests currently in
// process with that tag.  Rules specify the maximum value a tag may have in the
// scorecard before the request should be isolated.
//
// A Scorecard implementation must provide internal synchronization between all
// calls to preserve the semantics described below.
type Scorecard interface {
	// The rules currently used in the scorecard.
	//
	// The returned value should be a copy and modifying it cannot change
	// scorecard behavior.
	Rules() []Rule
	// Track a request that was tagged with the given list of tags.  For each
	// tag, the value associated with the tag will be incremented by one.  This
	// value is then compared against each rule in Rules().  The first rule
	// whose pattern matches the tag will be used to determine the maximum
	// permissible value for the tag in the scorecard.  If the newly incremented
	// value exceeds the capacity specified by the rule, modifications to the
	// scorecard are reverted and the TrackingInfo is false and specifies the
	// rule and value that were observed in the violation.
	//
	// If this function returns a true TrackingInfo, the request need not be
	// isolated according to the scorecard.  it is the caller's responsibility to
	// call Untrack on the TrackingInfo in the future.
	//
	// If this function returns a false TrackingInfo, the request is considered
	// suspect and should be isolated.  No further obligations to call into the
	// scorecard are imposed upon the caller.
	TrackRequest(tags []Tag) *TrackingInfo
	// Inspect the scorecard's current set of values.
	// The returned map is only consistent if external synchronization is used
	// to ensure that Inspect() does not run concurrently with TrackRequest()
	// and TrackingInfo.Untrack().
	//
	// The returned value should be a copy and modifying it cannot change
	// scorecard behavior.
	Inspect() map[Tag]uint
}

// A DynamicScorecard may change over time.
// Extend the scorecard interface to allow stopping the background go-routines
// that make this dynamic magic possible.
type DynamicScorecard interface {
	// Embed the full scorecard interface.
	Scorecard
	// Reconfigure a scorecard to use the new config.  This is a blocking call
	// and will only return once the new scorecard is in effect.  The old
	// scorecard can still be in effect as well.  See dynamic_scorecard.go to
	// see how two scorecards interact with each other.
	Reconfigure(rules []Rule)
}

// TrackingInfo is the result of a TrackRequest to the scorecard.  `Tracked` indicates whether
// the request should be allowed.  A value of true indicates.  Violated and
// Value members are default initialized.  If false, the rule that was violated
// and current scorecard value are returned.
type TrackingInfo struct {
	Tracked bool

	// When there's a violation, what rule was violated, its limit, and the tag that triggered the
	// violation.
	Violated Rule
	Value    uint
	Tag      Tag

	trackedTags []Tag
	scorecard   Scorecard
	callback    func(s Scorecard, tt []Tag)
}

// Untrack removes the request from scorecard.
//
// It is scorecard-implementation agnostic.
func (ti *TrackingInfo) Untrack() {
	if ti.callback != nil && ti.scorecard != nil && ti.trackedTags != nil {
		ti.callback(ti.scorecard, ti.trackedTags)
		ti.callback = nil
		ti.scorecard = nil
		ti.trackedTags = nil
	}
}
