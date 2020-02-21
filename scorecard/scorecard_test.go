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
	"fmt"
	insecure_random "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ScorecardSuite struct {
	suite.Suite
}

func TestScorecardSuite(t *testing.T) {
	suite.

		// Test the scorecard described in the isolation document
		Run(t, new(ScorecardSuite))
}

func (s *ScorecardSuite) TestBasics() {
	// Setup
	req1 := []Tag{"meta_www", "TeamUserAssoc", "GID_10", "Point_Read"}
	req2 := []Tag{"meta_api", "UserEntity", "GID_20", "Point_Read"}
	req3 := []Tag{"meta_www", "TeamUserAssoc", "GID_30", "List_Read"}
	sc := NewScorecard([]Rule{})
	// Request 1
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	// Request 2
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)
	// Request 3
	ret3 := sc.TrackRequest(req3)
	require.True(s.T(), ret3.Tracked)
	// Check the map state
	vals := sc.Inspect()
	require.Equal(s.T(), len(vals), 9)
	require.Contains(s.T(), vals, Tag("meta_www"))
	require.Equal(s.T(), vals[Tag("meta_www")], uint(2))
	require.Contains(s.T(), vals, Tag("TeamUserAssoc"))
	require.Equal(s.T(), vals[Tag("TeamUserAssoc")], uint(2))
	require.Contains(s.T(), vals, Tag("GID_10"))
	require.Equal(s.T(), vals[Tag("GID_10")], uint(1))
	require.Contains(s.T(), vals, Tag("Point_Read"))
	require.Equal(s.T(), vals[Tag("Point_Read")], uint(2))
	require.Contains(s.T(), vals, Tag("meta_api"))
	require.Equal(s.T(), vals[Tag("meta_api")], uint(1))
	require.Contains(s.T(), vals, Tag("UserEntity"))
	require.Equal(s.T(), vals[Tag("UserEntity")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_20"))
	require.Equal(s.T(), vals[Tag("GID_20")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_30"))
	require.Equal(s.T(), vals[Tag("GID_30")], uint(1))
	require.Contains(s.T(), vals, Tag("List_Read"))
	require.Equal(s.T(), vals[Tag("List_Read")], uint(1))
	// Undo Request 3
	ret3.Untrack()
	// Check the map state
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 7)
	require.Contains(s.T(), vals, Tag("meta_www"))
	require.Equal(s.T(), vals[Tag("meta_www")], uint(1))
	require.Contains(s.T(), vals, Tag("TeamUserAssoc"))
	require.Equal(s.T(), vals[Tag("TeamUserAssoc")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_10"))
	require.Equal(s.T(), vals[Tag("GID_10")], uint(1))
	require.Contains(s.T(), vals, Tag("Point_Read"))
	require.Equal(s.T(), vals[Tag("Point_Read")], uint(2))
	require.Contains(s.T(), vals, Tag("meta_api"))
	require.Equal(s.T(), vals[Tag("meta_api")], uint(1))
	require.Contains(s.T(), vals, Tag("UserEntity"))
	require.Equal(s.T(), vals[Tag("UserEntity")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_20"))
	require.Equal(s.T(), vals[Tag("GID_20")], uint(1))
	// Undo Request 2
	ret2.Untrack()
	// Check the map state
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 4)
	require.Contains(s.T(), vals, Tag("meta_www"))
	require.Equal(s.T(), vals[Tag("meta_www")], uint(1))
	require.Contains(s.T(), vals, Tag("TeamUserAssoc"))
	require.Equal(s.T(), vals[Tag("TeamUserAssoc")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_10"))
	require.Equal(s.T(), vals[Tag("GID_10")], uint(1))
	require.Contains(s.T(), vals, Tag("Point_Read"))
	require.Equal(s.T(), vals[Tag("Point_Read")], uint(1))
	// Undo Request 1
	ret1.Untrack()
	// Check the map state
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 0)
}

// Test repeated untrack (which is against API, but shouldn't be undefined)
func (s *ScorecardSuite) TestRepeatedUntrack() {
	// Setup
	req1 := []Tag{"meta_www", "TeamUserAssoc", "GID_10", "Point_Read"}
	req2 := []Tag{"meta_api", "UserEntity", "GID_20", "Point_Read"}
	req3 := []Tag{"meta_www", "TeamUserAssoc", "GID_30", "List_Read"}
	sc := NewScorecard([]Rule{})
	// Request 1
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	// Request 2
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)
	// Request 3
	ret3 := sc.TrackRequest(req3)
	require.True(s.T(), ret3.Tracked)
	// Check the map state
	vals := sc.Inspect()
	require.Equal(s.T(), len(vals), 9)
	require.Contains(s.T(), vals, Tag("meta_www"))
	require.Equal(s.T(), vals[Tag("meta_www")], uint(2))
	require.Contains(s.T(), vals, Tag("TeamUserAssoc"))
	require.Equal(s.T(), vals[Tag("TeamUserAssoc")], uint(2))
	require.Contains(s.T(), vals, Tag("GID_10"))
	require.Equal(s.T(), vals[Tag("GID_10")], uint(1))
	require.Contains(s.T(), vals, Tag("Point_Read"))
	require.Equal(s.T(), vals[Tag("Point_Read")], uint(2))
	require.Contains(s.T(), vals, Tag("meta_api"))
	require.Equal(s.T(), vals[Tag("meta_api")], uint(1))
	require.Contains(s.T(), vals, Tag("UserEntity"))
	require.Equal(s.T(), vals[Tag("UserEntity")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_20"))
	require.Equal(s.T(), vals[Tag("GID_20")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_30"))
	require.Equal(s.T(), vals[Tag("GID_30")], uint(1))
	require.Contains(s.T(), vals, Tag("List_Read"))
	require.Equal(s.T(), vals[Tag("List_Read")], uint(1))
	// Undo Request 3
	ret3.Untrack()
	// Check the map state
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 7)
	require.Contains(s.T(), vals, Tag("meta_www"))
	require.Equal(s.T(), vals[Tag("meta_www")], uint(1))
	require.Contains(s.T(), vals, Tag("TeamUserAssoc"))
	require.Equal(s.T(), vals[Tag("TeamUserAssoc")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_10"))
	require.Equal(s.T(), vals[Tag("GID_10")], uint(1))
	require.Contains(s.T(), vals, Tag("Point_Read"))
	require.Equal(s.T(), vals[Tag("Point_Read")], uint(2))
	require.Contains(s.T(), vals, Tag("meta_api"))
	require.Equal(s.T(), vals[Tag("meta_api")], uint(1))
	require.Contains(s.T(), vals, Tag("UserEntity"))
	require.Equal(s.T(), vals[Tag("UserEntity")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_20"))
	require.Equal(s.T(), vals[Tag("GID_20")], uint(1))
	// Undo Request 3 again
	ret3.Untrack()
	// Check the map state
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 7)
	require.Contains(s.T(), vals, Tag("meta_www"))
	require.Equal(s.T(), vals[Tag("meta_www")], uint(1))
	require.Contains(s.T(), vals, Tag("TeamUserAssoc"))
	require.Equal(s.T(), vals[Tag("TeamUserAssoc")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_10"))
	require.Equal(s.T(), vals[Tag("GID_10")], uint(1))
	require.Contains(s.T(), vals, Tag("Point_Read"))
	require.Equal(s.T(), vals[Tag("Point_Read")], uint(2))
	require.Contains(s.T(), vals, Tag("meta_api"))
	require.Equal(s.T(), vals[Tag("meta_api")], uint(1))
	require.Contains(s.T(), vals, Tag("UserEntity"))
	require.Equal(s.T(), vals[Tag("UserEntity")], uint(1))
	require.Contains(s.T(), vals, Tag("GID_20"))
	require.Equal(s.T(), vals[Tag("GID_20")], uint(1))
}

func (s *ScorecardSuite) TestRegex() {
	var benchmarkRules = []Rule{
		{"traffic:live_traffic;source:metaserver*", 400},
		{"traffic:live_traffic;source:*", 50},
	}
	bc := NewScorecard(benchmarkRules)
	request := []Tag{
		"client_id:meta",
		"source:metaserver_courier_live_site-main.py",
		"traffic:live_traffic",
		"rpc_op:ep_add_or_get_for_ke",
		"datatype:PublicDomainSuffixEntity",
		"op:read_xtxn_conflict",
	}
	for i := 0; i < 51; i++ {
		info := bc.TrackRequest(request)
		require.Truef(s.T(), info.Tracked, "Violated: %s", info.Violated)
	}
}

// Test that an isolated request does not alter the scorecard state
func (s *ScorecardSuite) TestIsolatedRequest() {
	// Setup
	rules := []Rule{{"op:read", 2}}
	req1 := []Tag{"op:read", "gid:13"}
	req2 := []Tag{"op:read", "gid:42"}
	req3 := []Tag{"op:read", "gid:1337"}
	sc := NewScorecard(rules)
	// Request 1
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	vals := sc.Inspect()
	require.Equal(s.T(), len(vals), 2)
	require.Contains(s.T(), vals, Tag("op:read"))
	require.Equal(s.T(), vals[Tag("op:read")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:13"))
	require.Equal(s.T(), vals[Tag("gid:13")], uint(1))
	// Request 2
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 3)
	require.Contains(s.T(), vals, Tag("op:read"))
	require.Equal(s.T(), vals[Tag("op:read")], uint(2))
	require.Contains(s.T(), vals, Tag("gid:13"))
	require.Equal(s.T(), vals[Tag("gid:13")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(1))
	// Request 3
	ret3 := sc.TrackRequest(req3)
	require.False(s.T(), ret3.Tracked)
	require.Equal(s.T(), ret3.Violated, Rule{"op:read", 2})
	require.Equal(s.T(), ret3.Value, uint(2))
	require.Equal(s.T(), ret3.Tag, Tag("op:read"))
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 3)
	require.Contains(s.T(), vals, Tag("op:read"))
	require.Equal(s.T(), vals[Tag("op:read")], uint(2))
	require.Contains(s.T(), vals, Tag("gid:13"))
	require.Equal(s.T(), vals[Tag("gid:13")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(1))
	// Untrack of failed must be a NOP
	ret3.Untrack()
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 3)
	require.Contains(s.T(), vals, Tag("op:read"))
	require.Equal(s.T(), vals[Tag("op:read")], uint(2))
	require.Contains(s.T(), vals, Tag("gid:13"))
	require.Equal(s.T(), vals[Tag("gid:13")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(1))
	// Untrack Request 1
	ret1.Untrack()
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 2)
	require.Contains(s.T(), vals, Tag("op:read"))
	require.Equal(s.T(), vals[Tag("op:read")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(1))
	// Untrack Request 2
	ret2.Untrack()
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 0)
}

// Test multiple matching rules will always match the first
func (s *ScorecardSuite) TestMultipleRules() {
	// Setup
	rules := []Rule{{"op:read", 1}, {"op:read", 2}}
	req1 := []Tag{"op:read", "gid:13"}
	req2 := []Tag{"op:read", "gid:42"}
	sc := NewScorecard(rules)
	// Request 1
	ret := sc.TrackRequest(req1)
	require.True(s.T(), ret.Tracked)
	// Request 2
	ret = sc.TrackRequest(req2)
	require.False(s.T(), ret.Tracked)
	require.Equal(s.T(), ret.Violated, Rule{"op:read", 1})
	require.Equal(s.T(), ret.Value, uint(1))
	require.Equal(s.T(), ret.Tag, Tag("op:read"))
}

// Test pattern matching
func (s *ScorecardSuite) TestPatternMatching() {
	// Setup
	rules := []Rule{{"op:*", 1}}
	req1 := []Tag{"op:read"}
	req2 := []Tag{"op:write"}
	req3 := []Tag{"op:read"}
	sc := NewScorecard(rules)
	// Request 1
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	// Request 2
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)
	// Request 3
	ret3 := sc.TrackRequest(req3)
	require.False(s.T(), ret3.Tracked)
	require.Equal(s.T(), ret3.Violated, Rule{"op:*", 1})
	require.Equal(s.T(), ret3.Value, uint(1))
	// Will show specific tag that tripped the rule.
	require.Equal(s.T(), ret3.Tag, Tag("op:read"))
}

// Test conjunctions
func (s *ScorecardSuite) TestConjunctions() {
	// Setup
	rules := []Rule{{"op:list;gid:42", 1}}
	req1 := []Tag{"op:list"}
	req2 := []Tag{"gid:42"}
	req3 := []Tag{"op:list", "gid:42"}
	sc := NewScorecard(rules)
	// Request 1
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	// Request 2
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)
	// Request 3
	ret3 := sc.TrackRequest(req3)
	require.True(s.T(), ret3.Tracked)
	// Check the map state
	vals := sc.Inspect()
	require.Equal(s.T(), len(vals), 3)
	require.Contains(s.T(), vals, Tag("op:list"))
	require.Equal(s.T(), vals[Tag("op:list")], uint(2))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(2))
	require.Contains(s.T(), vals, Tag("op:list;gid:42"))
	require.Equal(s.T(), vals[Tag("op:list;gid:42")], uint(1))
	// Untrack all and see empty map
	ret2.Untrack()
	ret3.Untrack()
	ret1.Untrack()
	require.Equal(s.T(), len(sc.Inspect()), 0)
}

// Test conjunctions that isolate requests
func (s *ScorecardSuite) TestIsolatedConjunctions() {
	// Setup
	rules := []Rule{{"op:list;gid:42", 0}}
	req1 := []Tag{"op:list"}
	req2 := []Tag{"gid:42"}
	req3 := []Tag{"op:list", "gid:42"}
	sc := NewScorecard(rules)
	// Request 1
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	// Request 2
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)
	// Request 3
	ret3 := sc.TrackRequest(req3)
	require.False(s.T(), ret3.Tracked)
	require.Equal(s.T(), ret3.Violated, Rule{"op:list;gid:42", 0})
	require.Equal(s.T(), ret3.Value, uint(0))
	require.Equal(s.T(), ret3.Tag, Tag("op:list;gid:42"))
	// Check the map state
	vals := sc.Inspect()
	require.Equal(s.T(), len(vals), 2)
	require.Contains(s.T(), vals, Tag("op:list"))
	require.Equal(s.T(), vals[Tag("op:list")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(1))
	// Undo all and see empty map
	ret2.Untrack()
	ret1.Untrack()
	require.Equal(s.T(), len(sc.Inspect()), 0)
}

// Test pattern conjunctions
func (s *ScorecardSuite) TestPatternConjunctions() {
	// Setup
	rules := []Rule{{"op:*;gid:*", 1}}
	req1 := []Tag{"op:list"}
	req2 := []Tag{"gid:42"}
	req3 := []Tag{"op:list", "gid:42"}
	sc := NewScorecard(rules)
	// Request 1
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	// Request 2
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)
	// Request 3
	ret3 := sc.TrackRequest(req3)
	require.True(s.T(), ret3.Tracked)
	// Check the map state
	vals := sc.Inspect()
	require.Equal(s.T(), len(vals), 3)
	require.Contains(s.T(), vals, Tag("op:list"))
	require.Equal(s.T(), vals[Tag("op:list")], uint(2))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(2))
	require.Contains(s.T(), vals, Tag("op:list;gid:42"))
	require.Equal(s.T(), vals[Tag("op:list;gid:42")], uint(1))
	// Undo all and see empty map
	ret1.Untrack()
	ret2.Untrack()
	ret3.Untrack()
	require.Equal(s.T(), len(sc.Inspect()), 0)
}

// Test patterned conjunctions that isolate requests
func (s *ScorecardSuite) TestIsolatedPatternConjunctions() {
	// Setup
	rules := []Rule{{"op:*;gid:*", 0}}
	req1 := []Tag{"op:list"}
	req2 := []Tag{"gid:42"}
	req3 := []Tag{"op:list", "gid:42"}
	sc := NewScorecard(rules)
	// Request 1
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	// Request 2
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)
	// Request 3
	ret3 := sc.TrackRequest(req3)
	require.False(s.T(), ret3.Tracked)
	require.Equal(s.T(), ret3.Violated, Rule{"op:*;gid:*", 0})
	require.Equal(s.T(), ret3.Value, uint(0))
	// Check the map state
	vals := sc.Inspect()
	require.Equal(s.T(), len(vals), 2)
	require.Contains(s.T(), vals, Tag("op:list"))
	require.Equal(s.T(), vals[Tag("op:list")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(1))
	// Undo all and see empty map
	ret1.Untrack()
	ret2.Untrack()
	require.Equal(s.T(), len(sc.Inspect()), 0)
}

func (s *ScorecardSuite) TestDuplicate() {
	req := []Tag{"op:gid", "colo:1"}
	sc := NewScorecard([]Rule{{"op:gid;colo:*", 1}, {"op:gid;colo:*", 1}})
	t1 := sc.TrackRequest(req)
	require.Equal(s.T(), t1.Tracked, true)
}

// Test if we have two rules with the same pattern, the first rule overrides the second
func (s *ScorecardSuite) TestOverride() {
	sc := NewScorecard([]Rule{{"op:txn", 100}, {"op:txn", 1}})
	req := []Tag{"op:txn"}
	for i := 0; i < 100; i++ {
		t := sc.TrackRequest(req)
		require.Equal(s.T(), t.Tracked, true)
	}

	t := sc.TrackRequest(req)
	require.Equal(s.T(), t.Tracked, false)
}

const CONCURRENT = 128

func concurrentTester(sc Scorecard, iters int, wg *sync.WaitGroup) {
	tracked := make([]*TrackingInfo, 0, CONCURRENT+1)
	for i := 0; i < iters; i++ {
		num := insecure_random.Int63()
		tags := make([]Tag, 4)
		tags[0] = Tag("op:read")
		tags[1] = Tag(fmt.Sprintf("colo:%d", num&0xffffff))
		tags[2] = Tag(fmt.Sprintf("gid:%d", num))
		tags[3] = Tag(fmt.Sprintf("nsid:%d", num))
		ret := sc.TrackRequest(tags)
		if ret.Tracked {
			tracked = append(tracked, ret)
		}
		if len(tracked) > CONCURRENT {
			idx := insecure_random.Intn(len(tracked))
			tracked[idx] = tracked[CONCURRENT]
			tracked = tracked[:CONCURRENT]
		}
	}
	wg.Done()
}

const PARALLEL = 16
const ITERS = 100000

// Test that it's OK to access rules and other slices initialized in the
// scorecard without further synchronization during normal operation.
//
// The test doesn't assert anything, but should be run under race detector.
func (s *ScorecardSuite) TestConcurrent() {
	// Setup
	rules := []Rule{{"op:*;gid:*", 5}, {"gid:*", 5}, {"colo:*", 5}}
	sc := NewScorecard(rules)
	wg := &sync.WaitGroup{}
	wg.Add(PARALLEL)
	for i := 0; i < PARALLEL; i++ {
		go concurrentTester(sc, ITERS, wg)
	}
	wg.Wait()
}

// This is different than the concurrent tester b/c the colos are identical every single time. In
// particular, this catches an off-by-one error where we hit a limit on the second or Nth rule but
// only dereferenced the first N-2.
func (s *ScorecardSuite) TestParallel() {
	sc := NewScorecard([]Rule{
		{"op:gid_create_txn;colo:*", 1},
	})

	tags := []Tag{
		"source:UNKNOWN_SERVICE",
		"traffic:LIVE_TRAFFIC",
		"op:gid_create_txn",
		"colo:0",
		"colo:1",
	}

	num := 100
	for j := 0; j < 200; j++ {
		var wg sync.WaitGroup
		wg.Add(num)
		for i := 0; i < num; i++ {
			go func(index int) {
				defer wg.Done()
				t := sc.TrackRequest(tags)
				if t.Tracked {
					t.Untrack()
				}
			}(i)
		}
		wg.Wait()
		require.Equal(s.T(), len(sc.Inspect()), 0, "SCORECARD HAS LEAKED")
	}
}

func (s *ScorecardSuite) TestReconfigureIncreaseCapacity() {
	rules := []Rule{
		{
			Pattern:  "op:*",
			Capacity: 1,
		},
		{
			Pattern:  "gid:*",
			Capacity: 1,
		},
	}
	req1 := []Tag{"op:list"}
	req2 := []Tag{"gid:42"}

	sc := NewDynamicScorecard(rules)
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)

	newRules := []Rule{
		{
			Pattern:  "op:*",
			Capacity: 2,
		},
		{
			Pattern:  "gid:*",
			Capacity: 1,
		},
	}
	// should be allowed with new rules
	req3 := []Tag{"op:list"}
	// should be rejected
	req4 := []Tag{"gid:42"}
	// should be also rejected
	req5 := []Tag{"op:list"}

	sc.Reconfigure(newRules)

	ret3 := sc.TrackRequest(req3)
	require.True(s.T(), ret3.Tracked)

	ret4 := sc.TrackRequest(req4)
	require.False(s.T(), ret4.Tracked)

	ret5 := sc.TrackRequest(req5)
	require.False(s.T(), ret5.Tracked)

	ret1.Untrack()
	ret2.Untrack()
	ret3.Untrack()
	ret4.Untrack()
	ret5.Untrack()
	require.Equal(s.T(), len(sc.Inspect()), 0)
}

func (s *ScorecardSuite) TestReconfigureDecreaseCapacity() {
	rules := []Rule{
		{
			Pattern:  "op:*",
			Capacity: 2,
		},
		{
			Pattern:  "gid:*",
			Capacity: 1,
		},
	}
	// take whole resource
	req1 := []Tag{"op:list"}
	req2 := []Tag{"op:list"}
	req3 := []Tag{"gid:42"}

	sc := NewDynamicScorecard(rules)
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)

	ret3 := sc.TrackRequest(req3)
	require.True(s.T(), ret3.Tracked)

	newRules := []Rule{
		{
			Pattern:  "op:*",
			Capacity: 1,
		},
		{
			Pattern:  "gid:*",
			Capacity: 1,
		},
	}
	// should be rejected with new rules
	req4 := []Tag{"op:list"}
	// should be rejected
	req5 := []Tag{"gid:42"}

	sc.Reconfigure(newRules)

	ret4 := sc.TrackRequest(req4)
	require.False(s.T(), ret4.Tracked)

	ret5 := sc.TrackRequest(req5)
	require.False(s.T(), ret5.Tracked)

	ret1.Untrack()

	// still reject
	req6 := []Tag{"op:list"}

	ret6 := sc.TrackRequest(req6)
	require.False(s.T(), ret6.Tracked)

	ret2.Untrack()

	// now ok
	req7 := []Tag{"op:list"}

	ret7 := sc.TrackRequest(req7)
	require.True(s.T(), ret7.Tracked)

	ret1.Untrack()
	ret2.Untrack()
	ret3.Untrack()
	ret4.Untrack()
	ret5.Untrack()
	ret6.Untrack()
	ret7.Untrack()
	require.Equal(s.T(), len(sc.Inspect()), 0)
}

func (s *ScorecardSuite) TestReconfigureNewRule() {
	rules := []Rule{
		{
			Pattern:  "op:*",
			Capacity: 1,
		},
		{
			Pattern:  "gid:*",
			Capacity: 1,
		},
	}
	req1 := []Tag{"op:list"}
	req2 := []Tag{"gid:42"}

	// take new resource
	req3 := []Tag{"new:30"}

	sc := NewDynamicScorecard(rules)
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)
	// Request 2
	ret3 := sc.TrackRequest(req3)
	require.True(s.T(), ret3.Tracked)

	newRules := []Rule{
		{
			Pattern:  "op:*",
			Capacity: 1,
		},
		{
			Pattern:  "gid:*",
			Capacity: 1,
		},
		{
			Pattern:  "new:*",
			Capacity: 1,
		},
	}

	// no resource under new rules
	req4 := []Tag{"new:30"}
	sc.Reconfigure(newRules)

	ret4 := sc.TrackRequest(req4)
	require.False(s.T(), ret4.Tracked)

	ret1.Untrack()
	ret2.Untrack()
	ret3.Untrack()
	ret4.Untrack()
	require.Equal(s.T(), len(sc.Inspect()), 0)
}

func (s *ScorecardSuite) TestReconfigureDeleteRule() {
	rules := []Rule{
		{
			Pattern:  "op:*",
			Capacity: 1,
		},
		{
			Pattern:  "nsid:*;gid:*",
			Capacity: 1,
		},
	}
	req1 := []Tag{"op:list"}
	req2 := []Tag{"nsid:abc", "gid:42"}

	sc := NewDynamicScorecard(rules)
	ret1 := sc.TrackRequest(req1)
	require.True(s.T(), ret1.Tracked)
	ret2 := sc.TrackRequest(req2)
	require.True(s.T(), ret2.Tracked)

	newRules := []Rule{
		{
			Pattern:  "op:*",
			Capacity: 1,
		},
	}

	// op still rejected
	req3 := []Tag{"op:list"}
	// nsid;gid should be allowed because there is no compound rule now
	req4 := []Tag{"nsid:abc", "gid:42"}
	sc.Reconfigure(newRules)

	ret3 := sc.TrackRequest(req3)
	require.False(s.T(), ret3.Tracked)

	ret4 := sc.TrackRequest(req4)
	require.True(s.T(), ret4.Tracked)

	ret1.Untrack()
	ret2.Untrack()
	ret3.Untrack()
	ret4.Untrack()
	require.Equal(s.T(), len(sc.Inspect()), 0)
}

var testScorecard1 = []Rule{
	{"op:read", 1},
}

var testScorecard13b = []Rule{
	{"op:read", 2},
}

func (s *ScorecardSuite) TestDynamicScorecardForLeaks() {
	const threads = 100
	sc := NewDynamicScorecard(testScorecard1)
	results := make(chan struct{})
	done := make(chan struct{})
	go func() {
		time.Sleep(30 * time.Second)
		close(done)
	}()
	for i := 0; i < threads; i++ {
		go func(x int) {
			for {
				select {
				case <-done:
					results <- struct{}{}
					return
				default:
				}
				if x%2 == 0 {
					sc.Reconfigure(testScorecard1)
				} else {
					sc.Reconfigure(testScorecard13b)
				}
				go func() {
					t := sc.TrackRequest([]Tag{"op:read"})
					if t.Tracked {
						time.Sleep(10 * time.Millisecond)
						t.Untrack()
					}
				}()
			}
		}(i)
	}
	<-done
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for i := 0; i < threads; i++ {
		select {
		case <-timer.C:
			s.T().FailNow()
		case <-results:
		}
	}
}

func (s *ScorecardSuite) TestReconfigureParallel() {
	rules := []Rule{{"op:*;gid:*", 5}, {"gid:*", 5}, {"colo:*", 5}}
	configureVariants := [][]Rule{
		// base rules
		rules,
		// update rules
		{
			{"op:*;gid:*", 30},
			{"gid:*", 20},
			{"colo:*", 10},
		},
		// delete one rule
		{
			{"gid:*", 5},
			{"colo:*", 5},
		},
		// add new rule
		{
			{"op:*;gid:*", 5},
			{"gid:*", 5},
			{"colo:*", 5},
			{"nsid:*", 5},
		},
	}
	sc := NewDynamicScorecard(rules)
	wg := &sync.WaitGroup{}
	wg.Add(PARALLEL)
	reconfigureWg := &sync.WaitGroup{}
	reconfigureStopCh := make(chan struct{})
	for i := 0; i < 16; i++ {
		reconfigureWg.Add(1)
		go func() {
			defer reconfigureWg.Done()
			ticker := time.NewTicker(30 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					rulesToReconfig := configureVariants[insecure_random.Intn(len(configureVariants))]
					sc.Reconfigure(rulesToReconfig)
					// check Rules for races
					_ = sc.Rules()
				case <-reconfigureStopCh:
					return
				}
			}
		}()
	}
	for i := 0; i < PARALLEL; i++ {
		go concurrentTester(sc, ITERS, wg)
	}
	wg.Wait()
	close(reconfigureStopCh)
	reconfigureWg.Wait()
}
