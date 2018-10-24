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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DynamicScorecardSuite struct {
	suite.Suite
}

func TestDynamicScorecardSuite(t *testing.T) {
	suite.Run(t, new(DynamicScorecardSuite))
}

var SCORECARD1 = []Rule{
	{"op:read", 1},
}

var SCORECARD3b = []Rule{
	{"op:read", 2},
}

func (s *DynamicScorecardSuite) expectationHelper(

	sc DynamicScorecard,
	hasActive bool,
	hasDecomm bool,
	activeActive uint64,
	activeDecomm uint64,
	decommActive uint64,
	decommDecomm uint64) {
	dsc := sc.(*dynamicScorecard)
	ha, hd, aa, ad, da, dd := dsc.peek()
	require.Equal(s.T(), ha, hasActive)
	require.Equal(s.T(), hd, hasDecomm)
	require.Equal(s.T(), aa, activeActive)
	require.Equal(s.T(), ad, activeDecomm)
	require.Equal(s.T(), da, decommActive)
	require.Equal(s.T(), dd, decommDecomm)
}

func (s *DynamicScorecardSuite) TestDynamicScorecard() {
	sc := NewDynamicScorecard([]Rule{})
	// Initial state
	require.Equal(s.T(), len(sc.Inspect()), 0)
	require.Equal(s.T(), len(sc.Rules()), 0)
	s.expectationHelper(sc, true, false, 0, 0, 0, 0)
	// There will be four requests (req 3 retried 3 times):
	// 1.  Without a scorecard in AFS (success)
	// 2.  With a scorecard in AFS that limits us to 1 read (success)
	// 3a. Attempt to perform a second read (suspicious)
	// 3b. With two scorecards: the one from step 2 and a score card that allows
	//     two read operations. (suspicious)
	// 3c. Op from 2 is untracked; request 3 processed (success)
	// 4.  Attempt to perform request (suspicious)
	// There will be 2 explicit scorecards (indices indicate when introduced):
	// 1.  []Rule{{"op:read", 1}}
	// 3b. []Rule{{"op:read", 2}}
	//
	// Scorecard reconfigure may be fired in a background goroutine to simulate
	// async reconfigure from AFS.  Tests (expectationHelper in particular) will
	// check the progress of the background routine.
	req1 := []Tag{"op:read", "gid:13"}
	req2 := []Tag{"op:read", "gid:42"}
	req3 := []Tag{"op:read", "gid:1337"}
	req4 := []Tag{"op:read", "gid:99"}
	// Step 1:
	ret1 := sc.TrackRequest(req1)
	vals := sc.Inspect()
	require.True(s.T(), ret1.Tracked)
	require.Equal(s.T(), len(vals), 2)
	require.Contains(s.T(), vals, Tag("op:read"))
	require.Equal(s.T(), vals[Tag("op:read")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:13"))
	require.Equal(s.T(), vals[Tag("gid:13")], uint(1))
	s.expectationHelper(sc, true, false, 1, 0, 0, 0)
	// Step 2:
	sc.Reconfigure(SCORECARD1)
	s.expectationHelper(sc, true, true, 0, 0, 1, 0)
	ret2 := sc.TrackRequest(req2)
	vals = sc.Inspect()
	require.True(s.T(), ret2.Tracked)
	require.Equal(s.T(), len(vals), 3)
	require.Contains(s.T(), vals, Tag("op:read"))
	// it equals 1 because the best estimate we can give is max of the two
	// scorecards, even though the requests are different.
	require.Equal(s.T(), vals[Tag("op:read")], uint(2))
	require.Contains(s.T(), vals, Tag("gid:13"))
	require.Equal(s.T(), vals[Tag("gid:13")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(1))
	// Step 3a:
	ret3 := sc.TrackRequest(req3)
	vals = sc.Inspect()
	require.False(s.T(), ret3.Tracked)
	require.Equal(s.T(), ret3.Violated, Rule{"op:read", 1})
	require.Equal(s.T(), ret3.Tag, Tag("op:read"))
	require.Equal(s.T(), len(vals), 3)
	require.Contains(s.T(), vals, Tag("op:read"))
	require.Equal(s.T(), vals[Tag("op:read")], uint(2))
	require.Contains(s.T(), vals, Tag("gid:13"))
	require.Equal(s.T(), vals[Tag("gid:13")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(1))
	// Step 3b:
	// this scorecard cannot go into effect yet; ret1 is still holding the
	// emtpy scorecard's ref
	s.expectationHelper(sc, true, true, 1, 0, 1, 1)
	go sc.Reconfigure(SCORECARD3b)
	s.expectationHelper(sc, true, true, 1, 0, 1, 1)
	ret3 = sc.TrackRequest(req3)
	vals = sc.Inspect()
	require.False(s.T(), ret3.Tracked)
	require.Equal(s.T(), ret3.Violated, Rule{"op:read", 1})
	require.Equal(s.T(), ret3.Tag, Tag("op:read"))
	require.Equal(s.T(), len(vals), 3)
	require.Contains(s.T(), vals, Tag("op:read"))
	require.Equal(s.T(), vals[Tag("op:read")], uint(2))
	require.Contains(s.T(), vals, Tag("gid:13"))
	require.Equal(s.T(), vals[Tag("gid:13")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:42"))
	require.Equal(s.T(), vals[Tag("gid:42")], uint(1))
	// Step 3c:
	ret1.Untrack()
	time.Sleep(10 * time.Millisecond) // sleep needed to let background catchup
	// shift right the scorecards, pushing in a new one with zero ref
	s.expectationHelper(sc, true, true, 0, 0, 1, 0)
	ret2.Untrack()
	time.Sleep(10 * time.Millisecond) // sleep needed to let background catchup
	// release the decomm2 scorecard
	s.expectationHelper(sc, true, false, 0, 0, 0, 0)
	vals = sc.Inspect()
	require.Equal(s.T(), len(vals), 0)
	ret3 = sc.TrackRequest(req3)
	vals = sc.Inspect()
	require.True(s.T(), ret3.Tracked)
	require.Equal(s.T(), len(vals), 2)
	require.Contains(s.T(), vals, Tag("op:read"))
	require.Equal(s.T(), vals[Tag("op:read")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:1337"))
	require.Equal(s.T(), vals[Tag("gid:1337")], uint(1))
	s.expectationHelper(sc, true, false, 1, 0, 0, 0)
	// Step 4:
	ret4 := sc.TrackRequest(req4)
	vals = sc.Inspect()
	require.True(s.T(), ret4.Tracked)
	require.Equal(s.T(), len(vals), 3)
	require.Contains(s.T(), vals, Tag("op:read"))
	require.Equal(s.T(), vals[Tag("op:read")], uint(2))
	require.Contains(s.T(), vals, Tag("gid:1337"))
	require.Equal(s.T(), vals[Tag("gid:1337")], uint(1))
	require.Contains(s.T(), vals, Tag("gid:99"))
	require.Equal(s.T(), vals[Tag("gid:99")], uint(1))
	s.expectationHelper(sc, true, false, 2, 0, 0, 0)
	// Inspect
	dsc := sc.(*dynamicScorecard)
	dsc.mtx.Lock()
	defer dsc.mtx.Unlock()
	require.NotNil(s.T(), dsc.active)
	require.Nil(s.T(), dsc.decomm)
}
