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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type RuleParsingSuite struct {
	suite.Suite
}

func TestRuleParsingSuite(t *testing.T) {
	suite.Run(t, new(RuleParsingSuite))
}

type TagRuleMatch struct {
	T Tag
	R Rule
	M bool
}

// This is a cross-product of rules and tags from a snapshot of
// scorecard_test.go
var rules = []TagRuleMatch{
	{"meta_api", Rule{"op:*", 1}, false},
	{"meta_api", Rule{"op:*;gid:*", 0}, false},
	{"meta_api", Rule{"op:*;gid:*", 1}, false},
	{"meta_api", Rule{"op:list;gid:42", 0}, false},
	{"meta_api", Rule{"op:list;gid:42", 1}, false},
	{"meta_api", Rule{"op:read", 1}, false},
	{"meta_api", Rule{"op:read", 2}, false},
	{"meta_www", Rule{"op:*", 1}, false},
	{"meta_www", Rule{"op:*;gid:*", 0}, false},
	{"meta_www", Rule{"op:*;gid:*", 1}, false},
	{"meta_www", Rule{"op:list;gid:42", 0}, false},
	{"meta_www", Rule{"op:list;gid:42", 1}, false},
	{"meta_www", Rule{"op:read", 1}, false},
	{"meta_www", Rule{"op:read", 2}, false},
	{"List_Read", Rule{"op:*", 1}, false},
	{"List_Read", Rule{"op:*;gid:*", 0}, false},
	{"List_Read", Rule{"op:*;gid:*", 1}, false},
	{"List_Read", Rule{"op:list;gid:42", 0}, false},
	{"List_Read", Rule{"op:list;gid:42", 1}, false},
	{"List_Read", Rule{"op:read", 1}, false},
	{"List_Read", Rule{"op:read", 2}, false},
	{"Point_Read", Rule{"op:*", 1}, false},
	{"Point_Read", Rule{"op:*;gid:*", 0}, false},
	{"Point_Read", Rule{"op:*;gid:*", 1}, false},
	{"Point_Read", Rule{"op:list;gid:42", 0}, false},
	{"Point_Read", Rule{"op:list;gid:42", 1}, false},
	{"Point_Read", Rule{"op:read", 1}, false},
	{"Point_Read", Rule{"op:read", 2}, false},
	{"TeamUserAssoc", Rule{"op:*", 1}, false},
	{"TeamUserAssoc", Rule{"op:*;gid:*", 0}, false},
	{"TeamUserAssoc", Rule{"op:*;gid:*", 1}, false},
	{"TeamUserAssoc", Rule{"op:list;gid:42", 0}, false},
	{"TeamUserAssoc", Rule{"op:list;gid:42", 1}, false},
	{"TeamUserAssoc", Rule{"op:read", 1}, false},
	{"TeamUserAssoc", Rule{"op:read", 2}, false},
	{"UserEntity", Rule{"op:*", 1}, false},
	{"UserEntity", Rule{"op:*;gid:*", 0}, false},
	{"UserEntity", Rule{"op:*;gid:*", 1}, false},
	{"UserEntity", Rule{"op:list;gid:42", 0}, false},
	{"UserEntity", Rule{"op:list;gid:42", 1}, false},
	{"UserEntity", Rule{"op:read", 1}, false},
	{"UserEntity", Rule{"op:read", 2}, false},
	{"GID_10", Rule{"op:*", 1}, false},
	{"GID_10", Rule{"op:*;gid:*", 0}, false},
	{"GID_10", Rule{"op:*;gid:*", 1}, false},
	{"GID_10", Rule{"op:list;gid:42", 0}, false},
	{"GID_10", Rule{"op:list;gid:42", 1}, false},
	{"GID_10", Rule{"op:read", 1}, false},
	{"GID_10", Rule{"op:read", 2}, false},
	{"GID_20", Rule{"op:*", 1}, false},
	{"GID_20", Rule{"op:*;gid:*", 0}, false},
	{"GID_20", Rule{"op:*;gid:*", 1}, false},
	{"GID_20", Rule{"op:list;gid:42", 0}, false},
	{"GID_20", Rule{"op:list;gid:42", 1}, false},
	{"GID_20", Rule{"op:read", 1}, false},
	{"GID_20", Rule{"op:read", 2}, false},
	{"GID_30", Rule{"op:*", 1}, false},
	{"GID_30", Rule{"op:*;gid:*", 0}, false},
	{"GID_30", Rule{"op:*;gid:*", 1}, false},
	{"GID_30", Rule{"op:list;gid:42", 0}, false},
	{"GID_30", Rule{"op:list;gid:42", 1}, false},
	{"GID_30", Rule{"op:read", 1}, false},
	{"GID_30", Rule{"op:read", 2}, false},
	{"gid:13", Rule{"op:*", 1}, false},
	{"gid:13", Rule{"op:*;gid:*", 0}, false},
	{"gid:13", Rule{"op:*;gid:*", 1}, false},
	{"gid:13", Rule{"op:list;gid:42", 0}, false},
	{"gid:13", Rule{"op:list;gid:42", 1}, false},
	{"gid:13", Rule{"op:read", 1}, false},
	{"gid:13", Rule{"op:read", 2}, false},
	{"gid:1337", Rule{"op:*", 1}, false},
	{"gid:1337", Rule{"op:*;gid:*", 0}, false},
	{"gid:1337", Rule{"op:*;gid:*", 1}, false},
	{"gid:1337", Rule{"op:list;gid:42", 0}, false},
	{"gid:1337", Rule{"op:list;gid:42", 1}, false},
	{"gid:1337", Rule{"op:read", 1}, false},
	{"gid:1337", Rule{"op:read", 2}, false},
	{"gid:42", Rule{"op:*", 1}, false},
	{"gid:42", Rule{"op:*;gid:*", 0}, false},
	{"gid:42", Rule{"op:*;gid:*", 1}, false},
	{"gid:42", Rule{"op:list;gid:42", 0}, false},
	{"gid:42", Rule{"op:list;gid:42", 1}, false},
	{"gid:42", Rule{"op:read", 1}, false},
	{"gid:42", Rule{"op:read", 2}, false},
	{"op:list", Rule{"op:*", 1}, true},
	{"op:list", Rule{"op:*;gid:*", 0}, false},
	{"op:list", Rule{"op:*;gid:*", 1}, false},
	{"op:list", Rule{"op:list;gid:42", 0}, false},
	{"op:list", Rule{"op:list;gid:42", 1}, false},
	{"op:list", Rule{"op:read", 1}, false},
	{"op:list", Rule{"op:read", 2}, false},
	{"op:read", Rule{"op:*", 1}, true},
	{"op:read", Rule{"op:*;gid:*", 0}, false},
	{"op:read", Rule{"op:*;gid:*", 1}, false},
	{"op:read", Rule{"op:list;gid:42", 0}, false},
	{"op:read", Rule{"op:list;gid:42", 1}, false},
	{"op:read", Rule{"op:read", 1}, true},
	{"op:read", Rule{"op:read", 2}, true},
	{"op:write", Rule{"op:*", 1}, true},
	{"op:write", Rule{"op:*;gid:*", 0}, false},
	{"op:write", Rule{"op:*;gid:*", 1}, false},
	{"op:write", Rule{"op:list;gid:42", 0}, false},
	{"op:write", Rule{"op:list;gid:42", 1}, false},
	{"op:write", Rule{"op:read", 1}, false},
	{"op:write", Rule{"op:read", 2}, false},
}

func (s *RuleParsingSuite) TestTable() {
	for _, x := range rules {
		assert.Equal(s.T(), TagMatchesRule(x.T, x.R), x.M, "TagMatchesRule(%v, %v)", x.T, x.R)
		assert.Equal(s.T(), x.T.Matches(x.R), x.M, "\"%v\".Matches(%v)", x.T, x.R)
		assert.Equal(s.T(), x.R.Matches(x.T), x.M, "%v.Matches(%v)", x.R, x.T)
	}
}

// True iff a and b are have the same tags in the same order
func SameTags(a []Tag, b []Tag) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *RuleParsingSuite) TestCompoundGenerateSimple() {
	ctg := newCompoundTagGenerator([]Rule{{"op:read;gid:42", 5}})
	_, tags := ctg.combine([]Tag{"op:read"})
	assert.True(s.T(), SameTags(tags, []Tag{}))
	_, tags = ctg.combine([]Tag{"op:read", "gid:*"})
	assert.True(s.T(), SameTags(tags, []Tag{}))
	_, tags = ctg.combine([]Tag{"op:read", "gid:13"})
	assert.True(s.T(), SameTags(tags, []Tag{}))
	_, tags = ctg.combine([]Tag{"op:read", "gid:42"})
	assert.True(s.T(), SameTags(tags, []Tag{"op:read;gid:42"}))
}

func (s *RuleParsingSuite) TestCompoundGenerateNontrivial() {
	ctg := newCompoundTagGenerator([]Rule{{"op:read", 2}})
	_, tags := ctg.combine([]Tag{"op:read"})
	assert.True(s.T(), SameTags(tags, []Tag{}))
}

func (s *RuleParsingSuite) TestWildcard() {
	ctg := newCompoundTagGenerator([]Rule{{"op:*;gid:*", 5}})
	_, tags := ctg.combine([]Tag{"op:read"})
	assert.True(s.T(), SameTags(tags, []Tag{}))
	_, tags = ctg.combine([]Tag{"op:read", "gid:*"})
	assert.True(s.T(), SameTags(tags, []Tag{"op:read;gid:*"}))
	_, tags = ctg.combine([]Tag{"op:read", "gid:42"})
	assert.True(s.T(), SameTags(tags, []Tag{"op:read;gid:42"}))
	_, tags = ctg.combine([]Tag{"gid:42", "op:read"})
	assert.True(s.T(), SameTags(tags, []Tag{"op:read;gid:42"}))
}

func (s *RuleParsingSuite) TestRuleDupes() {
	ctg := newCompoundTagGenerator([]Rule{{"op:*;gid:*", 5}, {"gid:*;op:*", 5}})
	_, tags := ctg.combine([]Tag{"op:read"})
	assert.True(s.T(), SameTags(tags, []Tag{}))
	_, tags = ctg.combine([]Tag{"op:read", "gid:*"})
	assert.True(s.T(), SameTags(tags, []Tag{"op:read;gid:*", "gid:*;op:read"}))
	_, tags = ctg.combine([]Tag{"op:read", "gid:42"})
	assert.True(s.T(), SameTags(tags, []Tag{"op:read;gid:42", "gid:42;op:read"}))
	_, tags = ctg.combine([]Tag{"gid:42", "op:read"})
	assert.True(s.T(), SameTags(tags, []Tag{"op:read;gid:42", "gid:42;op:read"}))
}

func (s *RuleParsingSuite) TestTagDupes() {
	ctg := newCompoundTagGenerator([]Rule{{"op:*;gid:*", 5}})
	_, tags := ctg.combine([]Tag{"op:read", "op:write", "op:list", "gid:42", "gid:13"})
	out := []Tag{
		"op:read;gid:42",
		"op:read;gid:13",
		"op:write;gid:42",
		"op:write;gid:13",
		"op:list;gid:42",
		"op:list;gid:13",
	}
	assert.True(s.T(), SameTags(tags, out))
}
