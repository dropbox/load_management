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

	"github.com/stretchr/testify/require"
)

func TestMutations(t *testing.T) {
	config := Config{
		Rules: []Rule{
			{
				Pattern:  "rule_to_update:*",
				Capacity: 1,
			},
			{
				Pattern:  "rule_to_update_duplicate:a",
				Capacity: 2,
			},
			{
				Pattern:  "rule_to_delete_duplicate:b",
				Capacity: 3,
			},
			{
				Pattern:  "rule_to_delete:*",
				Capacity: 4,
			},
			{
				Pattern:  "rule_to_delete:b",
				Capacity: 5,
			},
			{
				Pattern:  "rule_to_update_duplicate:a",
				Capacity: 6,
			},
			{
				Pattern:  "rule_to_delete_duplicate:b",
				Capacity: 7,
			},
		},
	}

	configPtr := &config

	// Add succeeds
	err := configPtr.Add("rule_to_add:*", 8)
	require.NoError(t, err)

	// Duplicate add fails.
	err = configPtr.Add("rule_to_add:*", 9)
	require.Error(t, err)

	// Update succeeds
	err = configPtr.Update("rule_to_update:*", 10)
	require.NoError(t, err)

	// Update affects all duplicates of pattern
	err = configPtr.Update("rule_to_update_duplicate:a", 11)
	require.NoError(t, err)

	// Delete succeeds
	err = configPtr.Delete("rule_to_delete:*")
	require.NoError(t, err)

	// Delete missing fails
	err = configPtr.Delete("rule_to_delete:c")
	require.Error(t, err)

	// Delete affects all duplicates of pattern
	err = configPtr.Delete("rule_to_delete_duplicate:b")
	require.NoError(t, err)

	require.Equal(t, []Rule{
		{
			Pattern:  "rule_to_update:*",
			Capacity: 10,
		},
		{
			Pattern:  "rule_to_update_duplicate:a",
			Capacity: 11,
		},
		{
			Pattern:  "rule_to_delete:b",
			Capacity: 5,
		},
		{
			Pattern:  "rule_to_update_duplicate:a",
			Capacity: 11,
		},
		{
			Pattern:  "rule_to_add:*",
			Capacity: 8,
		},
	}, config.Rules)
}
