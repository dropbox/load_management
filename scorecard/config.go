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
	"github.com/pkg/errors"
)

// Add rule if it does not exist. Errors if it does.
func (c *Config) Add(pattern string, capacity uint) error {
	newRule := Rule{
		Pattern:  pattern,
		Capacity: capacity,
	}
	for _, rule := range c.Rules {
		if rule.Pattern == pattern {
			return errors.Errorf(
				"Failed to add rule for pattern %s. Already exists: %v", pattern, rule)
		}
	}
	c.Rules = append(c.Rules, newRule)
	return nil
}

// Update (all instances of) rule with pattern. Error if none exist.
func (c *Config) Update(pattern string, capacity uint) error {
	updated := false
	for idx, rule := range c.Rules {
		if rule.Pattern == pattern {
			updated = true
			c.Rules[idx].Capacity = capacity
		}
	}
	if !updated {
		return errors.Errorf(
			"Failed to update rule for pattern %s. Did not exist", pattern)
	}
	return nil
}

// Delete (all instances of) rule with pattern. Error if none exist.
func (c *Config) Delete(pattern string) error {
	newRules := make([]Rule, 0, len(c.Rules))
	for _, rule := range c.Rules {
		if rule.Pattern != pattern {
			newRules = append(newRules, rule)
		}
	}
	if len(newRules) == len(c.Rules) {
		return errors.Errorf(
			"Failed to delete rule for pattern %s. Did not exist", pattern)
	}
	c.Rules = newRules
	return nil
}
