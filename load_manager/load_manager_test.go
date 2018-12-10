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

package load_manager

import (
	"context"
	"fmt"
	insecure_rand "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ac "github.com/dropbox/load_management/admission_control"
	"github.com/dropbox/load_management/scorecard"
)

func TestGetResource(t *testing.T) {
	ctx := context.Background()

	queueNames, queues := makeQueues(1, 100)
	tag := scorecard.Tag("prefix:val")

	loadManager := NewLoadManager(
		queues,
		ac.NewAdmissionController(0),
		scorecard.NewDynamicScorecard(
			[]scorecard.Rule{scorecard.Rule{Pattern: string(tag), Capacity: 1}}),
		scorecard.NewDynamicScorecard(scorecard.NoRules()),
		scorecard.NoTags(),
	)

	r1 := loadManager.GetResource(ctx, queueNames[0], []scorecard.Tag{tag})
	require.True(t, r1.Acquired())
	// Populated with suspicious queue info
	require.Equal(t, r1.QueueInfo.Name, queueNames[0])
	require.Equal(t, r1.QueueInfo.Capacity, uint64(100))

	// Errors since we've hit limit
	r2 := loadManager.GetResource(ctx, queueNames[0], []scorecard.Tag{tag})
	require.False(t, r2.Acquired())
	require.True(t, r2.Suspicious())
	// Populated with suspicious queue info
	require.Equal(t, r2.QueueInfo.Name, suspiciousQueueName)
	require.Equal(t, r2.QueueInfo.Capacity, uint64(0))

	// After we release, we can acquire successfully
	r1.Release()
	r3 := loadManager.GetResource(ctx, queueNames[0], []scorecard.Tag{tag})
	require.True(t, r3.Acquired())
	defer r3.Release()
}

func TestGetResourceStrict(t *testing.T) {
	ctx := context.Background()

	queueNames, queues := makeQueues(1, 100)
	tag := scorecard.Tag("prefix:val")

	loadManager := NewLoadManager(
		queues,
		// Slow queue has no room and is gargantuanly high so we can ensure no race.
		ac.NewCustomAdmissionController(0, 1*time.Hour, 1*time.Hour),
		scorecard.NewDynamicScorecard(
			[]scorecard.Rule{scorecard.Rule{Pattern: string(tag), Capacity: 1}}),
		scorecard.NewDynamicScorecard(scorecard.NoRules()),
		scorecard.NoTags(),
	)

	r1 := loadManager.GetResource(ctx, queueNames[0], []scorecard.Tag{tag})
	require.True(t, r1.Acquired())
	defer r1.Release()

	// Errors since we've hit limit
	r2 := loadManager.GetResourceStrict(ctx, queueNames[0], []scorecard.Tag{tag})
	require.False(t, r2.Acquired())
	require.True(t, r2.Suspicious())

	rChan := make(chan *Resource, 1)
	go func() {
		rChan <- loadManager.GetResource(ctx, queueNames[0], []scorecard.Tag{tag})
	}()
	select {
	case <-rChan:
		t.Logf("GetResource should block on slowQueue but was faster than 3 seconds")
		t.FailNow()
	case <-time.After(3 * time.Second):
		// Should time out.
	}
}

func TestDoubleRelease(t *testing.T) {
	ctx := context.Background()

	queueNames, queues := makeQueues(1, 100)
	tag := scorecard.Tag("prefix:val")

	loadManager := NewLoadManager(
		queues,
		ac.NewAdmissionController(1),
		scorecard.NewDynamicScorecard(
			[]scorecard.Rule{scorecard.Rule{Pattern: string(tag), Capacity: 1}}),
		scorecard.NewDynamicScorecard(scorecard.NoRules()),
		scorecard.NoTags(),
	)

	r1 := loadManager.GetResource(ctx, queueNames[0], []scorecard.Tag{tag})
	require.True(t, r1.Acquired())
	// Double release is safe.
	r1.Release()
	require.False(t, r1.Acquired())
	r1.Release()
	require.False(t, r1.Acquired())

	// Check the same for slow path.
	r1 = loadManager.GetResource(ctx, queueNames[0], []scorecard.Tag{tag})
	defer r1.Release()
	r2 := loadManager.GetResource(ctx, queueNames[0], []scorecard.Tag{tag})
	require.True(t, r2.Acquired())
	// Double release is safe.
	r2.Release()
	require.False(t, r2.Acquired())
	r2.Release()
	require.False(t, r2.Acquired())
}

func TestDefaultTags(t *testing.T) {
	ctx := context.Background()

	queueNames, queues := makeQueues(1, 100)
	tag := scorecard.Tag("prefix:val")

	loadManager := NewLoadManager(
		queues,
		ac.NewAdmissionController(0),
		scorecard.NewDynamicScorecard(
			[]scorecard.Rule{scorecard.Rule{Pattern: string(tag), Capacity: 1}}),
		scorecard.NewDynamicScorecard(scorecard.NoRules()),
		[]scorecard.Tag{tag},
	)

	r1 := loadManager.GetResource(ctx, queueNames[0], scorecard.NoTags())
	require.True(t, r1.Acquired())
	defer r1.Release()

	// Errors since the default tag puts us over.
	r2 := loadManager.GetResource(ctx, queueNames[0], scorecard.NoTags())
	require.False(t, r2.Acquired())
	require.True(t, r2.Suspicious())
}

// Just test that it doesn't cause failure. Doesn't test logging side effect.
func TestCanary(t *testing.T) {
	ctx := context.Background()

	queueNames, queues := makeQueues(1, 100)
	tag := scorecard.Tag("prefix:val")

	loadManager := NewLoadManager(
		queues,
		ac.NewAdmissionController(0),
		scorecard.NewDynamicScorecard(scorecard.NoRules()),
		scorecard.NewDynamicScorecard(
			[]scorecard.Rule{scorecard.Rule{Pattern: string(tag), Capacity: 1}}),
		scorecard.NoTags(),
	)

	r1 := loadManager.GetResource(ctx, queueNames[0], []scorecard.Tag{tag})
	require.True(t, r1.Acquired())
	defer r1.Release()

	r2 := loadManager.GetResource(ctx, queueNames[0], []scorecard.Tag{tag})
	require.True(t, r2.Acquired())
	require.False(t, r2.Suspicious())
	require.True(t, r2.CanarySuspicious())
	defer r2.Release()
}

func TestMultiQueue(t *testing.T) {
	ctx := context.Background()

	queueNames, queues := makeQueues(2, 1)

	loadManager := NewLoadManager(
		queues,
		ac.NewAdmissionController(0),
		scorecard.NewDynamicScorecard(scorecard.NoRules()),
		scorecard.NewDynamicScorecard(scorecard.NoRules()),
		scorecard.NoTags(),
	)

	r1 := loadManager.GetResource(ctx, queueNames[0], scorecard.NoTags())
	require.True(t, r1.Acquired())
	defer r1.Release()

	// Same queue rejects
	r2 := loadManager.GetResource(ctx, queueNames[0], scorecard.NoTags())
	require.False(t, r2.Acquired())
	// NOT a scorecard error.
	require.False(t, r2.Suspicious())

	// Different queue ok
	r2 = loadManager.GetResource(ctx, queueNames[1], scorecard.NoTags())
	require.True(t, r2.Acquired())
	defer r2.Release()

	// Unknown queue rejects
	r3 := loadManager.GetResource(ctx, "garbage", scorecard.NoTags())
	require.False(t, r3.Acquired())
}

func TestStop(t *testing.T) {
	ctx := context.Background()

	queueNames, queues := makeQueues(1, 1)

	loadManager := NewLoadManager(
		queues,
		ac.NewAdmissionController(1),
		scorecard.NewDynamicScorecard(scorecard.NoRules()),
		scorecard.NewDynamicScorecard(scorecard.NoRules()),
		scorecard.NoTags(),
	)

	loadManager.Stop()

	r1 := loadManager.GetResource(ctx, queueNames[0], scorecard.NoTags())
	require.False(t, r1.Acquired())
	// Overload error, not scorecard.
	require.False(t, r1.Suspicious())
}

func TestLoadManagerRefcounting(t *testing.T) {
	capacity := 100
	numTags := 10
	numRules := 10

	prefixes, vals, rules := generatePrefixesValsAndRules(numTags, numRules, capacity)
	canaryRules := rulesFromTagParts(prefixes, vals, numRules/2, capacity/2)

	queueNames, queues := makeQueues(2, uint(capacity))
	slowQueue := ac.NewAdmissionController(uint(capacity))

	defaultTags := []scorecard.Tag{randomTag(prefixes, vals)}

	t.Logf("Testing configuration:\n\tRules: %v\n\tCanary Rules: %v\n\tDefault Tag: %v\n",
		rules, canaryRules, defaultTags)

	loadManager := NewLoadManager(
		queues,
		slowQueue,
		scorecard.NewDynamicScorecard(rules),
		scorecard.NewDynamicScorecard(canaryRules),
		defaultTags,
	)

	parallelism := 200
	numIters := 1000 // 30s with gorace on, 5s without
	wg := sync.WaitGroup{}
	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer wg.Done()

			ctx := context.Background()
			var tags []scorecard.Tag
			var resource *Resource

			for j := 0; j < numIters; j++ {
				tags = make([]scorecard.Tag, insecure_rand.Intn(numTags))
				for idx := range tags {
					tags[idx] = randomTag(prefixes, vals)
				}

				queueName := queueNames[insecure_rand.Intn(len(queueNames))]
				useStrict := insecure_rand.Int()%2 == 0

				if useStrict {
					resource = loadManager.GetResourceStrict(ctx, queueName, tags)
				} else {
					resource = loadManager.GetResource(ctx, queueName, tags)
				}

				if resource.Acquired() {
					resource.Release()
					// Sanity check double release.
					resource.Release()
				}
			}
		}()
	}

	wg.Wait()

	// All queues are empty
	for _, ac := range queues {
		require.Equal(t, uint64(0), ac.Admitted())
	}
	require.Equal(t, uint64(0), slowQueue.Admitted())

	// Scorecard is empty
	currentScorecard := loadManager.Scorecard.Inspect()
	for _, val := range currentScorecard {
		require.Equal(t, uint(0), val)
	}

	// Canary scorecard is empty
	currentScorecard = loadManager.CanaryScorecard.Inspect()
	for _, val := range currentScorecard {
		require.Equal(t, uint(0), val)
	}
}

func makeQueues(num int, capacity uint) ([]string, map[string]ac.AdmissionController) {
	queueNames := make([]string, num)
	for idx := range queueNames {
		queueNames[idx] = fmt.Sprintf("queue-%d", idx)
	}
	queues := make(map[string]ac.AdmissionController)
	for _, queueName := range queueNames {
		queues[queueName] = ac.NewAdmissionController(uint(capacity))
	}
	return queueNames, queues
}

// Generates appropriate number of tag prefixes, tag values, and rules. Rules may be duplicated.
// Doesn't worry about composite rules as we are not testing scorecard itself.
//
// Full tags take the form tag-N:val-M
func generatePrefixesValsAndRules(
	numTags, numRules, maxCap int) ([]string, []string, []scorecard.Rule) {

	prefixes := make([]string, numTags)
	vals := make([]string, numTags)
	for i := 0; i < cap(prefixes); i++ {
		prefixes[i] = fmt.Sprintf("tag-%d", i)
		vals[i] = fmt.Sprintf("val-%d", i)
	}
	rules := rulesFromTagParts(prefixes, vals, numRules, maxCap)
	return prefixes, vals, rules
}

func rulesFromTagParts(prefixes, vals []string, numRules, maxCap int) []scorecard.Rule {
	rules := make([]scorecard.Rule, 0, numRules)
	for len(rules) < numRules {
		val := scorecard.WildCard
		// Allow '*' for some rules, hence the + 1
		randIdx := insecure_rand.Intn(len(vals) + 1)
		if randIdx < len(vals) {
			val = vals[randIdx]
		}
		pattern := prefixes[insecure_rand.Intn(len(prefixes))] + scorecard.TagJoiner + val
		rules = append(rules, scorecard.Rule{
			Pattern:  pattern,
			Capacity: uint(insecure_rand.Intn(maxCap)),
		})
	}
	return rules
}

func randomTag(prefixes, vals []string) scorecard.Tag {
	tag := prefixes[insecure_rand.Intn(len(prefixes))] + scorecard.TagJoiner +
		vals[insecure_rand.Intn(len(vals))]
	return scorecard.Tag(tag)
}
