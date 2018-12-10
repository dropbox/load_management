package scorecard

import (
	"fmt"
	insecure_random "math/rand"
	"testing"
	"time"
)

func BenchmarkScorecard(b *testing.B) {
	b.SetParallelism(16)
	rules := [][]Rule{
		[]Rule{{"op:*;gid:*", 5}, {"gid:*", 5}, {"colo:*", 5}},
		[]Rule{{"op:*;gid:*", 10}, {"colo:*", 5}},
		[]Rule{{"gid:*", 5}, {"colo:*", 5}},
	}
	ops := []string{
		"select",
		"delete",
		"update",
		"insert",
	}
	sc := NewDynamicScorecard(rules[0])
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			op := ops[insecure_random.Intn(len(ops))]
			gid := insecure_random.Intn(5)
			colo := insecure_random.Intn(10)
			tags := []Tag{Tag("op:" + op), Tag(fmt.Sprintf("gid:%d", gid)), Tag(fmt.Sprintf("colo:%d", colo))}
			info := sc.TrackRequest(tags)
			if info.Tracked {
				time.Sleep(10 * time.Millisecond)
				info.Untrack()
			}
			// 10% of calls reconfigure scorecard
			if insecure_random.Float32() < 0.1 {
				newRules := rules[insecure_random.Intn(len(rules))]
				sc.Reconfigure(newRules)
			}
		}
	})
}
