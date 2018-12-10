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

package admission_control

import (
	"sync"
	"testing"
	"time"
)

func approx(expected, returned time.Duration, tolerance float64) bool {
	e := expected.Seconds()
	r := expected.Seconds()
	return e*(1-tolerance) <= r && r <= e*(1+tolerance)
}

func checkTimeout(t *testing.T, ac AdmissionController, to time.Duration) {
	start := time.Now()
	tk := ac.AdmitOne()
	end := time.Now()
	if tk != nil {
		t.Fatal("admission control failed to deny entry")
	}
	dur := end.Sub(start)
	if !approx(to, dur, 0.1) {
		t.Fatalf("admission control timeout incorrect: %v should be ~%v", dur, to)
	}
}

func TestConstructionOnly(t *testing.T) {
	NewAdmissionController(30)
}

func TestDefaultTimeout(t *testing.T) {
	ac := NewAdmissionController(1)
	defer ac.Stop()
	ac.AdmitOne()
	checkTimeout(t, ac, defaultN)
	time.Sleep(defaultN + defaultN/10)
	checkTimeout(t, ac, defaultN)
}

func TestResizeFails(t *testing.T) {
	ac := NewAdmissionController(1)
	if ac.Resize(10) != 1 {
		t.Fatal("resize failed")
	}
}

func TestStopFails(t *testing.T) {
	ac := NewAdmissionController(1)
	ac.Stop()
	if ac.AdmitOne() != nil {
		t.Fatal("got ticket from stopped controller")
	}
}

func TestNilTicketRelease(t *testing.T) {
	var tk *Ticket
	tk.Release()
}

func TestConcurrency(t *testing.T) {
	const CONCURRENCY = 16
	const ITERATIONS = 10000000
	ac := NewAdmissionController(CONCURRENCY)
	defer ac.Stop()
	iters := make(chan interface{})
	tickets := make(chan *Ticket, CONCURRENCY)
	for i := uint(0); i < CONCURRENCY; i++ {
		tickets <- nil
	}
	go func() {
		for i := 0; i < ITERATIONS; i++ {
			iters <- true
		}
		close(iters)
	}()
	wg := sync.WaitGroup{}
	wg.Add(CONCURRENCY)
	for i := 0; i < CONCURRENCY; i++ {
		go func() {
			for range iters {
				t := <-tickets
				if t != nil {
					t.Release()
				}
				x := ac.AdmitOne()
				tickets <- x
			}
			wg.Done()
		}()
	}
	wg.Wait()
	close(tickets)
	for t := range tickets {
		if t != nil {
			t.Release()
		}
	}
	ac.(*admissionControllerImpl).assertIdle()
}

// This is the default benchmark with a minimal NOP of an implemenation.
// Latencies to measure benchmark overhead.
//
// % bzl test //go/src/dropbox/util/admission_control:admission_control_test --test_arg '-test.bench=.' --test_arg '-test.benchtime=10s' --test_arg '-test.cpu=1,2,4,8' --test_output=streamed --nocache_test_results --test_timeout 3600
// WARNING: Streamed test output requested. All tests will be run locally, without sharding, one at a time.
// INFO: Found 1 test target...
// PASS
// BenchmarkNoConcurrency          2000000000               7.88 ns/op
// BenchmarkNoConcurrency-2        2000000000               7.85 ns/op
// BenchmarkNoConcurrency-4        2000000000               7.85 ns/op
// BenchmarkNoConcurrency-8        2000000000               7.85 ns/op
// BenchmarkConcurrency1e0         200000000               88.7 ns/op
// BenchmarkConcurrency1e0-2       50000000               265 ns/op
// BenchmarkConcurrency1e0-4       50000000               263 ns/op
// BenchmarkConcurrency1e0-8       50000000               266 ns/op
// BenchmarkConcurrency1e1         200000000               89.5 ns/op
// BenchmarkConcurrency1e1-2       100000000              181 ns/op
// BenchmarkConcurrency1e1-4       100000000              179 ns/op
// BenchmarkConcurrency1e1-8       100000000              177 ns/op
// BenchmarkConcurrency1e2         200000000               89.2 ns/op
// BenchmarkConcurrency1e2-2       100000000              169 ns/op
// BenchmarkConcurrency1e2-4       100000000              171 ns/op
// BenchmarkConcurrency1e2-8       100000000              169 ns/op
// BenchmarkConcurrency1e3         200000000               89.2 ns/op
// BenchmarkConcurrency1e3-2       100000000              169 ns/op
// BenchmarkConcurrency1e3-4       100000000              169 ns/op
// BenchmarkConcurrency1e3-8       100000000              168 ns/op
// BenchmarkConcurrency1e4         200000000               89.2 ns/op
// BenchmarkConcurrency1e4-2       100000000              169 ns/op
// BenchmarkConcurrency1e4-4       100000000              168 ns/op
// BenchmarkConcurrency1e4-8       100000000              168 ns/op
// BenchmarkConcurrency1e5         200000000               89.7 ns/op
// BenchmarkConcurrency1e5-2       100000000              171 ns/op
// BenchmarkConcurrency1e5-4       100000000              169 ns/op
// BenchmarkConcurrency1e5-8       100000000              169 ns/op
// BenchmarkConcurrency1e6         200000000               90.3 ns/op
// BenchmarkConcurrency1e6-2       100000000              171 ns/op
// BenchmarkConcurrency1e6-4       100000000              169 ns/op
// BenchmarkConcurrency1e6-8       100000000              169 ns/op
// BenchmarkConcurrency1e7         200000000               90.1 ns/op
// BenchmarkConcurrency1e7-2       100000000              171 ns/op
// BenchmarkConcurrency1e7-4       100000000              169 ns/op
// BenchmarkConcurrency1e7-8       100000000              169 ns/op

// This is what it was as of the first full checkin
// % bzl test //go/src/dropbox/util/admission_control:admission_control_test --test_arg '-test.bench=.' --test_arg '-test.benchtime=1s' --test_arg '-test.cpu=1,2,4,8' --test_output=streamed --nocache_test_results --test_timeout 3600
// WARNING: Streamed test output requested. All tests will be run locally, without sharding, one at a time.
// INFO: Found 1 test target...
// PASS
// BenchmarkNoConcurrency          10000000               189 ns/op
// BenchmarkNoConcurrency-2        10000000               192 ns/op
// BenchmarkNoConcurrency-4        10000000               188 ns/op
// BenchmarkNoConcurrency-8        10000000               187 ns/op
// BenchmarkConcurrency1e0          5000000               274 ns/op
// BenchmarkConcurrency1e0-2        3000000               476 ns/op
// BenchmarkConcurrency1e0-4        3000000               465 ns/op
// BenchmarkConcurrency1e0-8        3000000               470 ns/op
// BenchmarkConcurrency1e1          5000000               275 ns/op
// BenchmarkConcurrency1e1-2        5000000               389 ns/op
// BenchmarkConcurrency1e1-4        3000000               422 ns/op
// BenchmarkConcurrency1e1-8        3000000               436 ns/op
// BenchmarkConcurrency1e2          5000000               275 ns/op
// BenchmarkConcurrency1e2-2        5000000               389 ns/op
// BenchmarkConcurrency1e2-4        3000000               416 ns/op
// BenchmarkConcurrency1e2-8        3000000               443 ns/op
// BenchmarkConcurrency1e3          5000000               280 ns/op
// BenchmarkConcurrency1e3-2        5000000               389 ns/op
// BenchmarkConcurrency1e3-4        3000000               419 ns/op
// BenchmarkConcurrency1e3-8        3000000               438 ns/op
// BenchmarkConcurrency1e4          5000000               285 ns/op
// BenchmarkConcurrency1e4-2        5000000               396 ns/op
// BenchmarkConcurrency1e4-4        3000000               417 ns/op
// BenchmarkConcurrency1e4-8        3000000               443 ns/op
// BenchmarkConcurrency1e5          5000000               409 ns/op
// BenchmarkConcurrency1e5-2        3000000               447 ns/op
// BenchmarkConcurrency1e5-4        3000000               480 ns/op
// BenchmarkConcurrency1e5-8        3000000               494 ns/op
// BenchmarkConcurrency1e6          3000000               362 ns/op
// BenchmarkConcurrency1e6-2        5000000               430 ns/op
// BenchmarkConcurrency1e6-4        5000000               457 ns/op
// BenchmarkConcurrency1e6-8        5000000               482 ns/op
// BenchmarkConcurrency1e7          5000000               363 ns/op
// BenchmarkConcurrency1e7-2        5000000               437 ns/op
// BenchmarkConcurrency1e7-4        5000000               448 ns/op
// BenchmarkConcurrency1e7-8        5000000               472 ns/op

func BenchmarkNoConcurrency(b *testing.B) {
	ac := NewAdmissionController(30)
	defer ac.Stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := ac.AdmitOne()
		if x == nil {
			panic("benchmark failed")
		}
		x.Release()
	}
}

func helperConcurrency(conc, extra uint, b *testing.B) {
	if conc == 0 {
		panic("bad benchmark")
	}
	ac := NewAdmissionController(conc)
	defer ac.Stop()
	tickets := make(chan *Ticket, conc+extra)
	for i := uint(0); i < conc+extra; i++ {
		tickets <- nil
	}
	var mtx sync.Mutex
	failed := 0
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			t := <-tickets
			if t != nil {
				t.Release()
			}
			x := ac.AdmitOne()
			if x == nil {
				mtx.Lock()
				failed++
				mtx.Unlock()
			} else {
				tickets <- x
			}
		}
	})
	b.StopTimer()
	close(tickets)
	for t := range tickets {
		if t != nil {
			t.Release()
		}
	}
	mtx.Lock()
	defer mtx.Unlock()
	ac.(*admissionControllerImpl).assertIdle()
	if failed > 0 {
		b.Logf("there were %d acquire failures\n", failed)
	}
}

func BenchmarkConcurrency1e0(b *testing.B) {
	helperConcurrency(1e0, 0, b)
}

func BenchmarkConcurrency1e1(b *testing.B) {
	helperConcurrency(1e1, 0, b)
}

func BenchmarkConcurrency1e2(b *testing.B) {
	helperConcurrency(1e2, 0, b)
}

func BenchmarkConcurrency1e3(b *testing.B) {
	helperConcurrency(1e3, 0, b)
}

func BenchmarkConcurrency1e4(b *testing.B) {
	helperConcurrency(1e4, 0, b)
}

func BenchmarkConcurrency1e5(b *testing.B) {
	helperConcurrency(1e5, 0, b)
}

func BenchmarkConcurrency1e6(b *testing.B) {
	helperConcurrency(1e6, 0, b)
}

func BenchmarkConcurrency1e7(b *testing.B) {
	helperConcurrency(1e7, 0, b)
}

func BenchmarkConcurrency1e5WithExtra1e5(b *testing.B) {
	helperConcurrency(1e5, 1e3, b)
}
