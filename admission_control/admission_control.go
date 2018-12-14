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

// Package admission_control provies support for efficient queuing.
package admission_control

import (
	"sync"
	"time"

	"github.com/pkg/errors"
)

// An AdmissionController limits concurrent requests in order to protect a
// service from trying to do too many things at once.  In its ideal use, one
// admission controller is placed in front of a resource that exhibits
// throughput collapse and prevents the throughput from collapsing by limiting
// concurrent requests to the resource.  This possibly prevents the service from
// reaching its maximum throughput, but adds an extra degree of safety.
//
// The admission controller can be as simple as a FIFO queue with a semaphore or
// use much more complex strategies.  The default admission controller provided
// by this package uses CoDel and adaptive LIFO strategies explained in the
// following resources:
//  - http://queue.acm.org/detail.cfm?id=2839461
//  - https://youtu.be/dlixGkelP9U
//
// An admission controller is internally synchronized.
type AdmissionController interface {
	// Admit one to the guarded resource.  If ticket is nil, the admission is
	// denied because of a timeout.  This should prompt either a fail or backoff
	// (depending on the type of service being admitted).  If the ticket is
	// granted, it will be non-nil and it is up to the caller to call "Release".
	//
	// Lost tickets will not be refunded or replaced, so `defer` is your friend.
	AdmitOne() *Ticket
	// This call returns one ticket to the ticket pool.  A ticket should be
	// passed to this call exactly once for each time it is returned by
	// AdmitOne.
	Release(t *Ticket)
	// Return the number of tickets issued.
	Admitted() uint64
	// Return the current size of the controller.
	Capacity() uint64
	// Possibly resize the controller to issue more or fewer tickets and return
	// the new size.  This call should be synchronous.  If this requests an
	// increase, it should immediately take effect and wake waiters.  If this
	// requests a decrease, it should block until the now-excess tickets are
	// reclaimed.  The description says "possibly" resize because some things
	// cannot be resized.  If the requested size is not possible, do nothing and
	// return the maximum size supported.
	// Success:  return == tickets
	// Failure:  return != tickets
	Resize(tickets uint64) uint64
	// Stop the admission controller.  Further use of it is implementation
	// defined.
	Stop()
}

// Ticket must be used to Release resource back to admission controller.
type Ticket struct {
	// AcquisitionElapsed measures the elapsed time during admission.  If
	// the ticket is acquired without blocking this duration is exactly
	// zero, since the fast path is not instrumented.  Slow acquisition
	// paths will set this to a non-zero duration.
	AcquisitionElapsed time.Duration
	IssuedBy           AdmissionController
}

// Release this ticket back to the admission controller that created it.
//
// This call wraps `admissionController.Release(ticket)` in a way that makes it
// safe to call an unlimited number of times while adhering to the admission
// controller interface's requirement that it be called exactly once.
// `defer ticket.Release()` to your heart's content :-)
func (t *Ticket) Release() {
	if t != nil && t.IssuedBy != nil {
		t.IssuedBy.Release(t)
		t.IssuedBy = nil
	}
}

// NewAdmissionController creates a new admission controller that will allow
// `parallelism` units of execution through at one time.  This uses the default
// parameters for the admission controller, and should work fine for most RPC-based services.
func NewAdmissionController(parallelism uint) AdmissionController {
	return NewCustomAdmissionController(parallelism, defaultM, defaultN)
}

// NewCustomAdmissionController creates a new admission controller that will
// allow `parallelism` units of execution through at one time.
// This uses the provided parameters for the admission controller.
//
// Only use this interface if you understand the implications of tuning M,N.
// Probably the only time you should ever investigate tuning M,N and using this
// interface is if your average request latency is more than N or an order of
// magnitude less than M.
func NewCustomAdmissionController(parallelism uint, M, N time.Duration) AdmissionController {
	return &admissionControllerImpl{
		M:       M,
		N:       N,
		allowed: uint64(parallelism),
		waiters: make([]*waiter, startingQueueLen),
		mode:    fifoQueueMode,
	}
}

// Implementation details only below this point

const defaultM = 5 * time.Millisecond
const defaultN = 100 * time.Millisecond
const startingQueueLen = 64

const checkDebugInvariants = false

type queueMode int

const (
	fifoQueueMode queueMode = iota
	lifoQueueMode
)

// This implements a LIFO/FIFO-switchable queue.  It's implemented as a ring
// buffer, so the queue itself can wrap around the end.  It maintains a
// contiguous group of (possibly nil---we'll get to that) elements bounded by
// head and tail with the invariant that head==tail => empty queue.  A full
// queue will have tail + 1 == head modulo wraparound.  tail will never refer to
// a valid waiter; this implies one slot in the queue is wasted.
//
// Enqueues always extend the tail by adding the waiter of the enqueuer.
//
// In FIFO mode, dequeue operations will pop and increment the head.
// In LIFO mode, dequeue operations will pop and decrement the tail.
//
// If the queue is full, a new buffer is allocated with more capacity.
//
// When a waiter times out waiting to be removed from the queue, it replaces its
// pointer with a nil.  Enqueue/dequeue ops are nil-aware and will transparently
// skip over them when doing enqueue/dequeue.
type admissionControllerImpl struct {
	mtx       sync.Mutex
	stopped   bool
	M         time.Duration
	N         time.Duration
	allowed   uint64
	lastEmpty time.Time
	admitted  uint64
	waiters   []*waiter
	mode      queueMode
	head      uint64
	tail      uint64
}

type waiter struct {
	index uint64
	wake  chan interface{}
}

func modularInc(x uint64, w int) uint64 {
	return (x + 1) % uint64(w)
}

func modularDec(x uint64, w int) uint64 {
	z := uint64(w)
	return (x + z - 1) % z
}

func (ac *admissionControllerImpl) AdmitOne() *Ticket {
	tk := &Ticket{0, ac}
	ac.mtx.Lock()
	stopped := ac.stopped
	admit := false
	if ac.head == ac.tail && ac.admitted < ac.allowed {
		ac.admitted++
		admit = true
	}
	ac.mtx.Unlock()
	if stopped {
		return nil
	}
	if admit {
		return tk
	}
	return ac.admitOneSlowPath(tk)
}

func (ac *admissionControllerImpl) admitOneSlowPath(tk *Ticket) (rv *Ticket) {
	t0 := time.Now()
	defer func() {
		if rv != nil {
			rv.AcquisitionElapsed = time.Since(t0)
		}
	}()

	w := &waiter{
		wake: make(chan interface{}),
	}
	timeout := ac.enqueueWaiter(w)
	proceed := false
	timer := time.NewTimer(timeout)
	select {
	case <-w.wake:
		proceed = true
	case <-timer.C:
	}
	timer.Stop()
	ac.removeWaiter(w)
	// Check for the race between our timeout above and the removeWaiter call.
	if !proceed {
		select {
		case <-w.wake:
			proceed = true
		default:
		}
	}
	if proceed {
		// The ac.admitted value is preserved when we were sent our wake.  To
		// set it here would introduce a race condition.
		return tk
	}
	return nil
}

func (ac *admissionControllerImpl) enqueueWaiter(w *waiter) time.Duration {
	now := time.Now()
	ac.mtx.Lock()
	defer ac.mtx.Unlock()
	ac.checkInvariants()
	if modularInc(ac.tail, len(ac.waiters)) == ac.head {
		ac.resizeWaiters()
	}
	ac.adjustQueueMode(now)
	w.index = ac.tail
	ac.waiters[ac.tail] = w
	ac.tail = modularInc(ac.tail, len(ac.waiters))
	ac.checkInvariants()
	if ac.mode == lifoQueueMode {
		return ac.M
	}
	return ac.N
}

func (ac *admissionControllerImpl) removeWaiter(w *waiter) {
	ac.mtx.Lock()
	defer ac.mtx.Unlock()
	ac.removeWaiterNoMtx(w)
}

// call while holding ac.mtx
func (ac *admissionControllerImpl) removeWaiterNoMtx(w *waiter) {
	ac.checkInvariants()
	if w.index >= uint64(len(ac.waiters)) {
		panic("admission controller invariants violated: waiter out of bounds")
	}
	// The waiter could have been removed by a wakeup, or it could be a remove
	// by timeout.  This check makes sure we don't modify waiters unless it is
	// to remove w from the slice.
	if ac.waiters[w.index] == w {
		ac.waiters[w.index] = nil
		if ac.head == w.index {
			ac.stripHeadNils()
		}
		if ac.tail == modularInc(w.index, len(ac.waiters)) {
			ac.stripTailNils()
		}
		if ac.head == ac.tail {
			// Calling Time() under a mtx is not ideal.  If this performs
			// poorly, evaluate how often this branch is taken and move the
			// Time() call out of the mutex if it is taken frequently.
			ac.adjustQueueMode(time.Now())
		}
	}
	ac.checkInvariants()
}

func (ac *admissionControllerImpl) Release(tk *Ticket) {
	ac.mtx.Lock()
	defer ac.mtx.Unlock()
	ac.checkInvariants()
	if ac.admitted == 0 {
		panic("admission controller invariants violated: double release")
	}
	ac.admitted--
	if ac.admitted >= ac.allowed {
		panic("admission controller invariants violated: too many outstanding tickets")
	}
	ac.possiblyReleaseOneFromQueue()
	ac.checkInvariants()
}

// call while holding ac.mtx
func (ac *admissionControllerImpl) possiblyReleaseOneFromQueue() {
	ac.checkInvariants()
	if ac.head == ac.tail {
		return
	}
	var w *waiter
	switch ac.mode {
	case fifoQueueMode:
		if ac.waiters[ac.head] == nil {
			panic("admission controller invariants violated: nil at head")
		}
		w = ac.waiters[ac.head]
	case lifoQueueMode:
		idx := modularDec(ac.tail, len(ac.waiters))
		if ac.waiters[idx] == nil {
			panic("admission controller invariants violated: nil at tail")
		}
		w = ac.waiters[idx]
	}
	if w == nil {
		panic("admission controller invariants violated: unhandled queue mode")
	}
	ac.removeWaiterNoMtx(w)
	ac.admitted++
	close(w.wake)
	ac.checkInvariants()
}

func (ac *admissionControllerImpl) Admitted() uint64 {
	ac.mtx.Lock()
	defer ac.mtx.Unlock()
	return ac.admitted
}

func (ac *admissionControllerImpl) Capacity() uint64 {
	ac.mtx.Lock()
	defer ac.mtx.Unlock()
	return ac.allowed
}

func (ac *admissionControllerImpl) Resize(tickets uint64) uint64 {
	ac.mtx.Lock()
	defer ac.mtx.Unlock()
	return ac.allowed
}

// Do nothing; nothing to stop
func (ac *admissionControllerImpl) Stop() {
	ac.mtx.Lock()
	ac.stopped = true
	ac.mtx.Unlock()
}

// call while holding ac.mtx
func (ac *admissionControllerImpl) resizeWaiters() {
	ac.checkInvariants()
	if modularInc(ac.tail, len(ac.waiters)) != ac.head {
		panic("admission controller invariants violated: resize when not full")
	}
	sz := uint64(len(ac.waiters))
	newWaiters := make([]*waiter, sz*2)
	newIdx := uint64(0)
	for i := uint64(0); i < sz; i++ {
		idx := (ac.head + i) % sz
		if idx == ac.tail {
			break
		}
		if ac.waiters[idx] != nil {
			w := ac.waiters[idx]
			newWaiters[newIdx] = w
			ac.waiters[idx] = nil
			w.index = newIdx
			newIdx++
		}
	}
	ac.waiters = newWaiters
	ac.head = 0
	ac.tail = newIdx
	ac.checkInvariants()
}

// call while holding ac.mtx
func (ac *admissionControllerImpl) adjustQueueMode(now time.Time) {
	ac.checkInvariants()
	if ac.head == ac.tail {
		ac.lastEmpty = now
		ac.mode = fifoQueueMode
	} else if ac.mode == fifoQueueMode && ac.lastEmpty.Add(ac.N).Before(now) {
		ac.mode = lifoQueueMode
	}
	ac.checkInvariants()
}

// call while holding ac.mtx
func (ac *admissionControllerImpl) stripHeadNils() {
	for ac.head != ac.tail && ac.waiters[ac.head] == nil {
		ac.head = modularInc(ac.head, len(ac.waiters))
	}
}

// call while holding ac.mtx
func (ac *admissionControllerImpl) stripTailNils() {
	for ac.head != ac.tail {
		idx := modularDec(ac.tail, len(ac.waiters))
		if ac.waiters[idx] != nil {
			break
		}
		ac.tail = idx
	}
}

// call while holding ac.mtx
func (ac *admissionControllerImpl) checkInvariants() {
	if !checkDebugInvariants {
		return
	}
	for idx, w := range ac.waiters {
		if w == nil {
			continue
		}
		if w.index != uint64(idx) {
			panic("admission controller invariants violated: waiter at wrong index")
		}
	}
	if ac.head != ac.tail {
		if ac.waiters[ac.head] == nil {
			panic("admission controller invariants violated: nil head")
		}
		sz := uint64(len(ac.waiters))
		tailIdx := (ac.tail + sz - 1) % sz
		if ac.waiters[tailIdx] == nil {
			panic("admission controller invariants violated: nil tail")
		}
	}
}

func (ac *admissionControllerImpl) assertIdle() {
	ac.mtx.Lock()
	defer ac.mtx.Unlock()
	ac.checkInvariants()
	if ac.admitted != 0 {
		panic(errors.Errorf("admission controler leaked %d tickets when at idle", ac.admitted))
	}
	if ac.mode != fifoQueueMode {
		panic(errors.Errorf("admission controller queue not in FIFO mode at idle"))
	}
	if ac.head != ac.tail {
		panic(errors.Errorf("admission controller queue not empty at idle"))
	}
	for _, w := range ac.waiters {
		if w != nil {
			panic(errors.Errorf("admission controller has waiter in queue when at idle"))
		}
	}
	ac.checkInvariants()
}
