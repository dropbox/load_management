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
	"sync"
)

// The most difficult part of tracking a scorecard that can change is gracefully
// handling the rollover from one version to the next.  There are two options:
// 1.  Immediately register transactions in the new only scorecard and wait for
//     the old scorecard to drain
// 2.  Double register transactions in both scorecards.  Once all active
//     transactions are in both, stop using the old one.
// The first strategy is akin to permitting the union of the two scorecards; for
// a minor tweak, this could double the amount of traffic in the system.  The
// second strategy takes the intersection of the two scorecards, ensuring that
// traffic never exceeds the least restrictive limits between the two
// scorecards.  It can exceed the most restrictive rules only for requests that
// entered the system prior to any requests being registered under the new
// rules.  Both states are transient and should last for only as long as the
// longest lived request in the system.
//
// Because scorecard is designed to protect the system against load, it seems
// ill-advised to require people to consider behavior of a push that allows load
// to exceed stated limits.  For that reason, this implementation uses the
// second approach (which is harder to reason about; read on).

// To simplify implementation, only two scorecards are ever allowed to coexist:
// one being phased out and the other being phased in.  When there is only a
// single scorecard, it is referred to as `active`.  If there are two, the newer
// scorecard is `active` while the other is `decomm` (short for decommisioning).
//
// The trickiest part of this whole implementation is deciding the scorecard state changes.  If
// there is a single scorecard in use, a new scorecard pushed can be installed as `active` and the
// existing scorecard demoted to `decomm` status.  The system can stop using the `decomm` scorecard
// when all executing requests are tracked by the new (`active`) scorecard.
//
// To enable this bookkeeping, each scorecard is associated with two reference
// counts, one for each state in which new requests can be tracked by the
// scorecard.  When the scorecard is `active` (resp `decomm`), the `active`
// (`decomm`) refcount goes up.  When requests complete, they decrement the
// reference counter they first incremented.  This ensures that once a scorecard
// transitions to `decomm` state, the `active` refcount will no longer increase.
// When it reaches zero, the only requests still tracked by the scorecard will
// be reflected in the `decomm` refcount; these requests will be tracked in the
// `active` refcount by some other scorecard.  At that point it is safe to stop
// dobule-registering requests and forget about the `decomm` scorecard (leaving
// the requests still tracked by it to decrement and untrack before eventual
// garbage collection claims it entirely).

type dynamicScorecard struct {
	mtx *sync.Mutex
	cnd *sync.Cond
	// below synchronized by mtx
	active *refcountScorecard
	decomm *refcountScorecard
}

type refcountScorecard struct {
	sc Scorecard
	// refcounts
	activeRefCount uint64
	decommRefCount uint64
}

type trackedRequestState struct {
	d                  *dynamicScorecard
	active             *refcountScorecard
	activeRefPtr       *uint64
	activeTrackingInfo *TrackingInfo
	decomm             *refcountScorecard
	decommRefPtr       *uint64
	decommTrackingInfo *TrackingInfo
}

func (state *trackedRequestState) cleanup() {
	state.d.decref(state)
	// Intentionally don't lock the below because scorecards are internally
	// synchronized and locking would just add contention.
	if state.activeTrackingInfo != nil && state.activeTrackingInfo.Tracked {
		state.activeTrackingInfo.Untrack()
	}
	if state.decommTrackingInfo != nil && state.decommTrackingInfo.Tracked {
		state.decommTrackingInfo.Untrack()
	}
}

func (d *dynamicScorecard) grabNoRefCount() (*refcountScorecard, *refcountScorecard) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	active := d.active
	decomm := d.decomm
	return active, decomm
}

func (d *dynamicScorecard) incref(state *trackedRequestState) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	state.d = d
	state.active = d.active
	state.decomm = d.decomm
	state.active.activeRefCount++
	state.activeRefPtr = &state.active.activeRefCount
	if state.decomm != nil {
		state.decomm.decommRefCount++
		state.decommRefPtr = &state.decomm.decommRefCount
	}
}

func (d *dynamicScorecard) decref(state *trackedRequestState) {
	state.d.mtx.Lock()
	defer state.d.mtx.Unlock()
	// decrement ref counts that we incremented earlier
	if state.activeRefPtr != nil {
		*state.activeRefPtr--
	}
	if state.decommRefPtr != nil {
		*state.decommRefPtr--
	}
	// If the `decomm` scorecard pointed to by the multiplexer has no active
	// requests, switch it out and signal that we have a single scorecard.
	if state.d.decomm != nil && state.d.decomm.activeRefCount == 0 {
		state.d.decomm = nil
		state.d.cnd.Signal()
	}
}

// Returns rules for only the newest scorecard in use
func (d *dynamicScorecard) Rules() []Rule {
	active, _ := d.grabNoRefCount()
	return active.sc.Rules()
}

func (d *dynamicScorecard) TrackRequest(tags []Tag) *TrackingInfo {
	state := &trackedRequestState{}
	d.incref(state)
	// Track in the active scorecard
	state.activeTrackingInfo = state.active.sc.TrackRequest(tags)
	if !state.activeTrackingInfo.Tracked {
		defer state.cleanup()
		return &TrackingInfo{
			Tracked:  false,
			Violated: state.activeTrackingInfo.Violated,
			Value:    state.activeTrackingInfo.Value,
			Tag:      state.activeTrackingInfo.Tag,
		}
	}
	if state.decomm != nil {
		state.decommTrackingInfo = state.decomm.sc.TrackRequest(tags)
		if !state.decommTrackingInfo.Tracked {
			defer state.cleanup()
			return &TrackingInfo{
				Tracked:  false,
				Violated: state.decommTrackingInfo.Violated,
				Value:    state.decommTrackingInfo.Value,
				Tag:      state.decommTrackingInfo.Tag,
			}
		}
	}
	return &TrackingInfo{
		Tracked:     true,
		trackedTags: []Tag{},
		scorecard:   d,
		callback: func(s Scorecard, tt []Tag) {
			state.cleanup()
		},
	}
}

func (d *dynamicScorecard) Inspect() map[Tag]uint {
	active, decomm := d.grabNoRefCount()
	inspect := active.sc.Inspect()
	if decomm == nil {
		return inspect
	}
	// Take the max of either scorecard.  Both values bound number of active
	// requests (assuming atomicity) and so the max provides a good approx.
	// Cannot add them because you would have things double counted.
	//
	// The scorecard interface makes guarantees of atomicity of Inspect only
	// when external synchronization stops track/untrack.
	for tag, count := range decomm.sc.Inspect() {
		currentCount, ok := inspect[tag]
		if !ok || count > currentCount {
			inspect[tag] = count
		}
	}
	return inspect
}

func (d *dynamicScorecard) Reconfigure(rules []Rule) {
	newRefcountScorecard := &refcountScorecard{
		sc: NewScorecard(rules),
	}
	d.swizzel(newRefcountScorecard)
}

func (d *dynamicScorecard) swizzel(sc *refcountScorecard) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	for d.decomm != nil {
		d.cnd.Wait()
	}
	d.decomm = d.active
	d.active = sc
	// if we can drop the decomm right away, do so
	if d.decomm.activeRefCount == 0 {
		d.decomm = nil
	}
}

// used in testing:
// aa = active.active
// ad = active.decomm
// da = decomm.active
// dd = decomm.decomm
func (d *dynamicScorecard) peek() (bool, bool, uint64, uint64, uint64, uint64) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	hasActive := d.active != nil
	hasDecomm := false
	aa := d.active.activeRefCount
	ad := d.active.decommRefCount
	da := uint64(0)
	dd := uint64(0)
	if d.decomm != nil {
		hasDecomm = true
		da = d.decomm.activeRefCount
		dd = d.decomm.decommRefCount
	}
	return hasActive, hasDecomm, aa, ad, da, dd
}

func newDynamicScorecard(rules []Rule) DynamicScorecard {
	sc := &dynamicScorecard{}
	sc.mtx = &sync.Mutex{}
	sc.cnd = sync.NewCond(sc.mtx)
	sc.active = &refcountScorecard{
		sc: NewScorecard(rules),
	}
	return sc
}
