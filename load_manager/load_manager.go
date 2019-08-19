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

// Package load_manager combines admission_control and scorecard and provides fine control over resources.
package load_manager

import (
	"context"

	ac "github.com/dropbox/load_management/admission_control"
	"github.com/dropbox/load_management/scorecard"
)

const suspiciousQueueName = "suspicious"

const hardReject = uint(0)

// A LoadManager uses AdmissionControl and Scorecard to do traffic shaping.
//
// Specifically, it sets the following limits:
// 1. Hard concurrency limit + queueing using admission control. This can have multiple tiers as
// identified by a string constant.
// 2. Per-attribute (or combination of attributes) limits with a Scorecard
// 3. Canary Scorecard to track violations of new rules to try
// 4. Optional "slow path"/suspicious admission control for those clients that violate a scorecard
// limit.
type LoadManager struct {
	Scorecard       scorecard.Scorecard
	CanaryScorecard scorecard.Scorecard

	// Tags to include for every request (e.g. server host, tclass)
	baseTags []scorecard.Tag

	// Requests go through one of the main queues and then optionally converge through the slow
	// queue.
	mainQueues      map[string]ac.AdmissionController
	suspiciousQueue ac.AdmissionController
}

// NewLoadManager returns LoadManager with queues, scorecards and base tags.
// N.B. Default tags are applied on every request. Therefore, every call to GetResourceXXX for
// resources within a single logical operation (e.g. transaction or session) will double count the
// base tags. Thus, in general, you should only use defaultTags to match conjunctive rules (e.g.
// source_host:XXX;query_hash:YYY) rather than bare rules.
func NewLoadManager(
	mainQueues map[string]ac.AdmissionController,
	suspiciousQueue ac.AdmissionController,
	primaryScorecard scorecard.Scorecard,
	canaryScorecard scorecard.Scorecard, // optional
	baseTags []scorecard.Tag) *LoadManager {

	if canaryScorecard == nil {
		canaryScorecard = scorecard.NewScorecard(scorecard.NoRules())
	}
	return &LoadManager{
		mainQueues:      mainQueues,
		suspiciousQueue: suspiciousQueue,
		Scorecard:       primaryScorecard,
		CanaryScorecard: canaryScorecard,
		baseTags:        baseTags,
	}
}

// GetResource acquires resource from specified queue for request with provided tags.
// If scorecard will be violated method will try to admit it to "global" suspicious queue.
func (l *LoadManager) GetResource(
	ctx context.Context,
	queueName string,
	tags []scorecard.Tag) *Resource {

	resource := l.getResourceInternal(ctx, queueName, tags)

	// Fast path. GetResource succeeded, so return success.
	if resource.Acquired() {
		return resource
	}

	// Fast path. GetResource failed due to Admission Control or some other fatal error.
	if resource.QueueInfo.Ticket == nil {
		return resource
	}

        // If a rule violated has a capacity of 0 then we simply reject the request
        // without going through the suspicious queue.
        if resource.Suspicious() && resource.TrackingInfo.Violated.Capacity == hardReject {
                return resource
        }

	// Slow path. Scorecard violation. Maybe get a suspicious ticket.
	ticket := l.suspiciousQueue.AdmitOne()
	resource.QueueInfo = QueueInfo{
		Name:     suspiciousQueueName,
		Capacity: l.suspiciousQueue.Capacity(),
		Ticket:   ticket,
	}
	// If successful, mark the resource as acquired.
	if ticket != nil {
		resource.acquired = true
	}
	return resource
}

// GetResourceStrict is same as GetResource but it doesn't use suspicious queue.
func (l *LoadManager) GetResourceStrict(
	ctx context.Context,
	queueName string,
	tags []scorecard.Tag) *Resource {

	return l.getResourceInternal(ctx, queueName, tags)
}

// The overall GetResource flow is as follows:
// 1. Acquire a ticket from admission control
// -> If fails, go away.
// 2. Check scorecard limit
// -> If fails, release admission control ticket and try to pass through slow path
// 3. Check canary scorecard limit
// -> If fails, log and move on.
// 4. Return success.
//
// N.B. Callers should introspect resource to determine relevant further action.  e.g. GetResource()
// might want to try the suspiciousQueue but only if the initial ticketing succeeded and the failure
// was due to a scorecard violation.
func (l *LoadManager) getResourceInternal(
	ctx context.Context,
	queueName string,
	tags []scorecard.Tag) *Resource {

	resource := &Resource{}

	mainQueue, ok := l.mainQueues[queueName]
	if !ok {
		return resource
	}

	ticket := mainQueue.AdmitOne()
	resource.QueueInfo = QueueInfo{
		Name:     queueName,
		Capacity: mainQueue.Capacity(),
		Ticket:   ticket,
	}
	if ticket == nil {
		return resource
	}

	// Add in default tags. Allocate a new slice so as not to write into the backing array for tags
	combinedTags := make([]scorecard.Tag, 0, len(tags)+len(l.baseTags))
	combinedTags = append(combinedTags, tags...)
	combinedTags = append(combinedTags, l.baseTags...)

	resource.TrackingInfo = l.Scorecard.TrackRequest(combinedTags)
	if !resource.TrackingInfo.Tracked {
		// We failed scorecard tracking, so release mainQueue ticket.
		resource.QueueInfo.Ticket.Release()
		return resource
	}

	// At this point, we have a valid mainQueue ticket and scorecard tracking info.
	resource.acquired = true

	// See if we violate any canary rules.
	resource.CanaryTrackingInfo = l.CanaryScorecard.TrackRequest(tags)

	return resource
}

// Stop prevents future resource acquisitions.
func (l *LoadManager) Stop() {
	for _, ac := range l.mainQueues {
		ac.Stop()
	}
	l.suspiciousQueue.Stop()
}

// QueueInfo describes the properties of the queue in which a GetResource request waited and the
// ticket if acquisition was successful.
type QueueInfo struct {
	Name     string
	Capacity uint64
	Ticket   *ac.Ticket
}

// A Resource is returned by the load manager on calls to GetResource(). It has Acquired() == true
// if the resource acquisition was successful, false otherwise. Its QueueInfo and TrackingInfo can
// be introspected for instrumentation, but Acquired() provides the only necessary signal for
// control flow.
type Resource struct {
	// acquired describes whether the affiliated resources are being held.
	acquired bool

	// Metadata about the ticket acquisition. Will be populated in both success and failure cases,
	// as long as the queue was valid, and will contain the acquired ticket on success. Can be used
	// for instrumentation.
	QueueInfo QueueInfo

	// Scorecard and Canary scorecard. They can be inspected to generate error messages or perform
	// other instrumentation.
	TrackingInfo       *scorecard.TrackingInfo
	CanaryTrackingInfo *scorecard.TrackingInfo
}

// Release releases resource back to load manager which returned this Resource.
func (r *Resource) Release() {
	if r.acquired {
		if r.QueueInfo.Ticket != nil {
			r.QueueInfo.Ticket.Release()
		}
		if r.TrackingInfo != nil && r.TrackingInfo.Tracked {
			r.TrackingInfo.Untrack()
		}
		if r.CanaryTrackingInfo != nil && r.CanaryTrackingInfo.Tracked {
			r.CanaryTrackingInfo.Untrack()
		}
		r.acquired = false
	}
}

// Suspicious returns whether the resource was marked as suspicious.
func (r *Resource) Suspicious() bool {
	return r.TrackingInfo != nil && !r.TrackingInfo.Tracked
}

// CanarySuspicious returns whether the resource was marked as suspicious by canary.
func (r *Resource) CanarySuspicious() bool {
	return r.CanaryTrackingInfo != nil && !r.CanaryTrackingInfo.Tracked
}

// Acquired returns whether the resource was successfully acquired (and has not yet been released).
func (r *Resource) Acquired() bool {
	return r.acquired
}

// NewUntrackedResource returns a resource that is untracked but is acquired. This is meant to be
// used to simplify code flows when sometimes a client doesn't wish to track a request.
func NewUntrackedResource() *Resource {
	return &Resource{
		acquired: true,
	}
}
