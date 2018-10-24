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
	"math"
)

// This is an AdmissionController that admits everything.  The primary reason to use this is either
// during testing or to figure out what current usage looks like by using a
// MonitoredAdmissionController. New services should probably use a real admission controller from
// the get-go so they can adjust the limit during testing and development. However, for existing
// services it might be useful to establish a baseline.
func NewPassthroughAdmissionControl() AdmissionController {
	return &passthroughAdmissionControl{}
}

type passthroughAdmissionControl struct {
}

func (p *passthroughAdmissionControl) AdmitOne() *Ticket {
	return &Ticket{
		IssuedBy: p,
	}
}

func (p *passthroughAdmissionControl) Release(t *Ticket) {
	return
}

func (p *passthroughAdmissionControl) Admitted() uint64 {
	return math.MaxUint64
}

func (p *passthroughAdmissionControl) Capacity() uint64 {
	return math.MaxUint64
}

func (p *passthroughAdmissionControl) Resize(tickets uint64) uint64 {
	// The contract of the interface is to return tickets on success.
	return tickets
}

func (p *passthroughAdmissionControl) Stop() {
	return
}
