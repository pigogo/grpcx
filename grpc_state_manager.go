/*
 *
 * Copyright 2014 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 *	Modifiaction Statement
 *	ConnectivityStateManager and ConnectivityStateEvaluator has been modified by Kun Zhong
 */

package grpcx

import (
	"fmt"
	"sync"
)

// ConnectivityState indicates the state of a client connection.
type ConnectivityState int

const (
	// Idle indicates the ClientConn is idle.
	Idle ConnectivityState = iota
	// Connecting indicates the ClienConn is connecting.
	Connecting
	// Ready indicates the ClientConn is ready for work.
	Ready
	// TransientFailure indicates the ClientConn has seen a failure but expects to recover.
	TransientFailure
	//Draining indicates graceful close, unaccept new request but wait unfinish request
	Draining
	// Shutdown indicates the ClientConn has started shutting down.
	Shutdown
	// Goaway indicates server is on closing
	Goaway
)

func (s ConnectivityState) String() string {
	switch s {
	case Idle:
		return "IDLE"
	case Connecting:
		return "CONNECTING"
	case Ready:
		return "READY"
	case TransientFailure:
		return "TRANSIENT_FAILURE"
	case Shutdown:
		return "SHUTDOWN"
	case Goaway:
		return "GOAWAY"
	default:
		panic(fmt.Sprintf("unknown connectivity state: %d", s))
	}
}

// ConnectivityStateManager keeps the ConnectivityState of ClientConn.
// This struct will eventually be exported so the balancers can access it.
type ConnectivityStateManager struct {
	mu         sync.Mutex
	state      ConnectivityState
	notifyChan chan struct{}
	cse        *ConnectivityStateEvaluator
}

// UpdateState updates the ConnectivityState of ClientConn.
// If there's a change it notifies goroutines waiting on state change to
// happen.
func (csm *ConnectivityStateManager) UpdateState(state ConnectivityState) bool {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	if csm.state == Shutdown || (csm.state == Goaway && state != Shutdown) {
		return false
	}
	if csm.state == state {
		return false
	}

	if csm.cse != nil {
		csm.cse.recordTransition(csm.state, state)
	}

	csm.state = state
	if csm.notifyChan != nil {
		// There are other goroutines waiting on this channel.
		close(csm.notifyChan)
		csm.notifyChan = nil
	}
	return true
}

func (csm *ConnectivityStateManager) GetState() ConnectivityState {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	return csm.state
}

func (csm *ConnectivityStateManager) GetNotifyChan() <-chan struct{} {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	if csm.notifyChan == nil {
		csm.notifyChan = make(chan struct{})
	}
	return csm.notifyChan
}

// ConnectivityStateEvaluator gets updated by addrConns when their
// states transition, based on which it evaluates the state of
// ClientConn.
// Note: This code will eventually sit in the balancer in the new design.
type ConnectivityStateEvaluator struct {
	CsMgr               *ConnectivityStateManager
	numReady            uint64 // Number of addrConns in ready state.
	numConnecting       uint64 // Number of addrConns in connecting state.
	numTransientFailure uint64 // Number of addrConns in transientFailure.
}

// recordTransition records state change happening in every addrConn and based on
// that it evaluates what state the ClientConn is in.
// It can only transition between Ready, Connecting and TransientFailure. Other states,
// Idle and Shutdown are transitioned into by ClientConn; in the begining of the connection
// before any addrConn is created ClientConn is in idle state. In the end when ClientConn
// closes it is in Shutdown state.
// TODO Note that in later releases, a ClientConn with no activity will be put into an Idle state.
func (cse *ConnectivityStateEvaluator) recordTransition(oldState, newState ConnectivityState) {
	// Update counters.
	for idx, state := range []ConnectivityState{oldState, newState} {
		updateVal := 2*uint64(idx) - 1 // -1 for oldState and +1 for new.
		switch state {
		case Ready:
			cse.numReady += updateVal
		case Connecting:
			cse.numConnecting += updateVal
		case TransientFailure:
			cse.numTransientFailure += updateVal
		}
	}

	// Evaluate.
	if cse.numReady > 0 {
		cse.CsMgr.UpdateState(Ready)
		return
	}
	if cse.numConnecting > 0 {
		cse.CsMgr.UpdateState(Connecting)
		return
	}
	cse.CsMgr.UpdateState(TransientFailure)
}

// state of transport
type transportState int

const (
	reachable transportState = iota
	unreachable
	closing
	draining
)
