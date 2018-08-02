/*
 *
 * Copyright 2016 gRPC authors.
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
 *	callInfo has been modified by Kun Zhong
 */

package grpcx

import (
	"hash/crc32"
	"unsafe"
)

// callInfo contains all related configuration and information about an RPC.
type callInfo struct {
	failFast              bool
	token                 *int64
	hbKey                 *uint32
	maxReceiveMessageSize *int
	maxSendMessageSize    *int
}

const (
	defaultReceiveMessageSize = 4 * 1024 * 1024
	defaultSendMessageSize    = 4 * 1024 * 1024
)

var defaultCallInfo = callInfo{
	failFast: true,
}

// CallOption configures a Call before it starts or extracts information from
// a Call after it completes.
type CallOption interface {
	// before is called before the call is sent to any server.  If before
	// returns a non-nil error, the RPC fails with that error.
	before(*callInfo) error

	// after is called after the call has completed.  after cannot return an
	// error, so any failures should be reported via output parameters.
	after(*callInfo)
}

// EmptyCallOption does not alter the Call configuration.
// It can be embedded in another structure to carry satellite data for use
// by interceptors.
type EmptyCallOption struct{}

func (EmptyCallOption) before(*callInfo) error { return nil }
func (EmptyCallOption) after(*callInfo)        {}

type beforeCall func(c *callInfo) error

func (o beforeCall) before(c *callInfo) error { return o(c) }
func (o beforeCall) after(c *callInfo)        {}

type afterCall func(c *callInfo)

func (o afterCall) before(c *callInfo) error { return nil }
func (o afterCall) after(c *callInfo)        { o(c) }

// FailFast configures the action to take when an RPC is attempted on broken
// connections or unreachable servers. If failfast is true, the RPC will fail
// immediately. Otherwise, the RPC client will block the call until a
// connection is available (or the call is canceled or times out) and will retry
// the call if it fails due to a transient error. Please refer to
// https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md.
// Note: failFast is default to true.
func FailFast(failFast bool) CallOption {
	return beforeCall(func(c *callInfo) error {
		c.failFast = failFast
		return nil
	})
}

// MaxCallRecvMsgSize returns a CallOption which sets the maximum message size the client can receive.
func MaxCallRecvMsgSize(s int) CallOption {
	return beforeCall(func(o *callInfo) error {
		o.maxReceiveMessageSize = &s
		return nil
	})
}

// MaxCallSendMsgSize returns a CallOption which sets the maximum message size the client can send.
func MaxCallSendMsgSize(s int) CallOption {
	return beforeCall(func(o *callInfo) error {
		o.maxSendMessageSize = &s
		return nil
	})
}

// WithToken return a CallOption which set the service channel hash token
// set the option if and only if the request must be serve in the same channel
// in the server
func WithToken(token int64) CallOption {
	return beforeCall(func(o *callInfo) error {
		o.token = &token
		return nil
	})
}

// WithHBalancerStrKey return a CallOption which set the consistent string hash key
func WithHBalancerStrKey(key string) CallOption {
	hval := uint32(0)
	if n := len(key); n < 64 {
		var slicingUpdate [64]byte
		copy(slicingUpdate[:], []byte(key)[:])
		hval = crc32.ChecksumIEEE(slicingUpdate[:n])
	} else {
		hval = crc32.ChecksumIEEE([]byte(key))
	}

	return beforeCall(func(o *callInfo) error {
		o.hbKey = &hval
		return nil
	})
}

// WithHBalancerKey return a CallOption which set the consistent integer hash key
// the key will be used to cal a crc32 val
func WithHBalancerKey(key int64) CallOption {
	hval := uint32(0)
	if key > 0 {
		var slicingUpdate [64]byte
		copy(slicingUpdate[:], (*[8]byte)(unsafe.Pointer(&key))[:])
		hval = crc32.ChecksumIEEE(slicingUpdate[:8])
	}

	return beforeCall(func(o *callInfo) error {
		o.hbKey = &hval
		return nil
	})
}
