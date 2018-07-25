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
 *	DialContext has been modified by Kun Zhong
 */

package grpcx

import (
	"time"

	"github.com/pigogo/grpcx/codec"
	"golang.org/x/net/context"
)

var (
	defaultDialOpts = []DialOption{
		WithTimeout(time.Second * 3),
		WithKeepAlive(time.Minute * 3),
		WithCodec(codec.ProtoCodec{}),
		WithMaxMsgSize(defaultClientMaxReceiveMessageSize),
		WithReaderWindowSize(defaultWindowSize),
	}
)

// Dial creates a client connection to the given target.
func Dial(target string, opts ...DialOption) (*ClientConn, error) {
	return DialContext(context.Background(), target, opts...)
}

// DialContext creates a client connection to the given target. ctx can be used to
// cancel or expire the pending connection. Once this function returns, the
// cancellation and expiration of ctx will be noop. Users should call ClientConn.Close
// to terminate all the pending operations after this function returns.
func DialContext(ctx context.Context, target string, opts ...DialOption) (conn *ClientConn, err error) {
	cc := &ClientConn{
		target: target,
		csMgr:  &ConnectivityStateManager{},
		conns:  make(map[string]*connDial),
	}
	cc.csEvltr = &ConnectivityStateEvaluator{CsMgr: cc.csMgr}
	cc.ctx, cc.cancel = context.WithCancel(context.Background())
	opts = append(defaultDialOpts, opts...)
	for _, opt := range opts {
		opt(&cc.dopts)
	}

	defer func() {
		select {
		case <-ctx.Done():
			conn, err = nil, ctx.Err()
		default:
		}

		if err != nil {
			cc.Close()
		}
	}()

	scSet := false
	if cc.dopts.scChan != nil {
		// Try to get an initial service config.
		select {
		case sc, ok := <-cc.dopts.scChan:
			if ok {
				cc.sc = sc
				scSet = true
			}
		default:
		}
	}

	waitC := make(chan error, 1)
	go func() {
		defer close(waitC)
		if cc.dopts.balancer != nil {
			config := BalancerConfig{}
			if err := cc.dopts.balancer.Start(target, config); err != nil {
				waitC <- err
				return
			}
			ch := cc.dopts.balancer.Notify()
			if ch != nil {
				if cc.dopts.block {
					doneChan := make(chan struct{})
					go cc.lbWatcher(doneChan)
					<-doneChan
				} else {
					go cc.lbWatcher(nil)
				}
				return
			}
		}
		// No balancer, or no resolver within the balancer.  Connect directly.
		if err := cc.resetAddrConn(target, cc.dopts.block); err != nil {
			waitC <- err
			return
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-waitC:
		if err != nil {
			return nil, err
		}
	}

	if cc.dopts.scChan != nil && !scSet {
		// Blocking wait for the initial service config.
		select {
		case sc, ok := <-cc.dopts.scChan:
			if ok {
				cc.sc = sc
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if cc.dopts.scChan != nil {
		go cc.scWatcher()
	}

	return cc, nil
}
