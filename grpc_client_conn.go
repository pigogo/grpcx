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
 *	Modifiaction Statement
 *	ClientConn has been modified by Kun Zhong
 */

package grpcx

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	xlog "google.golang.org/grpc/grpclog"
)

type connAddrKey struct{}

func getConnAddr(ctx context.Context) string {
	val := ctx.Value(connAddrKey{})
	if addr, ok := val.(string); ok {
		return addr
	}
	return ""
}

func withConnAddr(ctx context.Context, addr string) context.Context {
	return context.WithValue(ctx, connAddrKey{}, addr)
}

type waitCloseConn struct {
	deedline time.Time
	conn     *connDial
}

// ClientConn represents a client connection to an RPC server.
type ClientConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	target    string
	authority string
	dopts     dialOptions
	csMgr     *ConnectivityStateManager
	csEvltr   *ConnectivityStateEvaluator // This will eventually be part of balancer.
	sc        ServiceConfig

	mu            sync.RWMutex
	conns         map[string]*connDial
	waitCloseConn map[string]waitCloseConn
	streamIDBase  int64
}

// WaitForStateChange waits until the ConnectivityState of ClientConn changes from sourceState or
// ctx expires. A true value is returned in former case and false in latter.
func (cc *ClientConn) WaitForStateChange(ctx context.Context, sourceState ConnectivityState) bool {
	ch := cc.csMgr.GetNotifyChan()
	if cc.csMgr.GetState() != sourceState {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-ch:
		return true
	}
}

// GetState returns the ConnectivityState of ClientConn.
func (cc *ClientConn) GetState() ConnectivityState {
	return cc.csMgr.GetState()
}

func (cc *ClientConn) genStreamID() int64 {
	return atomic.AddInt64(&cc.streamIDBase, 1)
}

// lbWatcher watches the Notify channel of the balancer in cc and manages
// connections accordingly.  If doneChan is not nil, it is closed after the
// first successfull connection is made.
func (cc *ClientConn) lbWatcher(doneChan chan struct{}) {
	for addrs := range cc.dopts.balancer.Notify() {
		var (
			add []string // Addresses need to setup connections.
			del []Conn   // Address need to drain
		)
		cc.mu.Lock()
		for _, a := range addrs {
			if _, ok := cc.conns[a]; !ok {
				add = append(add, a)
			}
		}
		for k, c := range cc.conns {
			var keep bool
			for _, a := range addrs {
				if k == a {
					keep = true
					break
				}
			}
			if !keep {
				del = append(del, c)
				delete(cc.conns, k)
			}
		}
		cc.mu.Unlock()

		// add new connection
		for _, a := range add {
			var err error
			if doneChan != nil {
				err = cc.resetAddrConn(a, true)
				if err == nil {
					close(doneChan)
					doneChan = nil
				}
			} else {
				err = cc.resetAddrConn(a, false)
			}
			if err != nil {
				xlog.Warningf("Error creating connection to %v. Err: %v", a, err)
			}
		}

		// remove del connection
		var dones []<-chan struct{}
		for _, conn := range del {
			done := conn.gracefulClose()
			dones = append(dones, done)
		}

		ctx, _ := context.WithTimeout(context.Background(), time.Minute)
	loop:
		for _, done := range dones {
			select {
			case <-done:
			case <-ctx.Done():
				// timeout waiting: force all connection close
				for _, conn := range del {
					conn.close()
				}
				break loop
			}
		}
	}
}

// resetAddrConn creates an Conn for addr and adds it to cc.conns.
//
// We should never need to replace an Conn with a new one. This function is only used
// as newAddrConn to create new Conn.
// TODO rename this function and clean up the code.
func (cc *ClientConn) resetAddrConn(addr string, block bool) error {
	var up func() func(error)
	if cc.dopts.balancer != nil {
		up = func() func(error) {
			return cc.dopts.balancer.Up(addr)
		}
	}
	conn, err := newDialConn(cc.ctx, addr, cc.dopts, cc.csEvltr, up)
	if err != nil {
		return err
	}
	cc.mu.Lock()
	cc.conns[addr] = conn
	cc.mu.Unlock()
	return nil
}

func (cc *ClientConn) scWatcher() {
	for {
		select {
		case sc, ok := <-cc.dopts.scChan:
			if !ok {
				return
			}
			cc.mu.Lock()
			// TODO: load balance policy runtime change is ignored.
			// We may revist this decision in the future.
			cc.sc = sc
			cc.mu.Unlock()
		case <-cc.ctx.Done():
			return
		}
	}
}

// GetMethodConfig gets the method config of the input method.
// If there's an exact match for input method (i.e. /service/method), we return
// the corresponding MethodConfig.
// If there isn't an exact match for the input method, we look for the default config
// under the service (i.e /service/). If there is a default MethodConfig for
// the serivce, we return it.
// Otherwise, we return an empty MethodConfig.
func (cc *ClientConn) GetMethodConfig(method string) MethodConfig {
	// TODO: Avoid the locking here.
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	m, ok := cc.sc.Methods[method]
	if !ok {
		i := strings.LastIndex(method, "/")
		m, _ = cc.sc.Methods[method[:i+1]]
	}
	return m
}

func (cc *ClientConn) getConn(ctx context.Context, opts BalancerGetOptions) (*connDial, func(), error) {
	var (
		conn *connDial
		ok   bool
		put  func()
	)
	if cc.dopts.balancer == nil {
		// If balancer is nil, there should be only one addrConn available.
		cc.mu.RLock()
		if cc.conns == nil {
			cc.mu.RUnlock()
			return nil, nil, ErrClientConnClosing
		}
		for _, conn = range cc.conns {
			// Break after the first iteration to get the first addrConn.
			ok = true
			break
		}
		cc.mu.RUnlock()
	} else {
		var (
			addr string
			err  error
		)
		addr, put, err = cc.dopts.balancer.Get(ctx, opts)
		if err != nil {
			return nil, nil, err
		}
		cc.mu.RLock()
		if cc.conns == nil {
			cc.mu.RUnlock()
			return nil, nil, ErrClientConnClosing
		}
		conn, ok = cc.conns[addr]
		cc.mu.RUnlock()
	}
	if !ok {
		if put != nil {
			put()
		}
		return nil, nil, errConnClosing
	}
	err := conn.Wait(ctx, cc.dopts.balancer != nil, !opts.BlockingWait)
	if err != nil {
		if put != nil {
			put()
		}
		return nil, nil, err
	}
	return conn, put, nil
}

// Close tears down the ClientConn and all underlying connections.
func (cc *ClientConn) Close() error {
	if cc.csMgr.UpdateState(Shutdown) == false {
		return ErrClientConnClosing
	}

	cc.mu.Lock()
	cc.cancel()
	if cc.dopts.balancer != nil {
		cc.dopts.balancer.Close()
	}

	for _, conn := range cc.conns {
		conn.close()
	}

	for _, conn := range cc.waitCloseConn {
		conn.conn.close()
	}
	cc.mu.Unlock()

	return nil
}
