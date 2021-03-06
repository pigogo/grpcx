// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"time"

	"golang.org/x/net/context"
)

// CallContext is the async rpc request info
type CallContext struct {
	us *unarySession
	tm time.Time
}

// Done indicate the request is finish
func (cc *CallContext) Done() <-chan struct{} {
	return cc.us.done()
}

// Error return the requestion error
func (cc *CallContext) Error() error {
	return cc.us.err
}

// TimePass is the time cost for the request
func (cc *CallContext) TimePass() time.Duration {
	return time.Now().Sub(cc.tm)
}

// Invoke sends the RPC request on the wire and returns after response is received.
// Invoke is called by generated code. Also users can call Invoke directly when it
// is really needed in their use cases.
func Invoke(ctx context.Context, method string, args, reply interface{}, cc *ClientConn, opts ...CallOption) error {
	return cc.Invoke(ctx, method, args, reply, opts...)
}

// Invoke sends the RPC request on the wire and returns after response is
// received.  This is typically called by generated code.
//
// All errors returned by Invoke are compatible with the status package.
func (cc *ClientConn) Invoke(ctx context.Context, method string, args, reply interface{}, opts ...CallOption) error {
	// allow interceptor to see all applicable call options, which means those
	// configured as defaults from dial option as well as per-call options
	opts = combine(cc.dopts.callOptions, opts)

	/*if cc.dopts.unaryInt != nil {
		return cc.dopts.unaryInt(ctx, method, args, reply, cc, invoke, opts...)
	}*/
	return invoke(ctx, method, args, reply, cc, opts...)
}

func invoke(ctx context.Context, method string, req, reply interface{}, cc *ClientConn, opts ...CallOption) error {
	us, err := newUnarySession(ctx, cc, method, opts...)
	if err != nil {
		return err
	}

	return us.Run(req, reply)
}

// Call same as Invoke
func (cc *ClientConn) Call(ctx context.Context, method string, args, reply interface{}, opts ...CallOption) error {
	us, err := newUnarySession(ctx, cc, method, opts...)
	if err != nil {
		return err
	}

	return us.Run(args, reply)
}

// BackCall request async
func (cc *ClientConn) BackCall(ctx context.Context, method string, args, reply interface{}, opts ...CallOption) (*CallContext, error) {
	us, err := newUnarySession(ctx, cc, method, opts...)
	if err != nil {
		return nil, err
	}

	cctx := &CallContext{
		us: us,
		tm: time.Now(),
	}

	go us.Run(args, reply)
	return cctx, nil
}

// Send oneway request
func (cc *ClientConn) Send(ctx context.Context, method string, args interface{}, opts ...CallOption) error {
	os, err := newOnewaySession(ctx, cc, method, opts...)
	if err != nil {
		return err
	}

	return os.Run(args)
}
