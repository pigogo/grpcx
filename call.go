// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"golang.org/x/net/context"
	"time"
)

// CallContext is the async rpc request info
type CallContext struct {
	us *unarySession
	tm time.Time
}

// Done indicate the request is finish
func (cc *CallContext) Done() <-chan struct{} {
	return cc.us.finishNotify()
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
	us, err := newUnarySession(ctx, cc, method, opts...)
	if err != nil {
		return err
	}

	return us.Run(args, reply)
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

// SendRaw raw oneway request
func (cc *ClientConn) SendRaw(ctx context.Context, msg []byte) error {
	raw, err := newRawSession(ctx, cc)
	if err != nil {
		return err
	}

	return raw.Run(msg)
}
