// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"fmt"
	"io"
	"sync"

	"github.com/pigogo/grpcx/codec"
	xlog "github.com/pigogo/grpcx/grpclog"
	"golang.org/x/net/context"
)

// NewStream creates a new Stream for the client side. This is typically
// called by generated code. ctx is used for the lifetime of the stream.
//
// To ensure resources are not leaked due to the stream returned, one of the following
// actions must be performed:
//
//      1. Call Close on the ClientConn.
//      2. Cancel the context provided.
//      3. Call RecvMsg until a non-nil error is returned. A protobuf-generated
//         client-streaming RPC, for instance, might use the helper function
//         CloseAndRecv (note that CloseSend does not Recv, therefore is not
//         guaranteed to release all resources).
//      4. Receive a non-nil, non-io.EOF error from Header or SendMsg.
//
// If none of the above happen, a goroutine and a context will be leaked, and grpc
// will not call the optionally-configured stats handler with a stats.End message.
func (cc *ClientConn) NewStream(ctx context.Context, desc *StreamDesc, method string, opts ...CallOption) (ClientStream, error) {
	// allow interceptor to see all applicable call options, which means those
	// configured as defaults from dial option as well as per-call options
	opts = combine(cc.dopts.callOptions, opts)

	/*if cc.dopts.streamInt != nil {
		return cc.dopts.streamInt(ctx, desc, cc, method, newClientStream, opts...)
	}*/
	return newClientStream(ctx, desc, cc, method, opts...)
}

// NewClientStream creates a new Stream for the client side. This is called
func NewClientStream(ctx context.Context, desc *StreamDesc, cc *ClientConn, method string, opts ...CallOption) (_ ClientStream, err error) {
	return newClientStream(ctx, desc, cc, method, opts...)
}

func newClientStream(ctx context.Context, desc *StreamDesc, cc *ClientConn, method string, opts ...CallOption) (_ ClientStream, err error) {
	var (
		conn   *connDial
		put    func()
		cancel context.CancelFunc
		sctx   context.Context
	)
	c := defaultCallInfo
	mc := cc.GetMethodConfig(method)
	if mc.WaitForReady != nil {
		c.failFast = !*mc.WaitForReady
	}

	if mc.Timeout != nil {
		sctx, cancel = context.WithTimeout(ctx, *mc.Timeout)
	} else {
		sctx, cancel = context.WithCancel(ctx)
	}

	opts = append(cc.dopts.callOptions, opts...)
	for _, o := range opts {
		if err := o.before(&c); err != nil {
			return nil, err
		}
	}
	c.maxSendMessageSize = getMaxSize(mc.MaxReqSize, c.maxSendMessageSize, defaultClientMaxSendMessageSize)
	c.maxReceiveMessageSize = getMaxSize(mc.MaxRespSize, c.maxReceiveMessageSize, defaultClientMaxReceiveMessageSize)
	hkey := uint32(0)
	if c.hbKey != nil {
		hkey = *c.hbKey
	}
	gopts := BalancerGetOptions{
		BlockingWait: !c.failFast,
		HashKey:      hkey,
	}

	conn, put, err = cc.getConn(ctx, gopts)
	cs := &clientStream{
		opts:   opts,
		c:      c,
		desc:   desc,
		codec:  cc.dopts.codec,
		cancel: cancel,
		ctx:    sctx,
		buf:    newRecvBuffer(),

		put:       put,
		conn:      conn,
		method:    method,
		sessionid: cc.genStreamID(),
	}
	cs.connCancel, err = conn.withSession(cs.sessionid, cs)
	if err != nil {
		err = fmt.Errorf("grpcx: newClientStream save session fail:%v", err)
		xlog.Error(err)
		return nil, err
	}

	if err = cs.init(); err != nil {
		xlog.Errorf("grpcx: clientStream init fail:%v", err)
		cs.connCancel()
		return nil, err
	}

	// Listen on ctx.Done() to detect cancellation and s.Done() to detect normal termination
	// when there is no pending I/O operations on this stream.
	go func() {
		select {
		case <-conn.Error():
			break
		case <-cs.ctx.Done():
			break
		}
		cs.finish()
	}()
	return cs, nil
}

// clientStream implements a client side Stream.
type clientStream struct {
	ClientStream

	buf        *recvBuffer
	opts       []CallOption
	c          callInfo
	conn       *connDial
	desc       *StreamDesc
	codec      codec.Codec
	cancel     context.CancelFunc
	ctx        context.Context
	sessionid  int64
	connCancel func()
	err        error

	mu       sync.Mutex
	put      func()
	finished bool
	method   string
}

func (cs *clientStream) init() error {
	return cs.send(&PackHeader{
		Ptype:     PackType_SINI,
		Sessionid: cs.sessionid,
		Methord:   cs.method,
	}, nil)
}

func (cs *clientStream) Context() context.Context {
	return cs.ctx
}

func (cs *clientStream) streamID() int64 {
	return cs.sessionid
}

func (cs *clientStream) SendMsg(m interface{}) (err error) {
	defer func() {
		if err != nil {
			cs.finish()
		}

		if err == nil {
			return
		}

		if err == io.EOF {
			if !cs.desc.ClientStreams && cs.desc.ServerStreams {
				err = nil
			}
			return
		}

		xlog.Errorf("grpcx: clientStream SendMsg fail:%v", err)
	}()

	if cs.err != nil {
		return cs.err
	}

	head := &PackHeader{
		Ptype:     PackType_SREQ,
		Sessionid: cs.sessionid,
		Methord:   cs.method,
	}

	return cs.send(head, m)
}

func (cs *clientStream) send(head *PackHeader, m interface{}) (err error) {
	return cs.conn.send(head, m, *cs.c.maxSendMessageSize)
}

func (cs *clientStream) RecvMsg(m interface{}) (err error) {
	defer func() {
		if err != nil {
			if err != io.EOF {
				xlog.Errorf("grpcx: clientStream RecvMsg fail:%v sessionid:%v", err, cs.sessionid)
				cs.finish()
			}
		}
	}()

	var msg *netPack
	select {
	case msg = <-cs.buf.get():
		cs.buf.load()
		break
	case <-cs.ctx.Done():
		select {
		case msg = <-cs.buf.get():
			cs.buf.load()
			break
		default:
			err = cs.err
			if err == nil {
				return cs.ctx.Err()
			}
			return
		}
	}

	if msg.head.Ptype == PackType_EOF {
		return io.EOF
	} else if msg.head.Ptype == PackType_ERROR || msg.head.Ptype == PackType_GoAway {
		return cs.err
	}

	err = cs.codec.Unmarshal(msg.body, m)
	if err == nil {
		if !cs.desc.ClientStreams || cs.desc.ServerStreams {
			return
		}

		select {
		case msg = <-cs.buf.get():
			cs.buf.load()
			break
		case <-cs.conn.Error():
			err = fmt.Errorf("grpcx: connection error")
			break
		case <-cs.ctx.Done():
			err = cs.err
			if err == nil {
				err = cs.ctx.Err()
			}
		}

		if err != nil || msg.head.Ptype != PackType_EOF {
			return fmt.Errorf("grpcx: client streaming protocol violation: err get:%v want <EOF>", err)
		}

		cs.finish()
		return nil
	}

	return err
}

func (cs *clientStream) CloseSend() (err error) {
	defer func() {
		if err != nil {
			cs.finish()
		}
	}()

	if cs.err != nil {
		return cs.err
	}

	head := &PackHeader{
		Ptype:     PackType_EOF,
		Sessionid: cs.sessionid,
	}

	return cs.send(head, nil)
}

func (cs *clientStream) finish() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.finished {
		return
	}
	cs.finished = true

	for _, o := range cs.opts {
		o.after(&cs.c)
	}

	if cs.cancel != nil {
		cs.cancel()
		cs.cancel = nil
	}

	if cs.connCancel != nil {
		cs.connCancel()
		cs.connCancel = nil
	}

	if cs.put != nil {
		cs.put()
		cs.put = nil
	}
}

func (cs *clientStream) onMessage(conn Conn, head *PackHeader, body []byte) error {
	if cs.conn != conn {
		cs.err = fmt.Errorf("grpcx clientStream::onMessage unknow error:conn mix up")
		xlog.Error(cs.err)

		cs.buf.put(&netPack{
			head: head,
			body: []byte(cs.err.Error()),
		})
		return cs.err
	} else if head.Ptype == PackType_ERROR {
		cs.err = fmt.Errorf("grpcx: receive srv error")
	} else if head.Ptype == PackType_GoAway {
		cs.err = errSrvGoaway
	}

	cs.buf.put(&netPack{
		head: head,
		body: body,
	})

	if head.Ptype != PackType_SRSP {
		cs.finish()
	}
	return nil
}
