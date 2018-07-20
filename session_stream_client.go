// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/pigogo/grpcx/codec"
	"golang.org/x/net/context"
	xlog "google.golang.org/grpc/grpclog"
)

// NewClientStream creates a new Stream for the client side. This is called
// by generated code000000000000000000000.0
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

	gopts := BalancerGetOptions{
		BlockingWait: !c.failFast,
	}
	for {
		conn, put, err = cc.getConn(ctx, gopts)
		if err != nil {
			if err == errConnClosing || err == errConnUnavailable {
				if c.failFast {
					return nil, err
				}
				continue
			}
			// All the other errors are treated as Internal errors.
			return
		}

		break
	}

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
		xlog.Errorf("grpcx: newClientStream save session fail:%v", err)
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

	mu         sync.Mutex
	put        func()
	sendClosed int32
	finished   bool
	method     string
	once       sync.Once
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

	if atomic.LoadInt32(&cs.sendClosed) > 0 {
		return io.EOF
	}

	head := &PackHeader{
		Ptype:     PackType_SREQ,
		Sessionid: cs.sessionid,
		Methord:   cs.method,
	}

	return cs.send(head, m)
}

func (cs *clientStream) send(head *PackHeader, m interface{}) (err error) {
	return cs.conn.send(head, m)
}

func (cs *clientStream) RecvMsg(m interface{}) (err error) {
	defer func() {
		if err != nil {
			cs.finish()
			if err != io.EOF {
				xlog.Errorf("grpcx: clientStream RecvMsg fail:%v", err)
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
			err = cs.ctx.Err()
			break
		default:
			return cs.ctx.Err()
		}
	}

	if msg.head.Ptype == PackType_EOF {
		return io.EOF
	} else if msg.head.Ptype == PackType_ERROR {
		return fmt.Errorf("grpcx: get error from server")
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
			err = cs.ctx.Err()
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

	if atomic.AddInt32(&cs.sendClosed, 1) > 1 {
		//warming: already closed
		return nil
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
		err := fmt.Errorf("grpcx clientStream::onMessage unknow error:conn mix up")
		xlog.Error(err)
		return err
	}

	cs.buf.put(&netPack{
		head: head,
		body: body,
	})

	if head.Ptype == PackType_EOF || head.Ptype == PackType_ERROR {
		cs.finish()
	}
	return nil
}
