// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"fmt"
	"sync"
	"time"

	"github.com/pigogo/grpcx/codec"
	"golang.org/x/net/context"
	xlog "google.golang.org/grpc/grpclog"
)

type unaryState byte

const (
	unaryRequst    unaryState = 0
	unaryResponse  unaryState = 1
	unaryResendReq unaryState = 2
	unaryFinish    unaryState = 3
)

const (
	maxResendTimes = 3
)

func newUnarySession(ctx context.Context, cc *ClientConn, method string, opts ...CallOption) (_ *unarySession, err error) {
	var (
		conn   *connDial
		put    func()
		cancel context.CancelFunc
	)
	c := defaultCallInfo
	mc := cc.GetMethodConfig(method)
	if mc.WaitForReady != nil {
		c.failFast = !*mc.WaitForReady
	}

	if mc.Timeout != nil {
		ctx, cancel = context.WithTimeout(ctx, *mc.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
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

	getConn := func() (*connDial, func(), error) {
		for {
			conn, put, err := cc.getConn(ctx, gopts)
			if err != nil {
				if err == errConnClosing || err == errConnUnavailable {
					if c.failFast {
						return nil, nil, err
					}
					continue
				}
				return nil, nil, err
			}

			return conn, put, err
		}
	}
	conn, put, err = getConn()
	if err != nil {
		xlog.Warningf("grpcx: newUnarySession getConn fail:%v", err)
		return nil, err
	}

	cs := &unarySession{
		opts:      c,
		codec:     cc.dopts.codec,
		cancel:    cancel,
		ctx:       ctx,
		getConn:   getConn,
		put:       put,
		conn:      conn,
		method:    method,
		sessionid: cc.genStreamID(),
		pnotify:   make(chan struct{}),
		goawayCh:  make(chan struct{}),
		state:     unaryRequst,
	}

	cs.connCancel, _ = conn.withSession(cs.sessionid, cs)
	go func() {
		select {
		case <-cs.ctx.Done():
		}
		cs.finish()
	}()
	return cs, nil
}

// unarySession implements a client side Stream.
type unarySession struct {
	opts       callInfo
	conn       *connDial
	codec      codec.Codec
	msg        interface{}
	header     *PackHeader
	ctx        context.Context
	cancel     context.CancelFunc
	connCancel func()
	getConn    func() (*connDial, func(), error)
	method     string
	sessionid  int64

	err          error
	mu           sync.Mutex
	put          func()
	finished     bool
	pnotify      chan struct{}
	notifyNotify chan struct{}
	goawayCh     chan struct{}
	packet       *netPack
	state        unaryState
	resendTimes  int
}

func (cs *unarySession) Context() context.Context {
	return cs.ctx
}

func (cs *unarySession) streamID() int64 {
	return cs.sessionid
}

func (cs *unarySession) Run(args, reply interface{}) (err error) {
	defer cs.finish()

	for {
		switch cs.state {
		case unaryRequst:
			err = cs.SendMsg(args)
			if err != nil {
				return
			}
		case unaryResponse:
			err = cs.RecvMsg(reply)
			if err != nil {
				return err
			}
		case unaryResendReq:
			err = cs.resend()
			if err != nil {
				return err
			}
		case unaryFinish:
			return
		}
	}
}

func (cs *unarySession) SendMsg(m interface{}) (err error) {
	cs.header = &PackHeader{
		Ptype:     PackType_REQ,
		Sessionid: cs.sessionid,
		Methord:   cs.method,
	}

	if cs.opts.token != nil {
		cs.header.Metadata = withToken(cs.header.Metadata, *cs.opts.token)
	}

	cs.msg = m
	cs.err = cs.conn.send(cs.header, m)
	if cs.err != nil {
		xlog.Warningf("grpcx: SendMsg fail:%v", cs.err)
		if err = cs.switchConn(); err != nil {
			return err
		}

		cs.state = unaryResendReq
		return nil
	}
	cs.state = unaryResponse
	return
}

func (cs *unarySession) resend() error {
	cs.resendTimes++
	if cs.resendTimes >= maxResendTimes {
		return fmt.Errorf("grpcx: conn error after retry %v times", cs.resendTimes)
	}

	cs.err = cs.conn.send(cs.header, cs.msg)
	if cs.err != nil {
		xlog.Warningf("grpcx: resend fail:%v retry times:%v", cs.err, cs.resendTimes)
		return cs.switchConn()
	}
	cs.state = unaryResponse
	return nil
}

func (cs *unarySession) switchConn() error {
	conn, put, e := cs.getConn()
	if e != nil || conn == cs.conn {
		cs.err = fmt.Errorf("grpcx: connection error")
		return cs.err
	}

	cs.conn, cs.put = conn, put
	cs.connCancel, _ = conn.withSession(cs.sessionid, cs)
	cs.state = unaryResendReq
	return nil
}

func (cs *unarySession) RecvMsg(m interface{}) (err error) {
	defer func() {
		if err != nil {
			xlog.Warning(err)
		}
	}()

	select {
	case <-cs.pnotify:
	case <-cs.goawayCh:
		//ongoaway::wait 100ms up until retry
		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*100)
		//ctx := context.Background()
		select {
		case <-cs.pnotify:
			// cs.ctx timeout or receive msg from server
			break
		case <-cs.conn.Error():
			// maybe conn be closed by server
			if err = cs.switchConn(); err != nil {
				return
			}
		case <-ctx.Done():
			// wait timeout
			if err = cs.switchConn(); err != nil {
				return
			}
		}
	case <-cs.conn.Error():
		// transport error
		if err = cs.switchConn(); err != nil {
			return
		}
	}

	cs.state = unaryFinish
	if cs.packet == nil {
		if cs.err != nil {
			return cs.err
		}
		cs.err = fmt.Errorf("grpcx: unknow system error")
		return cs.err
	}

	if cs.packet.head.Ptype == PackType_RSP {
		cs.err = cs.codec.Unmarshal(cs.packet.body, m)
	} else if cs.packet.head.Ptype == PackType_ERROR {
		cs.err = fmt.Errorf("grpcx: error reply:%v", string(cs.packet.body))
	} else {
		cs.err = fmt.Errorf("grpcx: unknow reply:%v", string(cs.packet.body))
	}
	return cs.err
}

func (cs *unarySession) onGoaway() {
	defer func() {
		recover()
	}()

	close(cs.goawayCh)
}

func (cs *unarySession) onMessage(conn Conn, head *PackHeader, body []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("grpcx: unarySession::onMessage recover:%v", r)
		}

		if err != nil {
			xlog.Warning(err)
			cs.err = err
			cs.finish()
		}
	}()

	if cs.conn != conn {
		cs.packet = &netPack{
			head: &PackHeader{
				Ptype: PackType_ERROR,
			},
			body: []byte("grpcx: inner error:conn channel mix up"),
		}
		return fmt.Errorf("grpcx: inner error:conn channel mix up")
	}

	if head.Ptype == PackType_GoAway {
		cs.onGoaway()
		return
	}

	cs.packet = &netPack{
		head: head,
		body: body,
	}
	cs.closePNotify()
	return nil
}

func (cs *unarySession) finish() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.finished {
		return
	}

	cs.finished = true
	defer func() {
		if cs.cancel != nil {
			cs.cancel()
		}
	}()

	if cs.connCancel != nil {
		cs.connCancel()
		cs.connCancel = nil
	}

	if cs.put != nil {
		cs.put()
		cs.put = nil
	}

	if cs.notifyNotify != nil {
		cs.closeNotify()
	}
	cs.closePNotify()
}

func (cs *unarySession) closeNotify() {
	defer func() {
		recover()
	}()

	close(cs.notifyNotify)
}

func (cs *unarySession) closePNotify() {
	defer func() {
		recover()
	}()

	close(cs.pnotify)
}

func (cs *unarySession) done() <-chan struct{} {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.notifyNotify == nil {
		cs.notifyNotify = make(chan struct{})
	}
	return cs.notifyNotify
}

func (cs *unarySession) error() error {
	return cs.err
}
