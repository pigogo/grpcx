// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"fmt"
	"sync"
	"time"

	"github.com/pigogo/grpcx/codec"
	xlog "github.com/pigogo/grpcx/grpclog"
	"golang.org/x/net/context"
)

type unaryState byte

const (
	unaryRequst    unaryState = 0
	unaryResponse  unaryState = 1
	unaryResendReq unaryState = 2
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

	hkey := uint32(0)
	if c.hbKey != nil {
		hkey = *c.hbKey
	}
	gopts := BalancerGetOptions{
		BlockingWait: !c.failFast,
		HashKey:      hkey,
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
	cs.connCancel, err = conn.withSession(cs.sessionid, cs)
	if err != nil {
		err = fmt.Errorf("grpcx: conn withSession error:%v", err)
		xlog.Error(err)
		return nil, err
	}

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

	err error
	mu  sync.Mutex
	put func()

	pnotify     chan struct{}
	notifyCh    chan struct{}
	goawayCh    chan struct{}
	packet      *netPack
	state       unaryState
	resendTimes int
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
		case unaryResendReq:
			err = cs.resend()
			if err != nil {
				return err
			}
		case unaryResponse:
			return cs.RecvMsg(reply)
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
	cs.err = cs.conn.send(cs.header, m, *cs.opts.maxSendMessageSize)
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

	cs.err = cs.conn.send(cs.header, cs.msg, *cs.opts.maxSendMessageSize)
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
	cs.connCancel()
	cs.conn, cs.put = conn, put
	cs.connCancel, _ = conn.withSession(cs.sessionid, cs)
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
		// receive msg from server
	case <-cs.goawayCh:
		//ongoaway::wait 100ms up until retry
		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*100)
		//ctx := context.Background()
		select {
		case <-cs.pnotify:
			// receive msg from server
			break
		case <-cs.conn.Error():
			cs.err = fmt.Errorf("grpcx: service goaway")
			return cs.err
		case <-ctx.Done():
			cs.err = fmt.Errorf("grpcx: service goaway")
			return cs.err
		}
	case <-cs.conn.Error():
		cs.err = fmt.Errorf("grpcx: service goaway")
		return cs.err
	case <-cs.ctx.Done():
		return cs.ctx.Err()
	}

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
		cs.err = fmt.Errorf("grpcx: unknow reply:%v", cs.packet.head.Ptype)
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
	if cs.conn != conn {
		return
	}

	if head.Ptype == PackType_GoAway {
		cs.onGoaway()
		return
	}

	cs.packet = &netPack{
		head: head,
		body: body,
	}
	close(cs.pnotify)
	return nil
}

func (cs *unarySession) finish() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

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

	if cs.notifyCh != nil {
		close(cs.notifyCh)
		cs.notifyCh = nil
	}
}

func (cs *unarySession) done() <-chan struct{} {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.notifyCh == nil {
		cs.notifyCh = make(chan struct{})
	}
	return cs.notifyCh
}

func (cs *unarySession) error() error {
	return cs.err
}
