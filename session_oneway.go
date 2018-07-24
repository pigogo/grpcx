// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"golang.org/x/net/context"
	xlog "google.golang.org/grpc/grpclog"
)

type oneState byte

const (
	oneRequst    oneState = 0
	oneResendReq oneState = 1
	oneFinish    oneState = 2
)

func newOnewaySession(ctx context.Context, cc *ClientConn, method string, opts ...CallOption) (_ *onewaySession, err error) {
	var (
		conn *connDial
	)
	c := defaultCallInfo
	mc := cc.GetMethodConfig(method)
	if mc.WaitForReady != nil {
		c.failFast = !*mc.WaitForReady
	}

	opts = append(cc.dopts.callOptions, opts...)
	for _, o := range opts {
		if err := o.before(&c); err != nil {
			return nil, err
		}
	}
	c.maxSendMessageSize = getMaxSize(mc.MaxReqSize, c.maxSendMessageSize, defaultClientMaxSendMessageSize)
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
	conn, _, err = getConn()
	if err != nil {
		xlog.Warningf("grpcx: newOnewaySession getConn fail:%v", err)
		return nil, err
	}

	cs := &onewaySession{
		opts:      c,
		getConn:   getConn,
		conn:      conn,
		method:    method,
		state:     oneRequst,
		sessionid: cc.genStreamID(),
	}

	return cs, nil
}

type onewaySession struct {
	opts      callInfo
	conn      *connDial
	method    string
	msg       interface{}
	header    *PackHeader
	sessionid int64
	getConn   func() (*connDial, func(), error)

	err         error
	packet      *netPack
	state       oneState
	resendTimes int
}

func (cs *onewaySession) Run(args interface{}) (err error) {
	for {
		switch cs.state {
		case oneRequst:
			err = cs.SendMsg(args)
			if err != nil {
				return
			}
		case oneResendReq:
			err = cs.resend()
			if err != nil {
				return err
			}
		case oneFinish:
			return
		}
	}
}

func (cs *onewaySession) SendMsg(m interface{}) (err error) {
	cs.header = &PackHeader{
		Ptype:     PackType_REQ,
		Methord:   cs.method,
		Sessionid: cs.sessionid,
	}
	if cs.opts.token != nil {
		cs.header.Metadata = withToken(cs.header.Metadata, *cs.opts.token)
	}
	cs.msg = m
	cs.err = cs.conn.send(cs.header, m)
	if cs.err != nil {
		xlog.Warningf("grpcx: SendMsg fail:%v", cs.err)
		conn, put, e := cs.getConn()
		if e != nil || conn == cs.conn {
			return cs.err
		}
		cs.conn, _ = conn, put
		cs.state = oneResendReq
		return nil
	}
	cs.state = oneFinish
	return
}

func (cs *onewaySession) resend() error {
	cs.resendTimes++
	if cs.resendTimes >= maxResendTimes {
		return cs.err
	}

	cs.err = cs.conn.send(cs.header, cs.msg)
	if cs.err != nil {
		xlog.Warningf("grpcx: resend fail:%v", cs.err)
		conn, put, e := cs.getConn()
		if e != nil || cs.conn == conn {
			return cs.err
		}
		cs.conn, _ = conn, put
		return nil
	}
	cs.state = oneFinish
	return nil
}
