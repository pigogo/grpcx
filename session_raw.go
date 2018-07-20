// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"golang.org/x/net/context"
	xlog "google.golang.org/grpc/grpclog"
)

type rawState byte

const (
	rawRequst    rawState = 0
	rawResendReq rawState = 1
	rawFinish    rawState = 2
)

func newRawSession(ctx context.Context, cc *ClientConn) (_ *rawSession, err error) {
	var (
		conn *connDial
	)
	c := defaultCallInfo
	for _, o := range cc.dopts.callOptions {
		if err := o.before(&c); err != nil {
			return nil, err
		}
	}
	c.maxSendMessageSize = getMaxSize(nil, c.maxSendMessageSize, defaultClientMaxSendMessageSize)
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
		xlog.Warningf("grpcx: rawSession getConn fail:%v", err)
		return nil, err
	}

	cs := &rawSession{
		opts:    c,
		getConn: getConn,
		conn:    conn,
		state:   rawRequst,
	}

	return cs, nil
}

type rawSession struct {
	opts        callInfo
	conn        *connDial
	getConn     func() (*connDial, func(), error)
	m           []byte
	err         error
	packet      *netPack
	state       rawState
	resendTimes int
}

func (cs *rawSession) Run(args []byte) (err error) {
	for {
		switch cs.state {
		case rawRequst:
			err = cs.SendRaw(args)
			if err != nil {
				return
			}
		case rawResendReq:
			err = cs.resend()
			if err != nil {
				return err
			}
		case rawFinish:
			return
		}
	}
}

func (cs *rawSession) SendRaw(m []byte) (err error) {
	cs.m = m
	cs.err = cs.conn.write(m)
	if cs.err != nil {
		xlog.Warningf("grpcx: SendRaw fail:%v", cs.err)
		conn, put, e := cs.getConn()
		if e != nil || conn == cs.conn {
			return cs.err
		}
		cs.conn, _ = conn, put
		cs.state = rawResendReq
		return nil
	}
	cs.state = rawFinish
	return
}

func (cs *rawSession) resend() error {
	cs.resendTimes++
	if cs.resendTimes >= maxResendTimes {
		return cs.err
	}

	cs.err = cs.conn.write(cs.m)
	if cs.err != nil {
		xlog.Warningf("grpcx: resend fail:%v", cs.err)
		conn, put, e := cs.getConn()
		if e != nil || cs.conn == conn {
			return cs.err
		}
		cs.conn, _ = conn, put
		return nil
	}
	cs.state = rawFinish
	return nil
}
