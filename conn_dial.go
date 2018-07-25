// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"golang.org/x/net/context"
	xlog "google.golang.org/grpc/grpclog"
)

const infinitTime = time.Hour * 24 * 30 * 12

type connDial struct {
	Conn
	mux     sync.RWMutex
	connMap map[int64]Stream

	cancel context.CancelFunc
	ctx    context.Context

	netaddr        string
	conn           net.Conn
	csm            ConnectivityStateManager
	drainDone      chan struct{}
	goawayDone     chan struct{}
	closed         bool
	copts          callInfo
	opts           dialOptions
	input          *bytes.Buffer
	errch          chan struct{}
	reconnectTimer *time.Timer
	lastAliveTime  time.Time
	pingSid        int64
	up             func() (down func(error))
	down           func(error)
}

func newDialConn(ctx context.Context, netaddr string, opts dialOptions, cse *ConnectivityStateEvaluator, up func() (down func(error))) (*connDial, error) {
	dconn := &connDial{
		opts:           opts,
		netaddr:        netaddr,
		input:          new(bytes.Buffer),
		reconnectTimer: time.NewTimer(infinitTime),
		connMap:        make(map[int64]Stream),
		up:             up,
	}

	for _, op := range opts.callOptions {
		op.before(&dconn.copts)
	}

	dconn.ctx, dconn.cancel = context.WithCancel(ctx)
	dconn.ctx = withConnAddr(dconn.ctx, netaddr)
	dconn.csm.cse = cse
	go dconn.reconnectMonitor()
	return dconn, nil
}

func (dconn *connDial) context() context.Context {
	return dconn.ctx
}

func (dconn *connDial) setState(state ConnectivityState) bool {
	if dconn.csm.UpdateState(state) {
		switch state {
		case Ready:
			if dconn.up != nil {
				dconn.down = dconn.up()
			}
			if dconn.opts.connPlugin != nil {
				dconn.opts.connPlugin.OnPostConnect(dconn)
			}
			dconn.resetReconnectTimer(infinitTime)
		case TransientFailure:
			if dconn.down != nil {
				dconn.down(nil)
			}
			if dconn.opts.connPlugin != nil {
				dconn.opts.connPlugin.OnPostDisconnect(dconn)
			}
			if dconn.conn != nil {
				dconn.conn.Close()
				dconn.conn = nil
			}

			dconn.input.Reset() //clean input buffer
			dconn.resetReconnectTimer(time.Second)
			if dconn.errch != nil {
				close(dconn.errch)
				dconn.errch = nil
			}
		case Shutdown:
			if dconn.down != nil {
				dconn.down(nil)
			}
			if dconn.opts.connPlugin != nil {
				dconn.opts.connPlugin.OnPostDisconnect(dconn)
			}
			if dconn.conn != nil {
				dconn.conn.Close()
				dconn.conn = nil
			}
			if dconn.errch != nil {
				close(dconn.errch)
				dconn.errch = nil
			}
			dconn.input.Reset()
		case Goaway:
			if dconn.down != nil {
				dconn.down(nil)
			}
		}
		return true
	}
	return false
}

func (dconn *connDial) dial() (err error) {
	if err = dconn.redial(); err != nil {
		dconn.resetReconnectTimer(time.Second)
	} else {
		go dconn.read()
		dconn.setState(Ready)
	}
	return
}

func (dconn *connDial) redial() (err error) {
	if dconn.GetState() == Shutdown {
		return errConnClosing
	}

	if dconn.opts.connPlugin != nil {
		dconn.opts.connPlugin.OnPreConnect(dconn.netaddr)
	}

	dconn.conn, err = net.DialTimeout("tcp4", dconn.netaddr, dconn.opts.timeout)
	if err != nil {
		xlog.Errorf("grpcx: dial:%v error:%v", dconn.netaddr, err)
		return
	}

	if dconn.opts.creds != nil {
		dconn.conn, _, err = dconn.opts.creds.ClientHandshake(dconn.ctx, dconn.netaddr, dconn.conn)
		if err != nil {
			xlog.Errorf("grpcx: tls client handshake with:%v error:%v", dconn.netaddr, err)
			return
		}
	}

	if dconn.opts.connPlugin != nil {
		dconn.opts.connPlugin.OnPostConnect(dconn)
	}
	dconn.lastAliveTime = time.Now()
	return
}

func (dconn *connDial) close() {
	dconn.mux.Lock()
	defer func() {
		recover()
		dconn.mux.Unlock()
	}()

	if dconn.closed || !dconn.setState(Shutdown) {
		return
	}

	dconn.closed = true
	if dconn.errch != nil {
		close(dconn.errch)
		dconn.errch = nil
	}

	if dconn.cancel != nil {
		dconn.cancel()
		dconn.cancel = nil
	}

	if dconn.conn != nil {
		dconn.conn.Close()
		dconn.conn = nil
	}

	dconn.closeGoawayDone()
	dconn.closeGoawayDone()
	dconn.up = nil
	dconn.down = nil
	dconn.csm.cse = nil
	dconn.reconnectTimer.Stop()
}

func (dconn *connDial) closeGraceDone() {
	defer func() {
		recover()
	}()

	close(dconn.drainDone)
}

func (dconn *connDial) closeGoawayDone() {
	defer func() {
		recover()
	}()

	close(dconn.goawayDone)
}

func (dconn *connDial) gracefulClose() <-chan struct{} {
	dconn.mux.Lock()
	defer dconn.mux.Unlock()
	if dconn.drainDone != nil {
		return dconn.drainDone
	} else if dconn.goawayDone != nil {
		return dconn.goawayDone
	} else if dconn.closed {
		drainDone := make(chan struct{})
		close(drainDone)
		return drainDone
	}

	dconn.reconnectTimer.Stop()
	dconn.drainDone = make(chan struct{})
	if len(dconn.connMap) == 0 {
		close(dconn.drainDone)
		return dconn.drainDone
	}

	// close all stream session
	for _, session := range dconn.connMap {
		if cstream, ok := session.(*clientStream); ok {
			cstream.CloseSend()
		}
	}

	return dconn.drainDone
}

func (dconn *connDial) resetReconnectTimer(duration time.Duration) {
	dconn.reconnectTimer.Reset(duration)
}

func (dconn *connDial) GetState() ConnectivityState {
	return dconn.csm.GetState()
}

func (dconn *connDial) GetNotifyChan() <-chan struct{} {
	return dconn.csm.GetNotifyChan()
}

func (dconn *connDial) Ready() bool {
	return dconn.GetState() == Ready
}

func (dconn *connDial) Error() chan struct{} {
	dconn.mux.Lock()
	defer dconn.mux.Unlock()

	if dconn.errch == nil {
		state := dconn.GetState()
		if state == Ready || state == Connecting {
			dconn.errch = make(chan struct{})
		}
	}
	return dconn.errch
}

func (dconn *connDial) Wait(ctx context.Context, hasBalancer, failfast bool) error {
	var ch <-chan struct{}
	for {
		state := dconn.GetState()
		if state == Ready {
			return nil
		}

		switch state {
		case Shutdown:
			fallthrough
		case Goaway:
			if failfast || !hasBalancer {
				return errNetworkIO
			}
			return errConnClosing
		case TransientFailure:
			if failfast || hasBalancer {
				return errConnUnavailable
			}
		}

		if ch == nil {
			ch = dconn.csm.GetNotifyChan()
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			ch = nil
		}
	}
}

func (dconn *connDial) withSession(id int64, val Stream) (func(), error) {
	dconn.mux.Lock()
	defer dconn.mux.Unlock()

	if dconn.goawayDone != nil {
		return nil, errDraining
	}

	if _, ok := dconn.connMap[id]; ok {
		xlog.Errorf("grpcx: connDial session exist:%v", id)
		return nil, errKeyExist
	}

	dconn.connMap[id] = val
	return func() {
		dconn.mux.Lock()
		delete(dconn.connMap, id)
		//on draining: close when no session exist
		if (dconn.drainDone != nil || dconn.goawayDone != nil) && len(dconn.connMap) == 0 {
			dconn.mux.Unlock()
			dconn.close()
			return
		}
		dconn.mux.Unlock()
	}, nil
}

func (dconn *connDial) sessionOf(id int64) Stream {
	dconn.mux.RLock()
	defer dconn.mux.RUnlock()

	return dconn.connMap[id]
}

func (dconn *connDial) reconnectMonitor() {
	aliveTime := time.Duration(0)
	if dconn.opts.keepalivePeriod == 0 {
		aliveTime = time.Duration(infinitTime)
	} else {
		if dconn.opts.keepalivePeriod < time.Second {
			dconn.opts.keepalivePeriod = time.Second
		}

		aliveTime = dconn.opts.keepalivePeriod / 3
	}
	aliveTick := time.NewTicker(aliveTime)
	resetPeriod := time.Second
loop:
	for {
		select {
		case <-dconn.ctx.Done():
			break loop
		case _, ok := <-dconn.reconnectTimer.C:
			if !ok { // receive goaway
				break loop
			}
			if dconn.redial() == nil {
				dconn.setState(Ready)
				resetPeriod = time.Second
				go dconn.read()
			} else {
				resetPeriod *= 2
				if resetPeriod > time.Second*15 {
					resetPeriod = time.Second * 15
				}
				dconn.resetReconnectTimer(resetPeriod)
			}
		case <-aliveTick.C:
			dconn.aliveCheck()
		}
	}
	aliveTick.Stop()
}

func (dconn *connDial) aliveCheck() {
	if dconn.opts.keepalivePeriod == 0 {
		return
	}

	if dconn.GetState() != Ready {
		return
	}

	if time.Since(dconn.lastAliveTime) > dconn.opts.keepalivePeriod {
		dconn.setState(TransientFailure)
		xlog.Errorf("grpcx: connDial aliveCheck conn timeout after %v not receive Pong from server", dconn.opts.keepalivePeriod)
		return
	}

	dconn.pingSid++
	aliveReq := &PackHeader{
		Ptype:     PackType_PING,
		Sessionid: dconn.pingSid,
	}
	dconn.send(aliveReq, nil, defaultSendMessageSize)
}

func (dconn *connDial) send(head *PackHeader, m interface{}, maxMsgSize int) error {
	mtype := reflect.TypeOf(m)
	cbuf := getBufferFromPool(mtype)
	defer func() {
		putBufferToPool(mtype, cbuf)
	}()

	_, err := encodeNetmsg(dconn.opts.codec, dconn.opts.cp, head, m, cbuf, maxMsgSize)
	if err != nil {
		xlog.Warning("grpcx: connDial encodeNetmsg message error:%v", err)
		return err
	}

	return dconn.write(cbuf.Bytes())
}

func (dconn *connDial) write(out []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	_, err = dconn.conn.Write(out)
	if err != nil {
		dconn.setState(TransientFailure)
		dconn.conn.Close()
	}
	return err
}

func (dconn *connDial) read() (err error) {
	defer func() {
		if e := recover(); e != nil || err != nil {
			dconn.setState(TransientFailure)
			xlog.Infof("grpcx: connDial read exist with recover error:%v error:%v", e, err)
		}
	}()

	var (
		reader      = bufio.NewReaderSize(dconn.conn, int(dconn.opts.readerWindowSize))
		header      *PackHeader
		msg         []byte
		maxRecvSize = *dconn.copts.maxReceiveMessageSize
	)

	for {
		header, msg, err = parseAndRecvMsg(reader, dconn.opts.dc, maxRecvSize)
		if err != nil {
			return err
		}

		if header.Ptype == PackType_SRSP {
			dconn.handlePacket(header, msg)
		} else if header.Ptype == PackType_GoAway {
			dconn.handleGoAway(header)
		} else {
			go dconn.handlePacket(header, msg)
		}
	}
}

func (dconn *connDial) handlePacket(header *PackHeader, packet []byte) {
	if header.Ptype == PackType_PONG {
		if header.Sessionid <= dconn.pingSid {
			dconn.lastAliveTime = time.Now()
		}
		return
	} else if header.Ptype == PackType_Notify { //notify from server
		handleNotify(dconn, header, packet, dconn.opts.codec)
		return
	}

	stream := dconn.sessionOf(header.Sessionid)
	if stream == nil {
		xlog.Warningf("grpcx: handlePacket stream timeout:%v", header.Sessionid)
		return
	}

	stream.onMessage(dconn, header, packet)
}

func (dconn *connDial) handleGoAway(head *PackHeader) {
	xlog.Errorf("grpcx: handleGoAway sessionid:%v remoteaddr:%v", head.Sessionid, dconn.conn.RemoteAddr())
	dconn.mux.Lock()
	//no waiting task: close directory
	if len(dconn.connMap) == 0 {
		dconn.mux.Unlock()
		dconn.close()
		return
	}

	defer dconn.mux.Unlock()
	if dconn.setState(Goaway) != true {
		return
	}

	xlog.Warningf("grpcx: handleGoAway sessionNum:%v", len(dconn.connMap))
	dconn.goawayDone = make(chan struct{})
	//notify on serving task
	for _, session := range dconn.connMap {
		session.onMessage(dconn, head, []byte{})
	}
}
