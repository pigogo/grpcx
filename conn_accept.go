// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"golang.org/x/net/context"
	xlog "google.golang.org/grpc/grpclog"
)

var (
	errKeyExist = errors.New("key exist")
)

// Conn is a network connection to a given address.
type Conn interface {
	withSession(id int64, val Stream) (func(), error)
	sessionOf(id int64) Stream
	context() context.Context
	send(head *PackHeader, m interface{}, maxMsgSize int) error
	close()
	gracefulClose() <-chan struct{}
}

type connAccept struct {
	id            int64
	ctx           context.Context
	mux           sync.RWMutex
	opts          options
	connMap       map[int64]Stream
	maxStreamID   int64
	conn          net.Conn
	lastAliveTime time.Time
	lastPoneTime  time.Time
	closed        bool
	tick          *time.Ticker
	graceDone     chan struct{}
	onPacket      func(head *PackHeader, body []byte, conn Conn) error
	onClose       func(int64)
}

func newAcceptConn(id int64, opts options, conn net.Conn, onPacket func(head *PackHeader, body []byte, conn Conn) error, onClose func(id int64)) Conn {
	aconn := &connAccept{
		id:       id,
		opts:     opts,
		conn:     conn,
		connMap:  make(map[int64]Stream),
		onPacket: onPacket,
		onClose:  onClose,
	}
	aconn.ctx = withConnID(context.Background(), id)
	go aconn.read()
	if opts.keepalivePeriod > 0 {
		aconn.tick = time.NewTicker(opts.keepalivePeriod)
		go aconn.aliveLook()
	}
	return aconn
}

func (aconn *connAccept) close() {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()
	if aconn.closed {
		return
	}
	aconn.closed = true
	if aconn.opts.connPlugin != nil {
		aconn.opts.connPlugin.OnPreDisconnect(aconn)
	}

	aconn.conn.Close()
	if aconn.opts.connPlugin != nil {
		aconn.opts.connPlugin.OnPostDisconnect(aconn)
	}

	aconn.closeGraceDone()
	if aconn.tick != nil {
		aconn.tick.Stop()
		aconn.tick = nil
	}
}

func (aconn *connAccept) onclose() {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()
	if aconn.closed {
		return
	}

	aconn.closed = true
	aconn.conn.Close()
	if aconn.opts.connPlugin != nil {
		aconn.opts.connPlugin.OnPostDisconnect(aconn)
	}

	aconn.onClose(aconn.id)
	aconn.closeGraceDone()
	if aconn.tick != nil {
		aconn.tick.Stop()
		aconn.tick = nil
	}
}

func (aconn *connAccept) closeGraceDone() {
	defer func() {
		recover()
	}()

	close(aconn.graceDone)
}

func (aconn *connAccept) context() context.Context {
	return aconn.ctx
}

func (aconn *connAccept) gracefulClose() <-chan struct{} {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()
	if aconn.closed {
		graceDone := make(chan struct{})
		close(graceDone)
		return graceDone
	}

	if aconn.graceDone != nil {
		return aconn.graceDone
	}

	aconn.graceDone = make(chan struct{})
	aconn.notifyGoaway()
	return aconn.graceDone
}

func (aconn *connAccept) withSession(id int64, session Stream) (func(), error) {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()

	/*if aconn.draining { todo:
		xlog.Info("grpcx: withSession connAccept when connection is draining")
		return nil, errDraining
	}*/

	if _, ok := aconn.connMap[id]; ok {
		xlog.Warningf("grpcx: connAccept session is exist:%v", id)
		return nil, errKeyExist
	}

	aconn.connMap[id] = session

	return func() {
		aconn.mux.Lock()
		defer aconn.mux.Unlock()
		delete(aconn.connMap, id)
	}, nil
}

func (aconn *connAccept) sessionOf(id int64) Stream {
	aconn.mux.RLock()
	defer aconn.mux.RUnlock()

	return aconn.connMap[id]
}

func (aconn *connAccept) send(head *PackHeader, m interface{}, maxMsgSize int) error {
	mtype := reflect.TypeOf(m)
	cbuf := getBufferFromPool(mtype)
	defer func() {
		putBufferToPool(mtype, cbuf)
	}()

	_, err := encodeNetmsg(aconn.opts.codec, aconn.opts.cp, head, m, cbuf, maxMsgSize)
	if err != nil {
		return err
	}

	return aconn.write(cbuf.Bytes())
}

func (aconn *connAccept) write(out []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
		if err != nil {
			aconn.onclose()
		}
	}()

	_, err = aconn.conn.Write(out)
	return err
}

func (aconn *connAccept) read() (err error) {
	defer func() {
		if e := recover(); e != nil || err != nil {
			xlog.Infof("grpcx: connAccept read exist with recover error:%v error:%v", e, err)
		}
		aconn.onclose()
	}()

	var (
		reader = bufio.NewReaderSize(aconn.conn, int(aconn.opts.readerWindowSize))
		header *PackHeader
		msg    []byte
	)

	for {
		header, msg, err = parseAndRecvMsg(reader, aconn.opts.dc, aconn.opts.maxReceiveMessageSize)
		if err != nil {
			return err
		}

		if header.Ptype == PackType_PING {
			go aconn.handlePingPacket(header, msg)
			continue
		}

		if aconn.maxStreamID < header.Sessionid {
			aconn.maxStreamID = header.Sessionid
		} else {
			//todo:
		}

		aconn.onPacket(header, msg, aconn)
	}
}

func (aconn *connAccept) notifyGoaway() {
	goawayHead := &PackHeader{
		Ptype:     PackType_GoAway,
		Sessionid: aconn.maxStreamID,
	}

	if aconn.send(goawayHead, nil, defaultSendMessageSize) != nil {
		aconn.closeGraceDone()
	}
}

func (aconn *connAccept) aliveLook() {
	tick := aconn.tick
	for {
		_, ok := <-tick.C
		if !ok {
			return
		}

		aconn.mux.Lock()
		//timeout
		if time.Since(aconn.lastAliveTime) > aconn.opts.keepalivePeriod {
			aconn.mux.Unlock()
			xlog.Warningf("grpcx: aliveLook connection timeout:%v", aconn.conn.RemoteAddr())
			if aconn.opts.connPlugin != nil {
				aconn.opts.connPlugin.OnPreDisconnect(aconn)
			}
			aconn.onclose()
			return
		}
		aconn.mux.Unlock()
	}
}

func (aconn *connAccept) handlePingPacket(head *PackHeader, p []byte) {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()

	head.Ptype = PackType_PONG
	aconn.lastAliveTime = time.Now()
	if time.Since(aconn.lastPoneTime) < time.Millisecond*300 {
		return
	}
	aconn.lastPoneTime = aconn.lastAliveTime
	if e := aconn.send(head, nil, defaultSendMessageSize); e != nil {
		xlog.Warningf("grpcx: connAccept.handlePingPacket failed to send Pong: %v", e)
	}
}
