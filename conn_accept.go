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
	send(head *PackHeader, m interface{}) error
	close()
	closeRead()
	drain() <-chan struct{}
}

type connAccept struct {
	id            int64
	ctx           context.Context
	mux           sync.RWMutex
	opts          options
	connMap       map[int64]Stream
	conn          net.Conn
	lastAliveTime time.Time
	lastPoneTime  time.Time
	draining      bool
	closed        bool
	drainDone     chan struct{}
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
}

func (aconn *connAccept) closeRead() {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()
	if aconn.closed {
		return
	}
	aconn.conn.(*net.TCPConn).CloseRead()
}

func (aconn *connAccept) context() context.Context {
	return aconn.ctx
}

func (aconn *connAccept) drain() <-chan struct{} {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()
	if aconn.draining {
		return aconn.drainDone
	} else if aconn.closed {
		drainDone := make(chan struct{})
		close(drainDone)
		return drainDone
	}

	aconn.draining = true
	aconn.drainDone = make(chan struct{})
	if len(aconn.connMap) == 0 {
		close(aconn.drainDone)
		return aconn.drainDone
	}

	//put eof packet
	eofHeader := &PackHeader{
		Ptype: PackType_EOF,
	}
	for _, session := range aconn.connMap {
		session.onMessage(aconn, eofHeader, nil)
	}
	return aconn.drainDone
}

func (aconn *connAccept) withSession(id int64, session Stream) (func(), error) {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()

	if aconn.draining {
		xlog.Info("grpcx: withSession connAccept when connection is draining")
		return nil, errDraining
	}

	if _, ok := aconn.connMap[id]; ok {
		xlog.Warningf("grpcx: connAccept session is exist:%v", id)
		return nil, errKeyExist
	}

	aconn.connMap[id] = session

	return func() {
		aconn.mux.Lock()
		defer aconn.mux.Unlock()
		delete(aconn.connMap, id)
		if aconn.draining && len(aconn.connMap) == 0 {
			close(aconn.drainDone)
		}
	}, nil
}

func (aconn *connAccept) sessionOf(id int64) Stream {
	aconn.mux.RLock()
	defer aconn.mux.RUnlock()

	return aconn.connMap[id]
}

func (aconn *connAccept) send(head *PackHeader, m interface{}) error {
	mtype := reflect.TypeOf(m)
	cbuf := getBufferFromPool(mtype)
	defer func() {
		putBufferToPool(mtype, cbuf)
	}()

	_, err := encodeNetmsg(aconn.opts.codec, aconn.opts.cp, head, m, cbuf, aconn.opts.maxSendMessageSize)
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
	}()

	_, err = aconn.conn.Write(out)
	if err != nil {
		aconn.onclose()
	}
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
		reader = bufio.NewReaderSize(aconn.conn, 65535)
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

		aconn.onPacket(header, msg, aconn)
	}
}

func (aconn *connAccept) aliveLook() {
	tick := time.NewTicker(aconn.opts.keepalivePeriod)
	defer tick.Stop()

	for {
		<-tick.C
		aconn.mux.Lock()
		if aconn.draining || aconn.closed {
			aconn.mux.Unlock()
			return
		}

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
	if e := aconn.send(head, nil); e != nil {
		xlog.Warningf("grpcx: connAccept.handlePingPacket failed to send Pong: %v", e)
	}
}
