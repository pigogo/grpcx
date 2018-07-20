// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

// connAccept2 has the same implemention of connAccept except the write and send function.
// connAccept2 takes a buffer to hold the outbound packet in order to reduce the network
// flushing op wich is exclusive and take more time when the connection cross datacenter
// so connAccept2 is more suitable for the call which is cross datacenter and the connection is shared
// for diff calls in business layer 
package grpcx

import (
	"bufio"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"golang.org/x/net/context"
	xlog "google.golang.org/grpc/grpclog"
)

type connAccept2 struct {
	connAccept
	buf           *putBuffer
	id            int64
	ctx           context.Context
	cancel        context.CancelFunc
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

func newAcceptConn2(id int64, opts options, conn net.Conn, onPacket func(head *PackHeader, body []byte, conn Conn) error, onClose func(id int64)) Conn {
	aconn := &connAccept2{
		id:       id,
		opts:     opts,
		conn:     conn,
		connMap:  make(map[int64]Stream),
		onPacket: onPacket,
		onClose:  onClose,
		buf:      newPutBuffer(),
	}
	ctx := withConnID(context.Background(), id)
	aconn.ctx, aconn.cancel = context.WithCancel(ctx)
	go aconn.read()
	go aconn.write()
	if opts.keepalivePeriod > 0 {
		go aconn.aliveLook()
	}
	return aconn
}

func (aconn *connAccept2) close() {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()
	if aconn.closed {
		return
	}
	aconn.closed = true
	if aconn.opts.connPlugin != nil {
		aconn.opts.connPlugin.OnPreDisconnect(aconn)
	}
	aconn.cancel()
	aconn.conn.Close()
	if aconn.opts.connPlugin != nil {
		aconn.opts.connPlugin.OnPostDisconnect(aconn)
	}
}

func (aconn *connAccept2) onclose() {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()
	if aconn.closed {
		return
	}

	aconn.cancel()
	aconn.closed = true
	aconn.conn.Close()
	if aconn.opts.connPlugin != nil {
		aconn.opts.connPlugin.OnPostDisconnect(aconn)
	}
	aconn.onClose(aconn.id)
}

func (aconn *connAccept2) closeRead() {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()
	if aconn.closed {
		return
	}
	aconn.conn.(*net.TCPConn).CloseRead()
}

func (aconn *connAccept2) context() context.Context {
	return aconn.ctx
}

func (aconn *connAccept2) drain() <-chan struct{} {
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

func (aconn *connAccept2) withSession(id int64, session Stream) (func(), error) {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()

	if aconn.draining {
		xlog.Info("grpcx: withSession connAccept2 when connection is draining")
		return nil, errDraining
	}

	if _, ok := aconn.connMap[id]; ok {
		xlog.Warningf("grpcx: connAccept2 session is exist:%v", id)
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

func (aconn *connAccept2) sessionOf(id int64) Stream {
	aconn.mux.RLock()
	defer aconn.mux.RUnlock()

	return aconn.connMap[id]
}

func (aconn *connAccept2) send(head *PackHeader, m interface{}) error {
	mtype := reflect.TypeOf(m)
	cbuf := getBufferFromPool(mtype)
	_, err := encodeNetmsg(aconn.opts.codec, aconn.opts.cp, head, m, cbuf, aconn.opts.maxSendMessageSize)
	if err != nil {
		return err
	}

	aconn.buf.put(&outPack{
		mtype: mtype,
		body:  cbuf,
	})
	return nil
}

func (aconn *connAccept2) write() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
			xlog.Infof("grpcx: connAccept2 write exist with recover error:%v", err)
		}
		aconn.onclose()
	}()

	var msg *outPack
	var buf = bufio.NewWriterSize(aconn.conn, 65535)
	for {
		select {
		case msg = <-aconn.buf.get():
			aconn.buf.load()
			_, err = buf.Write(msg.body.Bytes())
			putBufferToPool(msg.mtype, msg.body)
			if err != nil {
				return err
			}
		loop:
			for {
				select {
				case msg = <-aconn.buf.get():
					aconn.buf.load()
					_, err = buf.Write(msg.body.Bytes())
					putBufferToPool(msg.mtype, msg.body)
					if err != nil {
						return err
					}
				default:
					break loop
				}
			}
			buf.Flush()
			break
		case <-aconn.ctx.Done():
			return
		}
	}
}

func (aconn *connAccept2) read() (err error) {
	defer func() {
		if e := recover(); e != nil || err != nil {
			xlog.Infof("grpcx: connAccept2 read exist with recover error:%v error:%v", e, err)
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

func (aconn *connAccept2) aliveLook() {
	tick := time.NewTicker(aconn.opts.keepalivePeriod)
	defer tick.Stop()

	for {
		<-tick.C
		select {
		case <-tick.C:
		case <-aconn.ctx.Done():
			return
		}

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

func (aconn *connAccept2) handlePingPacket(head *PackHeader, p []byte) {
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
