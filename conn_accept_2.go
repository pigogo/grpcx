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
	buf           *putBuffer
	id            int64
	ctx           context.Context
	cancel        context.CancelFunc
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
		aconn.tick = time.NewTicker(opts.keepalivePeriod)
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

	aconn.closeGraceDone()
	if aconn.tick != nil {
		aconn.tick.Stop()
		aconn.tick = nil
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
	aconn.closeGraceDone()
	if aconn.tick != nil {
		aconn.tick.Stop()
		aconn.tick = nil
	}
}

func (aconn *connAccept2) closeGraceDone() {
	defer func() {
		recover()
	}()

	close(aconn.graceDone)
}

func (aconn *connAccept2) context() context.Context {
	return aconn.ctx
}

func (aconn *connAccept2) gracefulClose() <-chan struct{} {
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

func (aconn *connAccept2) withSession(id int64, session Stream) (func(), error) {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()

	/*if aconn.draining { todo:
		xlog.Info("grpcx: withSession connAccept2 when connection is draining")
		return nil, errDraining
	}*/

	if _, ok := aconn.connMap[id]; ok {
		xlog.Warningf("grpcx: connAccept2 session is exist:%v", id)
		return nil, errKeyExist
	}

	aconn.connMap[id] = session

	return func() {
		aconn.mux.Lock()
		delete(aconn.connMap, id)
		if aconn.graceDone != nil && len(aconn.connMap) == 0 {
			aconn.mux.Unlock()
			aconn.close()
			return
		}
		aconn.mux.Unlock()
	}, nil
}

func (aconn *connAccept2) sessionOf(id int64) Stream {
	aconn.mux.RLock()
	defer aconn.mux.RUnlock()

	return aconn.connMap[id]
}

func (aconn *connAccept2) send(head *PackHeader, m interface{}, maxMsgSize int) error {
	mtype := reflect.TypeOf(m)
	cbuf := getBufferFromPool(mtype)
	_, err := encodeNetmsg(aconn.opts.codec, aconn.opts.cp, head, m, cbuf, maxMsgSize)
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
		if err != nil {
			aconn.onclose()
		}
	}()

	var msg *outPack
	var buf = bufio.NewWriterSize(aconn.conn, int(aconn.opts.readerWindowSize))
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

		if aconn.maxStreamID < header.Sessionid {
			aconn.maxStreamID = header.Sessionid
		} else {
			//todo:
		}

		aconn.onPacket(header, msg, aconn)
	}
}

func (aconn *connAccept2) notifyGoaway() {
	goawayHead := &PackHeader{
		Ptype:     PackType_GoAway,
		Sessionid: aconn.maxStreamID,
	}

	if aconn.send(goawayHead, nil, aconn.opts.maxSendMessageSize) != nil {
		aconn.closeGraceDone()
	}
}
func (aconn *connAccept2) aliveLook() {
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

func (aconn *connAccept2) handlePingPacket(head *PackHeader, p []byte) {
	aconn.mux.Lock()
	defer aconn.mux.Unlock()

	head.Ptype = PackType_PONG
	aconn.lastAliveTime = time.Now()
	if time.Since(aconn.lastPoneTime) < time.Millisecond*300 {
		return
	}
	aconn.lastPoneTime = aconn.lastAliveTime
	if e := aconn.send(head, nil, aconn.opts.maxSendMessageSize); e != nil {
		xlog.Warningf("grpcx: connAccept.handlePingPacket failed to send Pong: %v", e)
	}
}
