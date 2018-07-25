// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"fmt"
	"io"

	"github.com/pigogo/grpcx/codec"

	"golang.org/x/net/context"
	xlog "google.golang.org/grpc/grpclog"
)

// serverStream implements a server side Stream.
type serverStream struct {
	ServerStream

	opts      options
	err       error
	ctx       context.Context
	cancel    context.CancelFunc
	conn      Conn
	sessionid int64
	codec     codec.Codec
	buf       *recvBuffer
}

func (ss *serverStream) Context() context.Context {
	return ss.ctx
}

func (ss *serverStream) SendMsg(m interface{}) (err error) {
	head := &PackHeader{
		Ptype:     PackType_SRSP,
		Sessionid: ss.sessionid,
	}

	return ss.conn.send(head, m, ss.opts.maxSendMessageSize)
}

func (ss *serverStream) RecvMsg(m interface{}) (err error) {
	defer func() {
		if err != nil && ss.cancel != nil {
			ss.cancel()
			ss.cancel = nil
		}
	}()

	var p *netPack
	select {
	case p = <-ss.buf.get():
		ss.buf.load()
		break
	case <-ss.ctx.Done():
		return ss.ctx.Err()
	}

	if p.head.Ptype == PackType_EOF {
		return io.EOF
	} else if p.head.Ptype == PackType_ERROR {
		return fmt.Errorf("grpcx: get error from client")
	}

	if err = ss.codec.Unmarshal(p.body, m); err != nil {
		err = fmt.Errorf("grpcx: failed to unmarshal the received message %v", err)
		xlog.Error(err)
		return
	}

	return nil
}

func (ss *serverStream) onMessage(conn Conn, head *PackHeader, body []byte) (err error) {
	if ss.conn != conn {
		err = fmt.Errorf("grpcx: serverStream::onMessage unknow error:conn mix up")
		xlog.Error(err)

		ss.buf.put(&netPack{
			head: head,
			body: []byte(err.Error()),
		})
		return
	}

	ss.buf.put(&netPack{
		head: head,
		body: body,
	})

	return nil
}
