// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/juju/ratelimit"
	"github.com/pigogo/grpcx/codec"
	"golang.org/x/net/context"
	xlog "github.com/pigogo/grpcx/grpclog"
)

var (
	// ErrServerStopped indicates that the operation is now illegal because of
	// the server being stopped.
	ErrServerStopped = errors.New("grpcx: the server has been stopped")
	// ErrInvalidConnid indicates the connection is not exist index by the connid
	ErrInvalidConnid = errors.New("grpcx: connection id invalid, connection not exist")
)

type connidKey struct{}

func getConnID(ctx context.Context) int64 {
	val := ctx.Value(connidKey{})
	if cid, ok := val.(int64); ok {
		return cid
	}
	return 0
}

func withConnID(ctx context.Context, cid int64) context.Context {
	return context.WithValue(ctx, connidKey{}, cid)
}

// Server is a gRPC server to serve RPC requests.
type Server struct {
	opts options

	mu     sync.RWMutex // guards following
	serve  bool
	drain  bool
	ctx    context.Context
	cancel context.CancelFunc
	// A CondVar to let GracefulStop() blocks until all the pending RPCs are finished
	// and all the transport goes away.
	lncd       *sync.Cond
	m          map[string]*service // service name -> service info
	conns      map[int64]Conn
	connIDBase int64
	reqlimit   *ratelimit.Bucket
	dispatcher *taskDispatcher
}

// NewServer creates a gRPC server which has no service registered and has not
// started to accept requests yet.
func NewServer(opt ...ServerOption) *Server {
	opts := defaultServerOptions
	for _, o := range opt {
		o(&opts)
	}
	if opts.codec == nil {
		// Set the default codec.
		opts.codec = codec.ProtoCodec{}
	}
	s := &Server{
		opts:       opts,
		m:          make(map[string]*service),
		conns:      make(map[int64]Conn),
		dispatcher: newDispatcher(int(opts.maxConcurrentRoutine)),
		reqlimit:   ratelimit.NewBucket(time.Second, int64(opts.maxConcurrentRequest)),
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s
}

func (s *Server) waitShutdownCond() {
	s.lncd.L.Lock()
	s.lncd.Wait()
	s.lncd.L.Unlock()
}

func (s *Server) signalShutdownCond() {
	s.lncd.L.Lock()
	s.lncd.Broadcast()
	s.lncd.L.Unlock()
}

// Serve begin the service and will block up until the service finish
func (s *Server) Serve(addrs ...string) error {
	s.serve = true
	wait := sync.WaitGroup{}
	lns := []net.Listener{}
	s.lncd = sync.NewCond(&sync.Mutex{})

	for _, addr := range addrs {
		wait.Add(1)
		ln, err := net.Listen("tcp4", addr)
		if err != nil {
			return err
		}

		lns = append(lns, ln)
		go func() {
			defer wait.Done()
			for {
				conn, err := ln.Accept()
				if err != nil {
					if e, ok := err.(net.Error); ok && e.Temporary() {
						continue
					}
					s.signalShutdownCond()
					return
				}

				go func(conn net.Conn) {
					conn.(*net.TCPConn).SetKeepAlive(s.opts.keepalivePeriod > 0)
					conn.(*net.TCPConn).SetKeepAlivePeriod(s.opts.keepalivePeriod)
					conn.(*net.TCPConn).SetLinger(3)
					if s.opts.creds != nil {
						conn, _, err = s.opts.creds.ServerHandshake(conn)
						if err != nil {
							//todo: log handshake error
							xlog.Warningf("grpcx: on accept conn but handshake fail:%v", err)
						}
						conn.Close()
						return
					}

					s.mu.Lock()
					s.connIDBase++
					newConn := newAcceptConn(s.connIDBase, s.opts, conn, s.handleNetPacket, s.removeConn)
					if s.opts.connPlugin != nil {
						s.opts.connPlugin.OnPostConnect(newConn)
					}

					s.conns[s.connIDBase] = newConn
					s.mu.Unlock()
				}(conn)
			}
		}()
	}

	go func() {
		s.waitShutdownCond()
		for _, ln := range lns {
			ln.Close()
		}
	}()

	wait.Wait()
	<-s.ctx.Done()
	return nil
}

// Stop stops the gRPC server. It immediately closes all open
// connections and listeners.
// It cancels all active RPCs on the server side and the corresponding
// pending RPCs on the client side will get notified by connection
// errors.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.serve == false {
		s.mu.Unlock()
		return
	}
	conns := s.conns
	s.conns = nil
	s.serve = false
	s.mu.Unlock()

	s.signalShutdownCond()
	for _, conn := range conns {
		conn.close()
	}
	s.cancel()
}

// GracefulStop stops the gRPC server gracefully. It stops the server from
// accepting new connections and RPCs and blocks until all the pending RPCs are
// finished.
func (s *Server) GracefulStop(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			xlog.Errorf("GracefulStop exception:%v", r)
		}
	}()

	s.signalShutdownCond()
	s.mu.Lock()
	if s.serve == false || s.conns == nil || s.drain {
		s.mu.Unlock()
		return
	}

	conns := s.conns
	s.serve = false
	s.conns = nil
	s.drain = true
	s.mu.Unlock()

	//notify all peers to close
	var doneBuf []<-chan struct{}
	for _, c := range conns {
		done := c.gracefulClose()
		doneBuf = append(doneBuf, done)
	}

loop:
	for _, done := range doneBuf {
		select {
		case <-ctx.Done():
			//context timeout: force all connection close
			xlog.Warningf("graceful close wait timeout, force connection close")
			for _, c := range conns {
				c.close()
			}
			break loop
		case <-done:
			xlog.Info("graceful close done")
		}
	}

	//wait all tasks finish
	disDone := s.dispatcher.graceClose()
	select {
	case <-ctx.Done():
	case <-disDone:
	}
	s.cancel()
}

func (s *Server) removeConn(id int64) {
	s.mu.Lock()
	delete(s.conns, id)
	s.mu.Unlock()
}

// SendTo used to send data to client connection directly
// ctx must be the context get from the connection wich contain the connid
func (s *Server) SendTo(ctx context.Context, method string, m interface{}, keys ...metaKeyType) (err error) {
	cid := getConnID(ctx)
	if cid == 0 {
		return ErrInvalidConnid
	}

	s.mu.RLock()
	conn, ok := s.conns[cid]
	s.mu.RUnlock()
	if !ok {
		return ErrInvalidConnid
	}

	head := &PackHeader{
		Ptype:   PackType_Notify,
		Methord: method,
	}

	if len(keys) > 0 {
		head.Metadata = make(map[int32][]byte)
		for _, key := range keys {
			meta := ctx.Value(key)
			if meta, ok := meta.([]byte); ok {
				head.Metadata[int32(key)] = meta
			}
		}
	}

	return s.send(conn, head, m)
}

func (s *Server) send(conn Conn, head *PackHeader, m interface{}) (err error) {
	return conn.send(head, m, s.opts.maxSendMessageSize)
}

func (s *Server) sendError(conn Conn, sessionid int64, err error) error {
	head := &PackHeader{
		Sessionid: sessionid,
		Ptype:     PackType_ERROR,
	}
	return s.send(conn, head, nil)
}

func (s *Server) handleUnaryPacket(head *PackHeader, p []byte, conn Conn, hd *handlerDesc) (err error) {
	var (
		args     interface{}
		argsv    reflect.Value
		reply    interface{}
		callArgs []reflect.Value
		rctx     context.Context
	)

	args = getTypeFromPool(hd.argsType)
	argsv = reflect.ValueOf(args)
	if hd.argsType.Kind() != reflect.Ptr {
		argsv = argsv.Elem()
	}

	if err = s.opts.codec.Unmarshal(p, args); err != nil {
		xlog.Warning("grpcx: handleUnaryPacket msg unmarsh fail:%v", err)
		return err
	}
	rctx = conn.context()
	if hd.withMeta && head.Metadata != nil {
		rctx = withMetadata(rctx, head.Metadata)
	}

	if !hd.receiver.IsNil() {
		callArgs = []reflect.Value{hd.receiver, reflect.ValueOf(rctx), argsv}
	} else {
		callArgs = []reflect.Value{reflect.ValueOf(rctx), argsv}
	}

	if hd.replyType != nil {
		reply = getTypeFromPool(hd.replyType)
		callArgs = append(callArgs, reflect.ValueOf(reply))
	}

	handler := func() error {
		defer func() {
			if hd.replyType != nil {
				putTypeToPool(hd.replyType, reply)
			}
			putTypeToPool(hd.argsType, args)
		}()

		out := hd.fhandler.Call(callArgs)
		if !out[0].IsNil() {
			if e := s.sendError(conn, head.Sessionid, out[0].Interface().(error)); e != nil {
				xlog.Warningf("grpcx: Server.handleUnaryPacket failed to write error: %v", e)
			}
			return nil
		}

		if hd.replyType != nil {
			head.Ptype = PackType_RSP
			if e := s.send(conn, head, reply); e != nil {
				xlog.Warningf("grpcx: Server.handleUnaryPacket failed to write response, session id:%v: %v", head.Sessionid, e)
			}
		}

		return nil
	}
	return handler()
}

func (s *Server) handleGrpcUnaryPacket(head *PackHeader, msg []byte, conn Conn, md *MethodDesc, srv *service) (err error) {
	handleFun := md.Handler.(func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor UnaryServerInterceptor) (interface{}, error))
	df := func(v interface{}) error {
		if len(msg) > s.opts.maxReceiveMessageSize {
			return fmt.Errorf("grpcx: received message larger than max (%d vs. %d)", len(msg), s.opts.maxReceiveMessageSize)
		}
		if err := s.opts.codec.Unmarshal(msg, v); err != nil {
			return fmt.Errorf("grpcx: error unmarshalling request: %v  msg:%v str:%v", err, msg, string(msg))
		}
		return nil
	}

	handler := func() error {
		reply, appErr := handleFun(srv.server, s.ctx, df, nil)
		if appErr != nil {
			if e := s.sendError(conn, head.Sessionid, appErr); e != nil {
				xlog.Warningf("grpcx: Server.handleGrpcUnaryPacket failed to write error: %v", e)
			}
			return appErr
		}

		//on response:empty the method to reduce netflow
		head.Methord = ""
		head.Ptype = PackType_RSP
		return s.send(conn, head, reply)
	}
	return handler()
}

func (s *Server) handleStreamInit(head *PackHeader, conn Conn, sd *StreamDesc, srv *service) {
	//make stream
	ctx, cancel := context.WithCancel(context.Background())
	ss := &serverStream{
		ctx:       ctx,
		opts:      s.opts,
		cancel:    cancel,
		sessionid: head.Sessionid,
		conn:      conn,
		codec:     s.opts.codec,
		buf:       newRecvBuffer(),
	}

	connCancel, err := conn.withSession(head.Sessionid, ss)
	if err != nil {
		s.sendError(conn, head.Sessionid, err)
		return
	}

	handler := func() error {
		//remove stream in the end
		defer func() {
			connCancel()
			cancel()
		}()

		var server interface{}
		if srv != nil {
			server = srv.server
		}
		err := sd.Handler(server, ss)
		if err != nil {
			s.sendError(conn, head.Sessionid, err)
			return err
		}

		head := &PackHeader{
			Ptype:     PackType_EOF,
			Sessionid: head.Sessionid,
		}

		if err = s.send(conn, head, nil); err != nil {
			xlog.Warningf("grpcx: Stream failed to write eof: %v", err)
		}
		return nil
	}

	s.dispatcher.dispatch(0, handler)
	return
}

func (s *Server) handleReqPacket(head *PackHeader, p []byte, conn Conn) {
	var sd *StreamDesc
	var md *MethodDesc
	var serviceInfo *service
	var ok bool

	if head.Ptype == PackType_SREQ {
		session := conn.sessionOf(head.Sessionid)
		if session != nil {
			if err := session.onMessage(conn, head, p); err != nil {
				if e := s.sendError(conn, head.Sessionid, err); e != nil {
					xlog.Warningf("grpcx: Server.handleStreamPacket failed to write error: %v", e)
				}
			}
			return
		}

		if err := s.sendError(conn, head.Sessionid, fmt.Errorf("grpcx: session not exist")); err != nil {
			xlog.Warningf("grpcx:Server.handleReqPacket failed to write error: %v", err)
		}
		return
	}

	sm := head.Methord
	if len(sm) == 0 {
		err := fmt.Errorf("grpcx: malformed method name: %q", sm)
		xlog.Infoln(err)
		if e := s.sendError(conn, head.Sessionid, err); e != nil {
			xlog.Warningf("grpcx: Server.handleReqPacket failed to write error: %v", e)
		}
	}

	if sm[0] == '/' {
		sm = sm[1:]
	}
	pos := strings.LastIndex(sm, "/")
	if pos == -1 {
		err := fmt.Errorf("grpcx: malformed method name: %q", sm)
		xlog.Infoln(err)
		if e := s.sendError(conn, head.Sessionid, err); e != nil {
			xlog.Warningf("grpcx: Server.handleReqPacket failed to write error: %v", e)
		}
		return
	}

	srvname := sm[:pos]
	method := sm[pos+1:]
	serviceInfo, ok = s.m[srvname]
	if !ok {
		err := fmt.Errorf("grpcx: unknown service %v", sm)
		xlog.Infoln(err)
		if e := s.sendError(conn, head.Sessionid, err); e != nil {
			xlog.Warningf("grpcx: Server.handleReqPacket failed to write error: %v", e)
		}
		return
	}

	if md, ok = serviceInfo.md[method]; ok {
		if hd, ok := md.Handler.(*handlerDesc); ok {
			s.handleUnaryPacket(head, p, conn, hd)
			return
		}
		s.handleGrpcUnaryPacket(head, p, conn, md, serviceInfo)
		return
	} else if sd, ok = serviceInfo.sd[method]; ok {
		if head.Ptype != PackType_SINI {
			err := fmt.Errorf("grpcx: unknown pack type:%v", head.Ptype)
			xlog.Infoln(err)
			if e := s.sendError(conn, head.Sessionid, err); e != nil {
				xlog.Warningf("grpcx: Server.handleReqPacket failed to write error: %v", e)
			}
			return
		}
		s.handleStreamInit(head, conn, sd, serviceInfo)
		return
	}

	err := fmt.Errorf("grpcx: unknown method %v", method)
	xlog.Infoln(err)
	if e := s.sendError(conn, head.Sessionid, err); e != nil {
		xlog.Warningf("grpcx: Server.handleReqPacket failed to write error: %v", e)
	}
	return
}

func (s *Server) handleEOFPacket(head *PackHeader, p []byte, conn Conn) (err error) {
	session := conn.sessionOf(head.Sessionid)
	if session == nil {
		err = fmt.Errorf("grpcx: session not exist:%v", head.Sessionid)
		if e := s.sendError(conn, head.Sessionid, err); e != nil {
			xlog.Warningf("grpcx:Server.handleEOFPacket failed to write error: %v", e)
		}
		return
	}

	if err = session.onMessage(conn, head, p); err != nil {
		xlog.Infof("grpcx: session on eof message error:%v", err)
		if e := s.sendError(conn, head.Sessionid, err); e != nil {
			xlog.Warningf("grpcx: Server.handleEOFPacket failed to write error: %v", e)
		}
	}
	return
}

func (s *Server) handleErrorPacket(head *PackHeader, p []byte, conn Conn) {
	session := conn.sessionOf(head.Sessionid)
	if session != nil {
		session.onMessage(conn, head, p)
	}
}

func (s *Server) handleNetPacket(head *PackHeader, p []byte, conn Conn) (err error) {
	//rate control
	s.reqlimit.WaitMaxDuration(1, time.Millisecond*10)
	//stream request must keep in order
	if head.Ptype == PackType_SREQ || head.Ptype == PackType_SINI {
		s.handleReqPacket(head, p, conn)
		return
	}

	handler := func() error {
		switch head.Ptype {
		case PackType_REQ:
			s.handleReqPacket(head, p, conn)
			break
		case PackType_EOF:
			s.handleEOFPacket(head, p, conn)
			break
		case PackType_ERROR:
			s.handleErrorPacket(head, p, conn)
			break
		case PackType_GoAway:
			// unsupport
		default:
			err = fmt.Errorf("grpcx: unknow packtype:%v", head.Ptype)
			xlog.Info(err)
			if e := s.sendError(conn, head.Sessionid, err); e != nil {
				xlog.Warningf("grpcx: handleNetPacket fail to send error packet:%v", e)
			}
		}
		return nil
	}
	token := getServiceToken(head.Metadata)
	s.dispatcher.dispatch(token, handler)
	return
}
