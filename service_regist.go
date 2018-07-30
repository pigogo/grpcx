// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/context"
	xlog "github.com/pigogo/grpcx/grpclog"
)

type unaryMethodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor UnaryServerInterceptor) (interface{}, error)
type onewayMethodHandler func(ctx context.Context, args interface{}) error
type rawMethodHandler func(ctx context.Context, args interface{}, reply interface{}) error

type handlerDesc struct {
	receiver  reflect.Value
	fhandler  reflect.Value
	withMeta  bool
	argsType  reflect.Type
	replyType reflect.Type
}

// MethodInfo contains the information of an RPC including its method name and type.
type MethodInfo struct {
	// Name is the method name only, without the service name or package name.
	Name string
	// IsClientStream indicates whether the RPC is a client streaming RPC.
	IsClientStream bool
	// IsServerStream indicates whether the RPC is a server streaming RPC.
	IsServerStream bool
}

// ServiceInfo contains unary RPC method info, streaming RPC method info and metadata for a service.
type ServiceInfo struct {
	Methods []MethodInfo
	// Metadata is the metadata specified in ServiceDesc when registering service.
	Metadata interface{}
}

// MethodDesc represents an RPC service's method specification.
type MethodDesc struct {
	MethodName string
	Handler    interface{}
}

// ServiceDesc represents an RPC service's specification.
type ServiceDesc struct {
	ServiceName string
	// The pointer to the service interface. Used to check whether the user
	// provided implementation satisfies the interface requirements.
	HandlerType interface{}
	Methods     []MethodDesc
	Streams     []StreamDesc
	Metadata    interface{}
}

// service consists of the information of the server serving this service and
// the methods in this service.
type service struct {
	server interface{} // the server for service methods
	md     map[string]*MethodDesc
	sd     map[string]*StreamDesc
	mdata  interface{}
}

func exportedCheck(t reflect.Type) bool {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	rune, size := utf8.DecodeRuneInString(t.Name())
	if size != 1 || !unicode.IsUpper(rune) || t.PkgPath() == "" {
		return false
	}
	return true
}

func parseServiceName(name string) (srvname, method string, err error) {
	if name[0] == '/' {
		name = name[1:]
	}
	pos := strings.LastIndex(name, "/")
	if pos == -1 {
		err = fmt.Errorf("invalid regist name: %q; should be \"servicename/method\" format", name)
		return
	}

	srvname = name[:pos]
	method = name[pos+1:]
	return
}

// RegisterService registers a service and its implementation to the gRPC
// server. It is called from the IDL generated code. This must be called before
// invoking Serve.
func (s *Server) RegisterService(sd *ServiceDesc, ss interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ht := reflect.TypeOf(sd.HandlerType).Elem()
	st := reflect.TypeOf(ss)
	if !st.Implements(ht) {
		xlog.Fatalf("grpcx: Server.RegisterService found the handler of type %v that does not satisfy %v", st, ht)
	}
	s.register(sd, ss)
}

func (s *Server) register(sd *ServiceDesc, ss interface{}) {
	if s.serve {
		xlog.Fatalf("grpcx: Server.RegisterService after Server.Serve for %q", sd.ServiceName)
	}
	if _, ok := s.m[sd.ServiceName]; ok {
		xlog.Fatalf("grpcx: Server.RegisterService found duplicate service registration for %q", sd.ServiceName)
	}
	srv := &service{
		server: ss,
		md:     make(map[string]*MethodDesc),
		sd:     make(map[string]*StreamDesc),
		mdata:  sd.Metadata,
	}
	for i := range sd.Methods {
		d := &sd.Methods[i]
		srv.md[d.MethodName] = d
	}
	for i := range sd.Streams {
		d := &sd.Streams[i]
		srv.sd[d.StreamName] = d
	}
	s.m[sd.ServiceName] = srv
}

// RegistHandler used to regist a handler
// serviceName: must in the format "/{service}/{methord}"
// withMeta: if business layer need to get the metadata, set this and metadata will taken be context
func (s *Server) RegistHandler(serviceName string, handler interface{}, withMeta ...bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	hval := reflect.ValueOf(handler)
	if hval.Kind() != reflect.Func {
		err := fmt.Errorf("grpcx: invalid handler: not a function")
		xlog.Error(err)
		return err
	}

	htype := hval.Type()
	if err := s.registType(serviceName, htype, hval, reflect.Zero(htype), withMeta...); err != nil {
		xlog.Error(err)
		return err
	}
	return nil
}

// RegistStruct used to regist group handler group by struct
// erviceName: only taken the name of the service but no method and slash"
// struc is the value of the struct, must be pointer
func (s *Server) RegistStruct(serviceName string, struc interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	hval := reflect.ValueOf(struc)
	if hval.Kind() != reflect.Ptr || hval.Elem().Kind() != reflect.Struct {
		err := fmt.Errorf("grpcx: invalid handler: not a structure pointer")
		xlog.Error(err)
		return err
	}

	htype := hval.Type()
	nmethord := htype.NumMethod()
	registed := 0
	for i := 0; i < nmethord; i++ {
		methord := htype.Method(i)
		sname := fmt.Sprintf("%v/%v", serviceName, methord.Name)
		if s.registType(sname, methord.Type, methord.Func, hval) == nil {
			registed++
		}
	}

	if registed == 0 {
		err := fmt.Errorf("grpcx: no valid struct, no method be registed")
		xlog.Error(err)
		return err
	}
	return nil
}

func (s *Server) registType(serviceName string, htype reflect.Type, hval reflect.Value, receiver reflect.Value, withMeta ...bool) error {
	if s.serve {
		xlog.Fatalf("grpcx: RawRegister after Server.Serve for %q", serviceName)
	}

	if htype.NumOut() != 1 {
		return fmt.Errorf("grpcx: invalid handler: number out overflow, showld be 1 parameters")
	}

	otype := htype.Out(0)
	if reflect.TypeOf((*error)(nil)).Elem() != otype {
		return fmt.Errorf("grpcx: invalid handler: out should be an error")
	}
	offset := 0
	if !receiver.IsNil() {
		offset = 1
	}

	if htype.NumIn() < 2+offset || htype.NumIn() > 3+offset {
		return fmt.Errorf("grpcx: invalid handler: number in overflow, showld be 2 or 3 parameters")
	}

	ctxType := htype.In(offset)
	if ctxType.Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) == false {
		return fmt.Errorf("grpcx: invalid handler: first argument should be context.Context")
	}

	argType := htype.In(1 + offset)
	if !exportedCheck(argType) {
		return fmt.Errorf("grpcx: invalid handler: second argument should be exported")
	}

	//should be a struct to serialize binary data
	for {
		if argType.Kind() == reflect.Ptr {
			if argType.Elem().Kind() != reflect.Struct {
				return fmt.Errorf("grpcx: invalid handler: second argument should be a struct")
			}
			break
		}

		if argType.Kind() != reflect.Struct {
			return fmt.Errorf("grpcx: invalid handler: second argument should be a struct")
		}
		break
	}

	var replyType reflect.Type
	if htype.NumIn() == 3+offset {
		replyType = htype.In(2 + offset)
		if replyType.Kind() != reflect.Ptr {
			return fmt.Errorf("grpcx: invalid handler: reply argument should be a pointer")
		}

		if !exportedCheck(replyType) {
			return fmt.Errorf("grpcx: invalid handler: reply argument should be exported")
		}

		if replyType.Elem().Kind() != reflect.Struct {
			return fmt.Errorf("grpcx: invalid handler: reply argument should be a struct")
		}
		initTypePool(replyType)
	}

	srvname, method, err := parseServiceName(serviceName)
	if err != nil {
		return err
	}

	srv := s.m[srvname]
	if srv == nil {
		srv = &service{
			md: make(map[string]*MethodDesc),
			sd: make(map[string]*StreamDesc),
		}
		s.m[srvname] = srv
	}

	if _, ok := srv.md[method]; ok {
		return fmt.Errorf("grpcx: invalid handler: service already registed:%q", serviceName)
	}

	md := &MethodDesc{
		MethodName: htype.Name(),
		Handler: &handlerDesc{
			fhandler:  hval,
			argsType:  argType,
			replyType: replyType,
			receiver:  receiver,
			withMeta:  bool(len(withMeta) > 0),
		},
	}

	srv.md[method] = md
	initTypePool(argType)
	return nil
}
