// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pigogo/grpcx/codec"
	"golang.org/x/net/context"
	xlog "google.golang.org/grpc/grpclog"
)

type notifyDesc struct {
	receiver   reflect.Value
	fhandler   reflect.Value
	withMeta   bool
	notifyType reflect.Type
}

type notifyMethord map[string]*notifyDesc

var (
	mnotify = make(map[string]notifyMethord)
)

// RegistNotifyHandler used to regist a notify handler
// serviceName: must in the format "/{service}/{methord}"
// withMeta: if business layer need to get the metadata, set this and metadata will taken be context
// not goroutine safe: should be call before serving, otherwise may cause crash
func RegistNotifyHandler(serviceName string, handler interface{}, withMeta ...bool) error {
	hval := reflect.ValueOf(handler)
	if hval.Kind() != reflect.Func {
		err := fmt.Errorf("grpcx: invalid handler: not a function")
		xlog.Error(err)
		return err
	}

	htype := hval.Type()
	if err := registType(serviceName, htype, hval, reflect.Zero(htype), withMeta...); err != nil {
		xlog.Error(err)
		return err
	}
	return nil
}

// RegistNotifyStruct used to regist group notify handler group by struct
// erviceName: only taken the name of the service but no method and slash"
// struc is the value of the struct, must be pointer
// not goroutine safe: should be call before serving, otherwise may cause crash
func RegistNotifyStruct(serviceName string, struc interface{}) error {
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
		if registType(sname, methord.Type, methord.Func, hval) == nil {
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

func registType(serviceName string, htype reflect.Type, hval reflect.Value, receiver reflect.Value, withMeta ...bool) error {
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

	if htype.NumIn() != 2+offset {
		return fmt.Errorf("grpcx: invalid handler: number in overflow, showld be 2 parameters")
	}

	ctxType := htype.In(offset)
	if ctxType.Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) == false {
		return fmt.Errorf("grpcx: invalid handler: first argument should be context.Context")
	}

	var notifyType reflect.Type
	notifyType = htype.In(1 + offset)
	if notifyType.Kind() != reflect.Ptr {
		return fmt.Errorf("grpcx: invalid handler: notifyType argument should be a pointer")
	}

	if !exportedCheck(notifyType) {
		return fmt.Errorf("grpcx: invalid handler: notifyType argument should be exported")
	}

	if notifyType.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("grpcx: invalid handler: notifyType argument should be a struct")
	}
	initTypePool(notifyType)

	srvname, method, err := parseServiceName(serviceName)
	if err != nil {
		return err
	}

	notifys := mnotify[srvname]
	if notifys == nil {
		notifys = make(notifyMethord)
		mnotify[srvname] = notifys
	}

	if _, ok := notifys[method]; ok {
		return fmt.Errorf("grpcx: invalid handler: service already registed:%q", serviceName)
	}

	notifys[method] = &notifyDesc{
		fhandler:   hval,
		receiver:   receiver,
		notifyType: notifyType,
	}
	initTypePool(notifyType)
	return nil
}

func handleNotify(conn Conn, head *PackHeader, pack []byte, codec codec.Codec) {
	sm := head.Methord
	if len(sm) == 0 {
		xlog.Errorf("grpcx: handleNotify malformed method name: %q", sm)
		return
	}

	if sm[0] == '/' {
		sm = sm[1:]
	}
	pos := strings.LastIndex(sm, "/")
	if pos == -1 {
		xlog.Errorf("grpcx: handleNotify malformed method name: %q", sm)
		return
	}

	srvname := sm[:pos]
	method := sm[pos+1:]
	serviceInfo, ok := mnotify[srvname]
	if !ok {
		xlog.Errorf("grpcx: handleNotify service not exist: %q", sm)
		return
	}

	sdes, ok := serviceInfo[method]
	if !ok {
		xlog.Errorf("grpcx: handleNotify service not exist: %q", sm)
		return
	}

	ctx := conn.context()
	if sdes.withMeta && head.Metadata != nil {
		ctx = withMetadata(ctx, head.Metadata)
	}

	notifyArgs := getTypeFromPool(sdes.notifyType)
	if err := codec.Unmarshal(pack, notifyArgs); err != nil {
		xlog.Errorf("grpcx: handleNotify not a valid packet")
		return
	}

	var callArgs []reflect.Value
	if !sdes.receiver.IsNil() {
		callArgs = []reflect.Value{sdes.receiver, reflect.ValueOf(ctx), reflect.ValueOf(notifyArgs)}
	} else {
		callArgs = []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(notifyArgs)}
	}

	out := sdes.fhandler.Call(callArgs)
	if !out[0].IsNil() {
		xlog.Infof("grpcx: handleNotify callback fail: %v", out[0].Interface())
	}
}
