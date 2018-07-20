// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"bytes"
	"reflect"
	"sync"
)

var typePool = make(map[reflect.Type]*sync.Pool)
var bufPool = make(map[reflect.Type]*sync.Pool)
var plock sync.RWMutex

func initTypePool(t reflect.Type) {
	typePool[t] = &sync.Pool{
		New: func() interface{} {
			if t.Kind() == reflect.Ptr {
				return reflect.New(t.Elem()).Interface()
			}
			return reflect.New(t).Interface()
		},
	}

	bufPool[t] = &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
}

func getTypeFromPool(t reflect.Type) interface{} {
	if p, ok := typePool[t]; ok {
		return p.Get()
	}

	if t.Kind() == reflect.Ptr {
		return reflect.New(t.Elem()).Interface()
	}
	return reflect.New(t).Interface()
}

func putTypeToPool(t reflect.Type, v interface{}) {
	if p, ok := typePool[t]; ok {
		p.Put(v)
	}
}

func getBufferFromPool(t reflect.Type) *bytes.Buffer {
	plock.RLock()
	if p, ok := bufPool[t]; ok {
		plock.RUnlock()
		return p.Get().(*bytes.Buffer)
	}

	plock.RUnlock()
	plock.Lock()
	if p, ok := bufPool[t]; ok {
		plock.Unlock()
		return p.Get().(*bytes.Buffer)
	}

	bufPool[t] = &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
	plock.Unlock()

	return new(bytes.Buffer)
}

func putBufferToPool(t reflect.Type, buf *bytes.Buffer) {
	buf.Reset()
	plock.RLock()
	defer plock.RUnlock()
	if p, ok := bufPool[t]; ok {
		p.Put(buf)
		return
	}
}
