// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"bytes"
	"reflect"
	"sync"
)

// putBuffer is an unbounded channel of outPack structs.
type outPack struct {
	mtype reflect.Type
	body  *bytes.Buffer
}

type putBuffer struct {
	c       chan *outPack
	mu      sync.Mutex
	backlog []*outPack
}

func newPutBuffer() *putBuffer {
	b := &putBuffer{
		c: make(chan *outPack, 1),
	}
	return b
}

func (b *putBuffer) put(r *outPack) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.backlog) == 0 {
		select {
		case b.c <- r:
			return
		default:
		}
	}
	b.backlog = append(b.backlog, r)
}

func (b *putBuffer) load() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.backlog) > 0 {
		select {
		case b.c <- b.backlog[0]:
			b.backlog[0] = nil
			b.backlog = b.backlog[1:]
		default:
		}
	}
}

// get returns the channel that put a outPack in the buffer.
//
// Upon receipt of a outPack, the caller should call load to send another
// outPack onto the channel if there is any.
func (b *putBuffer) get() <-chan *outPack {
	return b.c
}
