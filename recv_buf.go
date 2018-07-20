// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"sync"
)

// recvBuffer is an unbounded channel of recvMsg structs.
type netPack struct {
	head *PackHeader
	body []byte
}

type recvBuffer struct {
	c       chan *netPack
	mu      sync.Mutex
	backlog []*netPack
}

func newRecvBuffer() *recvBuffer {
	b := &recvBuffer{
		c: make(chan *netPack, 1),
	}
	return b
}

func (b *recvBuffer) put(r *netPack) {
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

func (b *recvBuffer) load() {
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

// get returns the channel that receives a recvMsg in the buffer.
//
// Upon receipt of a recvMsg, the caller should call load to send another
// recvMsg onto the channel if there is any.
func (b *recvBuffer) get() <-chan *netPack {
	return b.c
}
