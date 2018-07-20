// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"sync"

	xlog "google.golang.org/grpc/grpclog"
)

func newDispatcher(maxConcurrentTask int) *taskDispatcher {
	ret := &taskDispatcher{
		idchan:            make(map[int64]*task),
		maxConcurrentTask: maxConcurrentTask,
	}
	return ret
}

type task struct {
	handlers []func() error
}

type waitTask struct {
	handler func() error
	chid    int64
}

type taskDispatcher struct {
	mux               sync.Mutex
	globalChan        []func() error
	idGlobalWait      []waitTask
	idchan            map[int64]*task
	concurrentTask    int
	maxConcurrentTask int
	drainDone         chan struct{}
	drain             bool
}

func (d *taskDispatcher) graceClose() <-chan struct{} {
	d.mux.Lock()
	defer d.mux.Unlock()
	if d.drain {
		return d.drainDone
	}

	d.drain = true
	d.drainDone = make(chan struct{})
	if d.concurrentTask == 0 {
		close(d.drainDone)
	}
	return d.drainDone
}

func (d *taskDispatcher) dispatch(chid int64, handler func() error) (err error) {
	d.mux.Lock()
	defer d.mux.Unlock()

	if d.drain {
		return errDraining
	}

	var golbalRunner func(handler func() error)
	var idRunner func(chid int64, handlers []func() error)
	var dgwaitTask func()

	handleWrapper := func(handler func() error) {
		defer func() {
			if r := recover(); r != nil {
				xlog.Warningf("grpcx: taskDispatcher handler crash:%v", r)
			}
		}()

		handler()
	}

	dgwaitTask = func() {
		if d.concurrentTask >= d.maxConcurrentTask {
			return
		}

		gwaitLen := len(d.idGlobalWait)
		glen := len(d.globalChan)
		widx := 0
		idx := 0
		for idx < glen || widx < gwaitLen {
			if widx < gwaitLen {
				t := d.idGlobalWait[widx]
				//channel already running, append to buffer
				if idt, ok := d.idchan[chid]; ok {
					idt.handlers = append(idt.handlers, t.handler)
				} else { //create new routine to run the handler
					d.concurrentTask++
					d.idchan[t.chid] = &task{}
					handlers := []func() error{t.handler}
					go idRunner(chid, handlers)
				}
				widx++
				if d.concurrentTask >= d.maxConcurrentTask {
					d.globalChan = d.globalChan[idx:]
					d.idGlobalWait = d.idGlobalWait[widx:]
					break
				}
			}

			//check global task
			if idx < glen {
				d.concurrentTask++
				go golbalRunner(d.globalChan[idx])
				idx++
				if d.concurrentTask >= d.maxConcurrentTask {
					d.globalChan = d.globalChan[idx:]
					d.idGlobalWait = d.idGlobalWait[widx:]
					break
				}
			}
		}
	}

	//task out of channel
	golbalRunner = func(handler func() error) {
		handleWrapper(handler)

		d.mux.Lock()
		defer d.mux.Unlock()

		d.concurrentTask--
		dgwaitTask()

		if d.drain && d.concurrentTask == 0 {
			close(d.drainDone)
		}
	}

	//task in channel
	idRunner = func(chid int64, handlers []func() error) {
		for {
			for _, handler := range handlers {
				handleWrapper(handler)
			}

			d.mux.Lock()
			if t, ok := d.idchan[chid]; ok {
				if len(t.handlers) > 0 {
					handlers = make([]func() error, len(t.handlers))
					copy(handlers[:], t.handlers[:])
					t.handlers = t.handlers[:0]
					d.mux.Unlock()
					continue
				}
				delete(d.idchan, chid)
			}
			break
		}

		d.concurrentTask--
		dgwaitTask()
		if d.drain && d.concurrentTask == 0 {
			close(d.drainDone)
		}
		d.mux.Unlock()
	}

	if chid == 0 {
		if len(d.globalChan) > 0 || d.concurrentTask >= d.maxConcurrentTask {
			d.globalChan = append(d.globalChan, handler)
			return
		}

		d.concurrentTask++
		go golbalRunner(handler)
		return
	}

	if t, ok := d.idchan[chid]; ok {
		t.handlers = append(t.handlers, handler)
		return
	}

	if d.concurrentTask >= d.maxConcurrentTask {
		d.idGlobalWait = append(d.idGlobalWait, waitTask{
			handler: handler,
			chid:    chid,
		})
		return
	}

	d.concurrentTask++
	d.idchan[chid] = &task{}
	handlers := []func() error{handler}
	go idRunner(chid, handlers)
	return
}
