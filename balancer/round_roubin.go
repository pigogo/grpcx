// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package balancer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pigogo/grpcx"
	"github.com/pigogo/grpcx/discover"
	xlog "github.com/pigogo/grpcx/grpclog"
)

// NewRoundRobinBalancer create a round roubin balancer
func NewRoundRobinBalancer(resolver discover.ResolverAPI) grpcx.Balancer {
	rr := &roundRoubin{
		resolver:   resolver,
		addrNotify: make(chan []string, 1),
	}
	return rr
}

type roundRoubin struct {
	resolver   discover.ResolverAPI
	sinfo      []*serviceInfo
	config     grpcx.BalancerConfig
	mux        sync.RWMutex
	next       uint32
	notifier   <-chan []*discover.NotifyInfo
	addrNotify chan []string
	unsub      func()
	stopCh     chan struct{}
	waitCh     chan struct{}
}

// Start does the initialization work to bootstrap a Balancer. For example,
// this function may start the name resolution and watch the updates. It will
// be called when dialing.
func (rr *roundRoubin) Start(target string, config grpcx.BalancerConfig) (err error) {
	rr.mux.Lock()
	defer rr.mux.Unlock()
	if rr.stopCh != nil {
		return fmt.Errorf("grpcx: roundRoubin start already")
	}

	rr.config = config
	rr.notifier, rr.unsub, err = rr.resolver.SubService(target)
	if err != nil {
		return
	}

	rr.stopCh = make(chan struct{})
	go rr.watch()
	return
}

func (rr *roundRoubin) Up(addr string) (down func(error)) {
	rr.mux.Lock()
	defer func() {
		rr.mux.Unlock()
	}()

	down = func(err error) {
		xlog.Errorf("grpcx: roundRoubin addr:%v out of service dueto:%v", addr, err)
		rr.mux.Lock()
		defer rr.mux.Unlock()
		for _, sinfo := range rr.sinfo {
			if sinfo.addr == addr {
				sinfo.connected = false
				return
			}
		}
	}

	for _, sinfo := range rr.sinfo {
		if sinfo.addr == addr {
			sinfo.connected = true
			// Get a new connected addr, notify waiter
			if rr.waitCh != nil {
				close(rr.waitCh)
				rr.waitCh = nil
			}
			return
		}
	}
	return nil
}

func (rr *roundRoubin) Get(ctx context.Context, opts grpcx.BalancerGetOptions) (addr string, put func(), err error) {
	for {
		rr.mux.Lock()
		if rr.next >= uint32(len(rr.sinfo)) {
			rr.next = 0
		}

		next := rr.next
		for next != rr.next {
			if rr.sinfo[next].connected {
				rr.next++
				addr = rr.sinfo[next].addr
				rr.mux.Unlock()
				return
			}
		}

		if !opts.BlockingWait {
			defer rr.mux.Unlock()
			if len(rr.sinfo) == 0 {
				err = errNoService
				return
			}
			addr = rr.sinfo[rr.next].addr
			rr.next++
			return
		}

		var waitCh chan struct{}
		if rr.waitCh != nil {
			waitCh = rr.waitCh
		} else {
			waitCh = make(chan struct{})
			rr.waitCh = waitCh
		}
		rr.mux.Unlock()
	waitLoop:
		for {
			select {
			case <-rr.stopCh:
				err = grpcx.ErrClientConnClosing //fmt.Errorf("grpcx: roundRound balancer closed")
				return
			case <-ctx.Done():
				err = fmt.Errorf("grpcx: roundRound get addr timeout:%v", ctx.Err())
				return
			case <-waitCh:
				break waitLoop
			}
		}
	}
}

func (rr *roundRoubin) Notify() <-chan []string {
	rr.mux.RLock()
	defer rr.mux.RUnlock()
	return rr.addrNotify
}

// Close shuts down the balancer.
func (rr *roundRoubin) Close() (err error) {
	rr.mux.Lock()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("grpcx: roudRoubin close recover:%v", r)
		}
		rr.mux.Unlock()
	}()
	close(rr.stopCh)
	rr.unsub()
	return nil
}

func (rr *roundRoubin) watch() {
	for notifys := range rr.notifier {
		var newAddrs []string
		for _, notify := range notifys {
			xlog.Infof("grpcx: roundRoubin get notification from resolver at key:%v version:%v val:%v", notify.Key, notify.LastVersion, notify.Val)
			newAddrs = append(newAddrs, notify.Key)
		}

		//renew the addr list
		rr.mux.Lock()
		var sinfos []*serviceInfo
		for _, addr := range newAddrs {
			bexist := false
			for _, sinfo := range rr.sinfo {
				if strings.Compare(sinfo.addr, addr) == 0 {
					bexist = true
					sinfos = append(sinfos, sinfo)
					break
				}
			}

			if !bexist {
				sinfos = append(sinfos, &serviceInfo{
					addr: addr,
				})
			}
		}
		rr.sinfo = sinfos
		rr.mux.Unlock()

		// publish the notify
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		select {
		case <-rr.stopCh:
		case <-ctx.Done():
			xlog.Errorf("grpcx: roundRoubin publish addr notify 30s timeout")
		case rr.addrNotify <- newAddrs:
		}
		cancel()
	}
}
