// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package balancer

import (
	"context"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pigogo/grpcx"
	"github.com/pigogo/grpcx/discover"
	xlog "github.com/pigogo/grpcx/grpclog"
)

type uint32s []uint32

func (us uint32s) Len() int {
	return len(us)
}

func (us uint32s) Less(i, j int) bool {
	return us[i] < us[j]
}

func (us uint32s) Swap(i, j int) {
	us[i], us[j] = us[j], us[i]
}

// NewConsistentBalancer create a consistent hash balancer
func NewConsistentBalancer(resolver discover.ResolverAPI) grpcx.Balancer {
	cb := &consistentHaser{
		resolver:   resolver,
		addrNotify: make(chan []string, 1),
		members:    make(map[string]uint32),
	}
	return cb
}

type consistentHaser struct {
	resolver      discover.ResolverAPI
	config        grpcx.BalancerConfig
	mux           sync.RWMutex
	cicleSrv      map[uint32]*serviceInfo
	count         int
	members       map[string]uint32
	sortedHashVal uint32s
	notifier      <-chan []*discover.NotifyInfo
	addrNotify    chan []string
	unsub         func()
	stopCh        chan struct{}
	waitCh        chan struct{}
}

// Start does the initialization work to bootstrap a Balancer. For example,
// this function may start the name resolution and watch the updates. It will
// be called when dialing.
func (cb *consistentHaser) Start(target string, config grpcx.BalancerConfig) (err error) {
	cb.mux.Lock()
	defer cb.mux.Unlock()
	if cb.stopCh != nil {
		return fmt.Errorf("grpcx: consistentHaser start already")
	}

	cb.config = config
	cb.notifier, cb.unsub, err = cb.resolver.SubService(target)
	if err != nil {
		return
	}

	cb.stopCh = make(chan struct{})
	go cb.watch()
	return
}

func (cb *consistentHaser) Up(addr string) (down func(error)) {
	cb.mux.Lock()
	defer func() {
		cb.mux.Unlock()
	}()

	down = func(err error) {
		xlog.Errorf("grpcx: consistentHaser addr:%v out of service dueto:%v", addr, err)
		cb.mux.Lock()
		defer cb.mux.Unlock()
		if offset, ok := cb.members[addr]; ok {
			cb.cicleSrv[offset].connected = false
			return
		}
	}

	if offset, ok := cb.members[addr]; ok {
		cb.cicleSrv[offset].connected = true
		if cb.waitCh != nil {
			close(cb.waitCh)
			cb.waitCh = nil
		}
		return
	}
	return nil
}

func (cb *consistentHaser) serach(hval uint32) int {
	cmp := func(n int) bool {
		return cb.sortedHashVal[n] > hval
	}

	offset := sort.Search(cb.count, cmp)
	if offset >= cb.count {
		return 0
	}
	return offset
}

func (cb *consistentHaser) hashKey(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

func (cb *consistentHaser) Get(ctx context.Context, opts grpcx.BalancerGetOptions) (addr string, put func(), err error) {
	hval := cb.hashKey(opts.HashStrKey)

	for {
		cb.mux.RLock()
		if cb.count == 0 {
			cb.mux.RUnlock()
			err = errNoService
			return
		}

		offset := cb.serach(hval)
		for i := offset; i != offset; {
			if cb.cicleSrv[cb.sortedHashVal[i]].connected {
				addr = cb.cicleSrv[cb.sortedHashVal[i]].addr
				cb.mux.RUnlock()
				return
			}
		}

		if !opts.BlockingWait {
			addr = cb.cicleSrv[cb.sortedHashVal[offset]].addr
			cb.mux.RUnlock()
			return
		}

		var waitCh chan struct{}
		if cb.waitCh != nil {
			waitCh = cb.waitCh
		} else {
			waitCh = make(chan struct{})
			cb.waitCh = waitCh
		}
		cb.mux.RUnlock()
	waitLoop:
		for {
			select {
			case <-cb.stopCh:
				err = grpcx.ErrClientConnClosing //fmt.Errorf("grpcx: roundRound balancer closed")
				return
			case <-ctx.Done():
				err = fmt.Errorf("grpcx: consistentHaser get addr timeout:%v", ctx.Err())
				return
			case <-waitCh:
				break waitLoop
			}
		}
	}
}

func (cb *consistentHaser) Notify() <-chan []string {
	cb.mux.RLock()
	defer cb.mux.RUnlock()
	return cb.addrNotify
}

// Close shuts down the balancer.
func (cb *consistentHaser) Close() (err error) {
	cb.mux.Lock()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("grpcx: roudRoubin close recover:%v", r)
		}
		cb.mux.Unlock()
	}()

	close(cb.stopCh)
	cb.unsub()
	cb.count = 0
	cb.members = nil
	cb.cicleSrv = nil
	cb.sortedHashVal = cb.sortedHashVal[:0]
	return nil
}

func (cb *consistentHaser) watch() {
	for notifys := range cb.notifier {
		var newAddrs []string
		for _, notify := range notifys {
			xlog.Infof("grpcx: consistentHaser get notification from resolver at key:%v version:%v val:%v", notify.Key, notify.LastVersion, notify.Val)
			newAddrs = append(newAddrs, notify.Key)
		}

		//renew the addr list
		cb.mux.Lock()
		var sinfos []*serviceInfo
		for _, addr := range newAddrs {
			bexist := false
			for _, sinfo := range cb.cicleSrv {
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
		cb.cicleSrv = make(map[uint32]*serviceInfo)
		cb.sortedHashVal = cb.sortedHashVal[:0]
		cb.members = make(map[string]uint32)
		for _, sinfo := range sinfos {
			hval := cb.hashKey(sinfo.addr)
			cb.members[sinfo.addr] = hval
			cb.cicleSrv[hval] = sinfo
			cb.sortedHashVal = append(cb.sortedHashVal, hval)
		}
		sort.Sort(cb.sortedHashVal)
		cb.count = cb.sortedHashVal.Len()
		cb.mux.Unlock()

		// publish the notify
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		select {
		case <-cb.stopCh:
		case <-ctx.Done():
			xlog.Errorf("grpcx: consistentHaser publish addr notify 30s timeout")
		case cb.addrNotify <- newAddrs:
		}
		cancel()
	}
}
