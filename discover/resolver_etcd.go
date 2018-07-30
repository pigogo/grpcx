// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package discover

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	xlog "github.com/pigogo/grpcx/grpclog"
)

// NewEtcdResolver used to create a etcd service discover
func NewEtcdResolver(endpoints []string, timeout time.Duration) (ResolverAPI, error) {
	return newEtcdResolver(endpoints, &store.Config{
		ConnectionTimeout: timeout,
	})
}

// NewEtcdResolverPasswordAuth used to create a etcd service discover with username and password as auth token
func NewEtcdResolverPasswordAuth(endpoints []string, timeout time.Duration, uname, pwd string) (ResolverAPI, error) {
	return newEtcdResolver(endpoints, &store.Config{
		ConnectionTimeout: timeout,
		Username:          uname,
		Password:          pwd,
	})
}

// NewEtcdResolverTLSAuth used to create a etcd service discover with tls
func NewEtcdResolverTLSAuth(endpoints []string, timeout time.Duration, tlscfg *tls.Config, certfile, keyfile, cacertfile string) (ResolverAPI, error) {
	return newEtcdResolver(endpoints, &store.Config{
		ConnectionTimeout: timeout,
		TLS:               tlscfg,
		ClientTLS: &store.ClientTLSConfig{
			CertFile:   certfile,
			CACertFile: cacertfile,
			KeyFile:    keyfile,
		},
	})
}

// NewEtcdResolver used to create a etcd service discover
func newEtcdResolver(endpoints []string, cfg *store.Config) (ResolverAPI, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("grpcx: empty endpoints")
	}

	for _, endpoint := range endpoints {
		if _, err := net.ResolveTCPAddr("tcp4", endpoint); err != nil {
			return nil, fmt.Errorf("grpcx: invalid Resolver endpoints:%v", err)
		}
	}

	if cfg.ConnectionTimeout < time.Second {
		cfg.ConnectionTimeout = time.Second
	}

	return &etcdResolver{
		endpoints: endpoints,
		config:    cfg,
		watchs:    make(map[string]notifiers),
	}, nil
}

type etcdResolver struct {
	driver    store.Store
	endpoints []string
	config    *store.Config
	watchs    map[string]notifiers
	mux       sync.RWMutex
	stopCh    chan struct{}
}

func (er *etcdResolver) Start() (err error) {
	er.mux.Lock()
	defer er.mux.Unlock()

	if er.stopCh != nil {
		err = fmt.Errorf("grpcx: etcdResolver already stared")
		xlog.Warningf("%v", err)
		return
	}

	er.driver, err = libkv.NewStore(store.ETCD, er.endpoints, er.config)
	if err != nil {
		er.stopCh = make(chan struct{})
	}
	return
}

func (er *etcdResolver) Stop() {
	er.mux.Lock()
	defer func() {
		if r := recover(); r != nil {
			xlog.Errorf("grpcx: etcdResolver recover:%v", r)
		}
		er.mux.Unlock()
	}()

	close(er.stopCh)
	er.driver.Close()

	for _, notifier := range er.watchs {
		for _, watcher := range notifier.watchers {
			close(watcher)
		}
		close(notifier.stopCh)
	}
	er.watchs = make(map[string]notifiers)
}

func (er *etcdResolver) SubService(spath string) (<-chan []*NotifyInfo, func(), error) {
	er.mux.Lock()
	defer er.mux.Unlock()

	if er.stopCh != nil {
		select {
		case <-er.stopCh:
			return nil, nil, fmt.Errorf("grpcx: etcdResolver already closed or unstart yet")
		default:
		}
	} else {
		return nil, nil, fmt.Errorf("grpcx: etcdResolver already closed or unstart yet")
	}

	var watchers *notifiers
	var gotifyCh = make(chan []*NotifyInfo)
	var unsub = func() {
		er.mux.Lock()
		defer er.mux.Unlock()

		wlen := len(watchers.watchers)
		for i, watcher := range watchers.watchers {
			if watcher == gotifyCh {
				watchers.watchers[i] = watchers.watchers[wlen-1]
				watchers.watchers = watchers.watchers[:wlen-1]
				//no watcher, remove it
				if wlen-1 == 0 {
					close(watchers.stopCh)
					delete(er.watchs, spath)
				}
			}
		}
	}

	gotify := func() {
		retryDelay := time.Duration(0)
		for {
			notifyCh, err := er.driver.WatchTree(spath, watchers.stopCh)
			if err != nil {
				xlog.Errorf("grpcx: etcd WatchTree fail:%v", err)
				select {
				case <-er.stopCh:
					return
				default:
					if retryDelay == 0 {
						retryDelay = time.Millisecond * 10
					} else {
						retryDelay *= 2
						if retryDelay > maxRetryDelay {
							retryDelay = maxRetryDelay
						}
					}

					ctx, cancel := context.WithTimeout(context.Background(), retryDelay)
					select {
					case <-er.stopCh:
						cancel()
						return
					case <-ctx.Done():
					}
					cancel()
					continue
				}
			} else {
				retryDelay = 0
			}

			for notifys := range notifyCh {
				var ninfos []*NotifyInfo
				for _, notify := range notifys {
					ninfos = append(ninfos, &NotifyInfo{
						Key:         notify.Key,
						Val:         string(notify.Value),
						LastVersion: notify.LastIndex,
					})
				}

				er.mux.RLock()
				for _, watcher := range watchers.watchers {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
					select {
					case watcher <- ninfos:
						break
					case <-er.stopCh:
						er.mux.RUnlock()
						cancel()
						return
					case <-ctx.Done():
						xlog.Warningf("grpcx: etcd update discard because of notify chan ful and put timeout:%v", ninfos)
					}
					cancel()
				}
				er.mux.RUnlock()
			}
		}
	}

	go gotify()
	return gotifyCh, unsub, nil
}
