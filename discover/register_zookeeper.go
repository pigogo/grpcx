// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package discover

import (
	"fmt"
	"net"
	"path"
	"strings"
	"time"

	"github.com/docker/libkv/store"
	xlog "github.com/pigogo/grpcx/grpclog"
	zk "github.com/samuel/go-zookeeper/zk"
)

type zookeeperNode struct {
	endpoints      []string
	client         *zk.Conn
	zkEvent        <-chan zk.Event
	stopCh         chan struct{}
	sessionTimeout time.Duration
	nodePath       string
	targetValue    string
}

// NewZookeeperNode create a zk node for service discovery
// basePath is the parent path of the service; all the service should group by the basePath
// sname os the service name
// target will be the key of the node which should layout as "ip:port" or "schema://ip:port"; the node path is "basePath/sname/target"
// targetValue should contain the meta data like hid, groupid etd.
// endpoints is the etcd server addr
// sessionTimeout indicate the node's ttl
func NewZookeeperNode(basePath, sname, targetValue, target string, endpoints []string, sessionTimeout time.Duration) (_ RegisterAPI, err error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("grpcx: empty endpoints")
	}

	for _, endpoint := range endpoints {
		if _, err := net.ResolveTCPAddr("tcp4", endpoint); err != nil {
			return nil, fmt.Errorf("grpcx: invalid Resolver endpoints:%v", err)
		}
	}

	s := &zookeeperNode{
		endpoints:      endpoints,
		stopCh:         make(chan struct{}),
		sessionTimeout: sessionTimeout,
		targetValue:    targetValue,
	}

	s.client, s.zkEvent, err = zk.Connect(endpoints, sessionTimeout)
	if err != nil {
		s = nil
		return
	}

	basePath = store.Normalize(basePath)
	sname = store.Normalize(sname)
	target = store.Normalize(target)
	if strings.Contains(sname, "/") {
		xlog.Fatalf("grpcx: invalid service name:%v", sname)
	}
	// create base path fail
	if _, err := s.client.Create(basePath, nil, 0, []zk.ACL{zk.ACL{Perms: zk.PermAll}}); err != nil && err != zk.ErrNodeExists {
		xlog.Fatalf("grpcx: create node base path fail:%v", err)
	}

	spath := path.Join(basePath, sname)
	// create service path fail
	if _, err := s.client.Create(spath, nil, 0, []zk.ACL{zk.ACL{Perms: zk.PermAll}}); err != nil && err != zk.ErrNodeExists {
		xlog.Fatalf("grpcx: create node service path fail:%v", err)
	}

	nodePath := path.Join(spath, target)
	s.nodePath = nodePath
	go s.eventLoop()
	return s, nil
}

func (s *zookeeperNode) eventLoop() {
	tickPeriod := time.Second * 3
	if tickPeriod < s.sessionTimeout/2 {
		tickPeriod = s.sessionTimeout / 2
	}
	tick := time.NewTicker(tickPeriod)
	defer tick.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case event, ok := <-s.zkEvent:
			if !ok {
				//zk closed
				xlog.Errorf("grpcx: zookeeper client be closed")
				return
			}

			if event.Type != zk.EventSession {
				continue
			}

			if event.State == zk.StateConnected {
				//reconnected: recreate node
				_, err := s.client.Create(s.nodePath, []byte(s.targetValue), zk.FlagEphemeral, []zk.ACL{zk.ACL{Perms: zk.PermAll}})
				if err != nil {
					xlog.Errorf("grpcx: create node fail:%v", err)
					continue
				}
			} else {
				xlog.Warningf("grpcx: get zookeeperNode session state:%v", event.State.String())
			}
		case <-tick.C:
			if s.client.State() != zk.StateConnected {
				continue
			}

			exists, stat, err := s.client.Exists(s.nodePath)
			if exists && err == nil {
				// not a ephemeral node, critical error
				if stat.EphemeralOwner != 0 {
					xlog.Errorf("grpcx: zookeeper node is not a ephermal node:%v", s.nodePath)
				}
				continue
			}

			xlog.Errorf("grpcx: zk node not exist recreate it")
			_, err = s.client.Create(s.nodePath, []byte(s.targetValue), zk.FlagEphemeral, []zk.ACL{zk.ACL{Perms: zk.PermAll}})
			if err != nil {
				xlog.Errorf("grpcx: create node fail:%v", err)
				continue
			}
		}
	}
}

func (s *zookeeperNode) Stop() {
	defer func() {
		recover()
	}()

	close(s.stopCh)
}
