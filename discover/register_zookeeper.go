package discover

import (
	"fmt"
	"net"
	"path"
	"strings"
	"time"

	"github.com/docker/libkv/store"
	zk "github.com/samuel/go-zookeeper/zk"
	xlog "github.com/pigogo/grpcx/grpclog"
)

type zookeeperNode struct {
	endpoints      []string
	client         *zk.Conn
	zkEvent        <-chan zk.Event
	stopCh         chan struct{}
	sessionTimeout time.Duration
	basePath       string
	nodePath       string
	nodeValue      []byte
}

// NewZookeeperNode create a zk node for service discovery
// nodeValue should contain the service ip and port etc. which can descrip a service
// sessionTimeout indicate the node's ttl
func NewZookeeperNode(basePath, sname string, nodeValue []byte, endpoints []string, sessionTimeout time.Duration) (_ RegisterAPI, err error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("grpcx: empty endpoints")
	}

	for _, endpoint := range endpoints {
		if _, err := net.ResolveTCPAddr("tcp4", endpoint); err != nil {
			return nil, fmt.Errorf("grpcx: invalid Resolver endpoints:%v", err)
		}
	}

	basePath = store.Normalize(basePath)
	sname = store.Normalize(sname)
	if strings.Contains(sname, "/") {
		xlog.Fatalf("grpcx: invalid service name:%v", sname)
	}

	nodePath := path.Join(basePath, sname)
	s := &zookeeperNode{
		endpoints:      endpoints,
		stopCh:         make(chan struct{}),
		sessionTimeout: sessionTimeout,
		basePath:       basePath,
		nodePath:       nodePath,
		nodeValue:      nodeValue,
	}

	s.client, s.zkEvent, err = zk.Connect(endpoints, sessionTimeout)
	if err != nil {
		s = nil
		return
	}

	// create base path fail
	if _, err := s.client.Create(s.basePath, nil, 0, []zk.ACL{zk.ACL{Perms: zk.PermAll}}); err != nil && err != zk.ErrNodeExists {
		xlog.Fatalf("grpcx: create node base path fail:%v", err)
	}

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
				_, err := s.client.Create(s.nodePath, s.nodeValue, zk.FlagEphemeral, []zk.ACL{zk.ACL{Perms: zk.PermAll}})
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
			_, err = s.client.Create(s.nodePath, s.nodeValue, zk.FlagEphemeral, []zk.ACL{zk.ACL{Perms: zk.PermAll}})
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
