package discover

import (
	"context"
	"fmt"
	"net"
	"path"
	"strings"
	"time"

	etcd "github.com/coreos/etcd/client"
	"github.com/docker/libkv/store"
	xlog "google.golang.org/grpc/grpclog"
)

const (
	periodicSync = time.Minute * 5
)

type etcdNode struct {
	endpoints      []string
	driver         store.Store
	stopCh         chan struct{}
	sessionTimeout time.Duration
	nodePath       string
	nodeValue      string
	basePath       string
	client         etcd.KeysAPI
}

// NewEtcdNode create a etcd node for service discovery
// nodeValue should contain the service ip and port etc. which can descrip a service
// sessionTimeout indicate the node's ttl
func NewEtcdNode(basePath, sname string, nodeValue []byte, endpoints []string, sessionTimeout time.Duration) (_ RegisterAPI, err error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("grpcx: empty endpoints")
	}

	for _, endpoint := range endpoints {
		if _, err := net.ResolveTCPAddr("tcp4", endpoint); err != nil {
			return nil, fmt.Errorf("grpcx: invalid Resolver endpoints:%v", err)
		}
	}

	basePath = store.Normalize(basePath)
	if strings.Contains(sname, "/") {
		err = fmt.Errorf("grpcx: invalid service name:%v", sname)
		return
	}

	nodePath := path.Join(basePath, sname)
	s := &etcdNode{
		stopCh:         make(chan struct{}),
		sessionTimeout: sessionTimeout,
		nodePath:       nodePath,
		nodeValue:      string(nodeValue),
		basePath:       basePath,
	}

	entries := store.CreateEndpoints(endpoints, "http")
	cfg := &etcd.Config{
		Endpoints:               entries,
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: sessionTimeout,
	}

	c, err := etcd.New(*cfg)
	if err != nil {
		xlog.Fatal(err)
	}

	s.client = etcd.NewKeysAPI(c)
	if _, err := s.client.Set(context.Background(), basePath, "", &etcd.SetOptions{
		Dir: true,
	}); err != nil {
		xlog.Infof("grpcx: set service base directory fail:%v", basePath)
	}

	if rsp, err := s.client.Get(context.Background(), basePath, &etcd.GetOptions{
		Recursive: false,
	}); err != nil || !rsp.Node.Dir {
		xlog.Fatal("grpcx: service base path not exist or not a path")
	}

	// Periodic Cluster Sync
	go func() {
		for {
			if err := c.AutoSync(context.Background(), periodicSync); err != nil {
				return
			}
		}
	}()

	go s.eventLoop()
	return s, nil
}

func (s *etcdNode) eventLoop() {
	refreshTick := time.NewTicker(s.sessionTimeout / 2)
	defer func() {
		refreshTick.Stop()
		//remove the node from etcd
		s.client.Delete(context.Background(), s.nodePath, nil)
	}()

	for {
		select {
		case <-s.stopCh:
			return
		case <-refreshTick.C:
			// first check exist or not: not exist create it, otherwist refresh it
			if rsp, err := s.client.Get(context.Background(), s.nodePath, nil); err != nil {
				xlog.Infof("grpcx: etcdNode node not exist, nodePath:%v", s.nodePath)
				if _, err := s.client.Set(context.Background(), s.nodePath, s.nodeValue, &etcd.SetOptions{
					PrevExist: etcd.PrevExist,
					TTL:       s.sessionTimeout,
				}); err != nil {
					xlog.Errorf("grpcx: etcdNode create node fail:%v nodepath:%v", err, s.nodePath)
				}
				continue
			} else if rsp.Node.Dir { // exist but not a file, critical error
				xlog.Errorf("grpcx: etcdNode invalid, want file get dir, nodePath:%v", s.nodePath)
				continue
			}

			//refresh node ttl: watcher will not recv notifyication when refresh ttl
			if _, err := s.client.Set(context.Background(), s.nodePath, s.nodeValue, &etcd.SetOptions{
				PrevExist: etcd.PrevExist,
				Refresh:   true,
				TTL:       s.sessionTimeout,
			}); err != nil {
				xlog.Errorf("grpcx: etcdNode refresh node ttl fail:%v", err)
			}
		}
	}
}

func (s *etcdNode) Stop() {
	defer func() {
		recover()
	}()

	close(s.stopCh)
}
