package discover

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/docker/libkv"

	"github.com/docker/libkv/store"
	xlog "google.golang.org/grpc/grpclog"
)

// NewZookeeperResolver used to create a zookeeper discovery
func NewZookeeperResolver(endpoints []string, timeout time.Duration) (ResolverAPI, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("grpcx: empty endpoints")
	}

	for _, endpoint := range endpoints {
		if _, err := net.ResolveTCPAddr("tcp4", endpoint); err != nil {
			return nil, fmt.Errorf("grpcx: invalid Resolver endpoints:%v", err)
		}
	}

	if timeout < time.Second {
		timeout = time.Second
	}

	return &zookeeperResolver{
		endpoints: endpoints,
		config: &store.Config{
			ConnectionTimeout: timeout,
		},
		stopCh: make(chan struct{}),
		watchs: make(map[string]notifiers),
	}, nil
}

type zookeeperResolver struct {
	driver    store.Store
	endpoints []string
	config    *store.Config
	watchs    map[string]notifiers
	mux       sync.RWMutex
	stopCh    chan struct{}
}

func (zr *zookeeperResolver) Start() (err error) {
	zr.mux.Lock()
	defer zr.mux.Unlock()
	if zr.stopCh != nil {
		err = fmt.Errorf("grpcx: etcdResolver already stared")
		xlog.Warningf("%v", err)
		return
	}

	zr.driver, err = libkv.NewStore(store.ETCD, zr.endpoints, zr.config)
	if err != nil {
		zr.stopCh = make(chan struct{})
	}
	return
}

func (zr *zookeeperResolver) Stop() {
	zr.mux.Lock()
	defer func() {
		if r := recover(); r != nil {
			xlog.Errorf("grpcx: zookeeperResolver recover:%v", r)
		}
		zr.mux.Unlock()
	}()

	close(zr.stopCh)
	zr.driver.Close()

	for _, notifier := range zr.watchs {
		close(notifier.gotifyCh)
		close(notifier.stopCh)
	}
	zr.watchs = make(map[string]notifiers)
}

func (zr *zookeeperResolver) SubService(spath string) (<-chan []*NotifyInfo, error) {
	zr.mux.Lock()
	defer zr.mux.Unlock()

	if zr.stopCh != nil {
		select {
		case <-zr.stopCh:
			return nil, fmt.Errorf("grpcx: etcdResolver already closed or unstart yet")
		default:
		}
	} else {
		return nil, fmt.Errorf("grpcx: etcdResolver already closed or unstart yet")
	}

	if _, ok := zr.watchs[spath]; ok {
		return nil, fmt.Errorf("grpcx: service already be subscribed:%v", spath)
	}

	stopCh := make(chan struct{})
	gotifyCh := make(chan []*NotifyInfo)
	zr.watchs[spath] = notifiers{
		stopCh:   stopCh,
		gotifyCh: gotifyCh,
	}

	gotify := func() {
		retryDelay := time.Duration(0)
		for {
			notifyCh, err := zr.driver.WatchTree(spath, stopCh)
			if err != nil {
				xlog.Errorf("grpcx: zookeeper WatchTree fail:%v", err)
				select {
				case <-zr.stopCh:
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
					case <-zr.stopCh:
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
						Val:         notify.Value,
						LastVersion: notify.LastIndex,
					})
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				select {
				case gotifyCh <- ninfos:
					break
				case <-zr.stopCh:
					cancel()
					return
				case <-ctx.Done():
					xlog.Warningf("grpcx: zookeeper update discard because of notify chan ful and put timeout:%v", ninfos)
				}
				cancel()
			}
		}
	}

	go gotify()
	return gotifyCh, nil
}

func (zr *zookeeperResolver) UnSubService(spath string) {
	zr.mux.Lock()
	defer zr.mux.Unlock()

	if notifier, ok := zr.watchs[spath]; ok {
		delete(zr.watchs, spath)
		close(notifier.stopCh)
		close(notifier.gotifyCh)
	}
}
