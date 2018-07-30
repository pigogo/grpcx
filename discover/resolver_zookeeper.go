package discover

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/docker/libkv"

	"github.com/docker/libkv/store"
	xlog "github.com/pigogo/grpcx/grpclog"
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
		watchs: make(map[string]*notifiers),
	}, nil
}

type zookeeperResolver struct {
	driver    store.Store
	endpoints []string
	config    *store.Config
	watchs    map[string]*notifiers
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
		for _, watcher := range notifier.watchers {
			close(watcher)
		}
		close(notifier.stopCh)
	}
	zr.watchs = make(map[string]*notifiers)
}

func (zr *zookeeperResolver) SubService(spath string) (<-chan []*NotifyInfo, func(), error) {
	zr.mux.Lock()
	defer zr.mux.Unlock()

	if zr.stopCh != nil {
		select {
		case <-zr.stopCh:
			return nil, nil, fmt.Errorf("grpcx: etcdResolver already closed or unstart yet")
		default:
		}
	} else {
		return nil, nil, fmt.Errorf("grpcx: etcdResolver already closed or unstart yet")
	}

	var watchers *notifiers
	var gotifyCh = make(chan []*NotifyInfo)
	var unsub = func() {
		zr.mux.Lock()
		defer zr.mux.Unlock()

		wlen := len(watchers.watchers)
		for i, watcher := range watchers.watchers {
			if watcher == gotifyCh {
				watchers.watchers[i] = watchers.watchers[wlen-1]
				watchers.watchers = watchers.watchers[:wlen-1]
				//no watcher, remove it
				if wlen-1 == 0 {
					close(watchers.stopCh)
					delete(zr.watchs, spath)
				}
			}
		}
	}

	if watchers := zr.watchs[spath]; watchers != nil {
		watchers.watchers = append(watchers.watchers, gotifyCh)
		return gotifyCh, unsub, nil
	}

	watchers = &notifiers{
		stopCh: make(chan struct{}),
	}
	watchers.watchers = append(watchers.watchers, gotifyCh)
	zr.watchs[spath] = watchers
	gotify := func() {
		retryDelay := time.Duration(0)
		for {
			notifyCh, err := zr.driver.WatchTree(spath, watchers.stopCh)
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

				zr.mux.RLock()
				for _, watcher := range watchers.watchers {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
					select {
					case watcher <- ninfos:
						break
					case <-zr.stopCh:
						zr.mux.RUnlock()
						cancel()
						return
					case <-ctx.Done():
						xlog.Warningf("grpcx: zookeeper update discard because of notify chan ful and put timeout:%v", ninfos)
					}
					cancel()
				}
				zr.mux.RUnlock()
			}
		}
	}

	go gotify()
	return gotifyCh, unsub, nil
}
