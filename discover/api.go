package discover

import (
	"time"
)

var maxRetryDelay = time.Second * 3

// NotifyInfo descript the info return from storage server
type NotifyInfo struct {
	Key         string
	Val         []byte
	LastVersion uint64
}

type notifiers struct {
	stopCh   chan struct{}
	watchers []chan []*NotifyInfo
}

// ResolverAPI is the interface of the discover
type ResolverAPI interface {
	// Start begin the resolver
	Start() error
	// Stop close the resolver
	Stop()
	// SubService watch the service; any change will be notify by the ch
	// unsub used to unsuband remove the notification
	SubService(spath string) (ch <-chan []*NotifyInfo, unsub func(), err error)
}

// RegisterAPI used to stop a service and delete the node
type RegisterAPI interface {
	Stop()
}
