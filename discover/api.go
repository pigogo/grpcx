// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package discover

import (
	"time"
)

var maxRetryDelay = time.Second * 3

// NotifyInfo descript the info return from storage server
type NotifyInfo struct {
	Key         string
	Val         string
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
