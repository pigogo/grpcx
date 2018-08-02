// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package balancer

import (
	"context"
	"testing"
	"time"

	"github.com/pigogo/grpcx"
	"github.com/stretchr/testify/assert"

	"github.com/pigogo/grpcx/discover"
)

type resolver struct {
	notify chan []*discover.NotifyInfo
}

func (r resolver) Start() error {
	return nil
}

func (r resolver) Stop() {

}

func (r *resolver) SubService(spath string) (ch <-chan []*discover.NotifyInfo, unsub func(), err error) {
	return r.notify, nil, nil
}

func test1(b grpcx.Balancer, r *resolver, t *testing.T) {
	assert.NoError(t, b.Start("roundroubin", grpcx.BalancerConfig{}))
	addr, put, err := b.Get(context.Background(), grpcx.BalancerGetOptions{
		BlockingWait: false,
	})
	assert.Error(t, err)
	assert.Nil(t, put)
	assert.Empty(t, addr)
}

func test2(b grpcx.Balancer, r *resolver, t *testing.T) {
	assert.NoError(t, b.Start("roundroubin", grpcx.BalancerConfig{}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	gbegin := time.Now()
	addr, put, err := b.Get(ctx, grpcx.BalancerGetOptions{
		BlockingWait: true,
	})
	tpast := time.Since(gbegin)
	assert.True(t, tpast > time.Second*3)
	assert.Error(t, err)
	assert.Nil(t, put)
	assert.Empty(t, addr)
}

func test3(b grpcx.Balancer, r *resolver, t *testing.T) {
	assert.NoError(t, b.Start("roundroubin", grpcx.BalancerConfig{}))

	go func() {
		r.notify <- []*discover.NotifyInfo{
			&discover.NotifyInfo{
				Key:         "localhost:0",
				Val:         "schema:tcp",
				LastVersion: 1,
			},
		}
	}()

	go func() {
		time.Sleep(time.Second)
		notifyCh := b.Notify()
		for notifys := range notifyCh {
			for _, notify := range notifys {
				assert.NotNil(t, b.Up(notify))
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	gbegin := time.Now()
	addr, _, err := b.Get(ctx, grpcx.BalancerGetOptions{
		BlockingWait: true,
	})
	tpast := time.Since(gbegin)
	assert.True(t, tpast > time.Second)
	assert.True(t, tpast < time.Second*2)

	assert.NoError(t, err)
	assert.Equal(t, addr, "localhost:0")
}
