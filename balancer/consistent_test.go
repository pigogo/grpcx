// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package balancer

import (
	"testing"

	"github.com/pigogo/grpcx/discover"
)

func TestConsistent1(t *testing.T) {
	r := &resolver{}
	consistent := NewConsistentBalancer(r)
	test1(consistent, r, t)
}

func TestConsistent2(t *testing.T) {
	r := &resolver{}
	consistent := NewConsistentBalancer(r)
	test2(consistent, r, t)
}

func TestConsistent3(t *testing.T) {
	r := &resolver{
		notify: make(chan []*discover.NotifyInfo),
	}

	consistent := NewConsistentBalancer(r)
	test3(consistent, r, t)
}
