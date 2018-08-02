// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package balancer

import (
	"testing"

	"github.com/pigogo/grpcx/discover"
)

func TestRoundroubin1(t *testing.T) {
	r := &resolver{}
	roundroubin := NewRoundRobinBalancer(r)
	test1(roundroubin, r, t)
}

func TestRoundroubin2(t *testing.T) {
	r := &resolver{}
	roundroubin := NewRoundRobinBalancer(r)
	test2(roundroubin, r, t)
}

func TestRoundroubin3(t *testing.T) {
	r := &resolver{
		notify: make(chan []*discover.NotifyInfo),
	}

	roundroubin := NewRoundRobinBalancer(r)
	test3(roundroubin, r, t)
}
