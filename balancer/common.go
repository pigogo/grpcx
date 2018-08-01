// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package balancer

import "errors"

var (
	errNoService = errors.New("grpcx: no service avaliable")
)

type serviceInfo struct {
	addr      string
	connected bool
}
