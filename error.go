// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"errors"
)

var (
	// ErrClientConnClosing indicates that the operation is illegal because
	// the ClientConn is closing.
	ErrClientConnClosing = errors.New("grpc: the client connection is closing")
	// errNetworkIO indicates that the connection is down due to some network I/O error.
	errNetworkIO = errors.New("grpcx: failed with network I/O error")
	// errDraining indicates the connection is draining
	errDraining = errors.New("grpcx: the connection is draining")
	// errConnUnavailable indicates that the connection is unavailable.
	errConnUnavailable = errors.New("grpcx: the connection is unavailable")
	// errConnClosing indicates that the connection is closing.
	errConnClosing = errors.New("grpcx: the connection is closing")
	// errConnGoaway indicates that the server is closing
	errSrvGoaway = errors.New("grpcx: the server is closing")
	// errBalancerClosed indicates that the balancer is closed.
	errBalancerClosed = errors.New("grpcx: balancer is closed")
)
