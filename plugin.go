// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

// IConnPlugin define the connection event interface
type IConnPlugin interface {
	// OnPreConnect call before dial a service
	// only client side can trigger this event
	OnPreConnect(addr string) error

	// OnPostCOnnect call when a connection happen(dial or accept)
	OnPostConnect(Conn) interface{}

	// OnPreDisconnect call when try to close a connection
	OnPreDisconnect(Conn)

	// OnPostDisconnect call when a connection closed
	OnPostDisconnect(Conn)
}
