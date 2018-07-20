/*
 *
 * Copyright 2014 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package grpcx

import (
	"golang.org/x/net/context"
)

// StreamHandler defines the handler called by gRPC server to complete the
// execution of a streaming RPC.
type StreamHandler func(srv interface{}, stream ServerStream) error

// StreamDesc represents a streaming RPC service's method specification.
type StreamDesc struct {
	StreamName string
	Handler    StreamHandler

	// At least one of these is true.
	ServerStreams bool
	ClientStreams bool
}

// Stream defines the common interface a client or server stream has to satisfy.
type Stream interface {
	// Context returns the context for this stream.
	Context() context.Context
	// SendMsg blocks until it sends m, the stream is done or the stream
	// breaks.
	// On error, it aborts the stream and returns an RPC status on client
	// side. On server side, it simply returns the error to the caller.
	// SendMsg is called by generated code. Also Users can call SendMsg
	// directly when it is really needed in their use cases.
	// It's safe to have a goroutine calling SendMsg and another goroutine calling
	// recvMsg on the same stream at the same time.
	// But it is not safe to call SendMsg on the same stream in different goroutines.
	SendMsg(m interface{}) error
	// RecvMsg blocks until it receives a message or the stream is
	// done. On client side, it returns io.EOF when the stream is done. On
	// any other error, it aborts the stream and returns an RPC status. On
	// server side, it simply returns the error to the caller.
	// It's safe to have a goroutine calling SendMsg and another goroutine calling
	// recvMsg on the same stream at the same time.
	// But it is not safe to call RecvMsg on the same stream in different goroutines.
	RecvMsg(m interface{}) error

	// onMessage: inner use
	onMessage(conn Conn, head *PackHeader, body []byte) (err error)
}

// ClientStream defines the interface a client stream has to satisfy.
type ClientStream interface {
	// CloseSend closes the send direction of the stream. It closes the stream
	// when non-nil error is met.
	CloseSend() error
	// Stream.SendMsg() may return a non-nil error when something wrong happens sending
	// the request. The returned error indicates the status of this sending, not the final
	// status of the RPC.
	// Always call Stream.RecvMsg() to get the final status if you care about the status of
	// the RPC.
	Stream
}

// ServerStream defines the interface a server stream has to satisfy.
type ServerStream interface {
	Stream
}
