/*
 *
 * Copyright 2016 gRPC authors.
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
 *
 *	Modifiaction Statement
 *	options has been modified by Kun Zhong
 */

package grpcx

import (
	"time"

	"github.com/pigogo/grpcx/codec"
	"github.com/pigogo/grpcx/compresser"
	"github.com/pigogo/grpcx/credentials"
)

const (
	defaultWindowSize                  = 4096
	defaultServerMaxReceiveMessageSize = 1024 * 1024 * 4
	defaultServerMaxSendMessageSize    = 1024 * 1024 * 4
	defaultServerMaxConcurrentReq      = 100000
	maxServerMaxConcurrentReq          = 300000
	minServerMaxConcurrentReq          = 1000

	defaultServerMaxConcurrentRoutine = 10000
	maxServerMaxConcurrentRoutine     = 100000
	minServerMaxConcurrentRoutine     = 10
)

type options struct {
	creds                 credentials.TransportCredentials
	codec                 codec.Codec
	cp                    compresser.Compressor
	dc                    compresser.Decompressor
	readerWindowSize      int32
	readTimeout           time.Duration
	writeTimeout          time.Duration
	maxReceiveMessageSize int
	maxSendMessageSize    int
	maxConcurrentRequest  uint32
	maxConcurrentRoutine  uint32
	keepalivePeriod       time.Duration
	connPlugin            IConnPlugin
}

var defaultServerOptions = options{
	maxReceiveMessageSize: defaultServerMaxReceiveMessageSize,
	maxSendMessageSize:    defaultServerMaxSendMessageSize,
	maxConcurrentRequest:  defaultServerMaxConcurrentReq,
	maxConcurrentRoutine:  defaultServerMaxConcurrentRoutine,
	readerWindowSize:      defaultWindowSize,
}

// A ServerOption sets options such as credentials, codec and keepalive parameters, etc.
type ServerOption func(*options)

// ReaderWindowSize returns a ServerOption which sets the value for reader window size for most data read once.
func ReaderWindowSize(s int32) ServerOption {
	return func(o *options) {
		o.readerWindowSize = s
	}
}

// ReadTimeout return a ServerOption which sets the conn's read timeout
func ReadTimeout(t time.Duration) ServerOption {
	return func(o *options) {
		o.readTimeout = t
	}
}

// WriteTimeout return a ServerOption which sets the conn's write timeout
func WriteTimeout(t time.Duration) ServerOption {
	return func(o *options) {
		o.writeTimeout = t
	}
}

// KeepalivePeriod returns a ServerOption that sets keepalive and max-age parameters for the server.
func KeepalivePeriod(kp time.Duration) ServerOption {
	return func(o *options) {
		if kp < time.Second {
			kp = time.Second
		}
		o.keepalivePeriod = kp
	}
}

// CustomCodec returns a ServerOption that sets a codec for message marshaling and unmarshaling.
func CustomCodec(codec codec.Codec) ServerOption {
	return func(o *options) {
		o.codec = codec
	}
}

// RPCCompressor returns a ServerOption that sets a compressor for outbound messages.
func RPCCompressor(cp compresser.Compressor) ServerOption {
	return func(o *options) {
		o.cp = cp
	}
}

// RPCDecompressor returns a ServerOption that sets a decompressor for inbound messages.
func RPCDecompressor(dc compresser.Decompressor) ServerOption {
	return func(o *options) {
		o.dc = dc
	}
}

// MaxMsgSize returns a ServerOption to set the max message size in bytes the server can receive.
// If this is not set, gRPC uses the default limit. Deprecated: use MaxRecvMsgSize instead.
func MaxMsgSize(m int) ServerOption {
	return MaxRecvMsgSize(m)
}

// MaxRecvMsgSize returns a ServerOption to set the max message size in bytes the server can receive.
// If this is not set, gRPC uses the default 4MB.
func MaxRecvMsgSize(m int) ServerOption {
	return func(o *options) {
		o.maxReceiveMessageSize = m
	}
}

// MaxSendMsgSize returns a ServerOption to set the max message size in bytes the server can send.
// If this is not set, gRPC uses the default 4MB.
func MaxSendMsgSize(m int) ServerOption {
	return func(o *options) {
		o.maxSendMessageSize = m
	}
}

// MaxConcurrentRequest returns a ServerOption that will apply a limit on the number
// of concurrent request.
func MaxConcurrentRequest(n uint32) ServerOption {
	return func(o *options) {
		if n > maxServerMaxConcurrentReq {
			n = maxServerMaxConcurrentReq
		} else if n < minServerMaxConcurrentReq {
			n = minServerMaxConcurrentReq
		}
		o.maxConcurrentRequest = n
	}
}

// MaxConcurrentRoutine returns a ServerOption that will apply a limit on the number
// of concurrent routine.
func MaxConcurrentRoutine(n uint32) ServerOption {
	return func(o *options) {
		if n > maxServerMaxConcurrentRoutine {
			n = maxServerMaxConcurrentRoutine
		} else if n < minServerMaxConcurrentRoutine {
			n = minServerMaxConcurrentRoutine
		}
		o.maxConcurrentRoutine = n
	}
}

// Creds returns a ServerOption that sets credentials for server connections.
func Creds(c credentials.TransportCredentials) ServerOption {
	return func(o *options) {
		o.creds = c
	}
}

// Plugin return ServerOption that handle connection event plugin
func Plugin(plugin IConnPlugin) ServerOption {
	return func(o *options) {
		o.connPlugin = plugin
	}
}
