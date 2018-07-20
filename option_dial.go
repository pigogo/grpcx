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
 *	dialOptions has been modified by Kun Zhong
 */

package grpcx

import (
	"time"

	"github.com/pigogo/grpcx/codec"
	"github.com/pigogo/grpcx/compresser"
	"github.com/pigogo/grpcx/credentials"
)

// dialOptions configure a Dial call. dialOptions are set by the DialOption
// values passed to Dial.
type dialOptions struct {
	creds           credentials.TransportCredentials
	codec           codec.Codec
	cp              compresser.Compressor
	dc              compresser.Decompressor
	balancer        Balancer
	block           bool
	insecure        bool
	timeout         time.Duration
	keepalivePeriod time.Duration
	scChan          <-chan ServiceConfig
	callOptions     []CallOption
	connPlugin      IConnPlugin
}

const (
	defaultClientMaxReceiveMessageSize = 1024 * 1024 * 4
	defaultClientMaxSendMessageSize    = 1024 * 1024 * 4
)

// DialOption configures how we set up the connection.
type DialOption func(*dialOptions)

// WithMaxMsgSize returns a DialOption which sets the maximum message size the client can receive. Deprecated: use WithDefaultCallOptions(MaxCallRecvMsgSize(s)) instead.
func WithMaxMsgSize(s int) DialOption {
	return WithDefaultCallOptions(MaxCallRecvMsgSize(s))
}

// WithDefaultCallOptions returns a DialOption which sets the default CallOptions for calls over the connection.
func WithDefaultCallOptions(cos ...CallOption) DialOption {
	return func(o *dialOptions) {
		o.callOptions = append(o.callOptions, cos...)
	}
}

// WithCodec returns a DialOption which sets a codec for message marshaling and unmarshaling.
func WithCodec(c codec.Codec) DialOption {
	return func(o *dialOptions) {
		o.codec = c
	}
}

// WithCompressor returns a DialOption which sets a CompressorGenerator for generating message
// compressor.
func WithCompressor(cp compresser.Compressor) DialOption {
	return func(o *dialOptions) {
		o.cp = cp
	}
}

// WithDecompressor returns a DialOption which sets a DecompressorGenerator for generating
// message decompressor.
func WithDecompressor(dc compresser.Decompressor) DialOption {
	return func(o *dialOptions) {
		o.dc = dc
	}
}

// WithBalancer returns a DialOption which sets a load balancer.
func WithBalancer(b Balancer) DialOption {
	return func(o *dialOptions) {
		o.balancer = b
	}
}

// WithServiceConfig returns a DialOption which has a channel to read the service configuration.
func WithServiceConfig(c <-chan ServiceConfig) DialOption {
	return func(o *dialOptions) {
		o.scChan = c
	}
}

// WithBlock returns a DialOption which makes caller of Dial blocks until the underlying
// connection is up. Without this, Dial returns immediately and connecting the server
// happens in background.
func WithBlock() DialOption {
	return func(o *dialOptions) {
		o.block = true
	}
}

// WithInsecure returns a DialOption which disables transport security for this ClientConn.
// Note that transport security is required unless WithInsecure is set.
func WithInsecure() DialOption {
	return func(o *dialOptions) {
		o.insecure = true
	}
}

// WithTimeout returns a DialOption that configures a timeout for dialing a ClientConn
// initially. This is valid if and only if WithBlock() is present.
// Deprecated: use DialContext and context.WithTimeout instead.
func WithTimeout(d time.Duration) DialOption {
	return func(o *dialOptions) {
		o.timeout = d
	}
}

// WithKeepAlive return DialOption that set the connection keep alive period
func WithKeepAlive(d time.Duration) DialOption {
	return func(o *dialOptions) {
		o.keepalivePeriod = d
	}
}

// WithCreds returns a DialOption that sets credentials for server connections.
func WithCreds(c credentials.TransportCredentials) DialOption {
	return func(o *dialOptions) {
		o.creds = c
	}
}

// WithPlugin return DialOption that handle connection event plugin
func WithPlugin(plugin IConnPlugin) DialOption {
	return func(o *dialOptions) {
		o.connPlugin = plugin
	}
}
