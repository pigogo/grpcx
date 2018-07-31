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
 *
 *	Modifiaction Statement
 *	this file has been modified by Kun Zhong
 */

package grpcx

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pigogo/grpcx/compresser"
)

// The format of the payload: compressed or not?
type payloadFormat uint8

const (
	compressionNone payloadFormat = iota // no compression
	compressionMade
)

func checkRecvPayload(recvCompress compresser.CompType, dc compresser.Decompressor) error {
	switch recvCompress {
	case compresser.CompTypeNone:
	default:
		if dc == nil || recvCompress != dc.Type() {
			return fmt.Errorf("grpc: Decompressor is not installed for grpc-encoding %q", recvCompress)
		}
	}
	return nil
}

// MethodConfig defines the configuration recommended by the service providers for a
// particular method.
// This is EXPERIMENTAL and subject to change.
type MethodConfig struct {
	// WaitForReady indicates whether RPCs sent to this method should wait until
	// the connection is ready by default (!failfast). The value specified via the
	// gRPC client API will override the value set here.
	WaitForReady *bool
	// Timeout is the default timeout for RPCs sent to this method. The actual
	// deadline used will be the minimum of the value specified here and the value
	// set by the application via the gRPC client API.  If either one is not set,
	// then the other will be used.  If neither is set, then the RPC has no deadline.
	Timeout *time.Duration
	// MaxReqSize is the maximum allowed payload size for an individual request in a
	// stream (client->server) in bytes. The size which is measured is the serialized
	// payload after per-message compression (but before stream compression) in bytes.
	// The actual value used is the minumum of the value specified here and the value set
	// by the application via the gRPC client API. If either one is not set, then the other
	// will be used.  If neither is set, then the built-in default is used.
	MaxReqSize *int
	// MaxRespSize is the maximum allowed payload size for an individual response in a
	// stream (server->client) in bytes.
	MaxRespSize *int
}

// ServiceConfig is provided by the service provider and contains parameters for how
// clients that connect to the service should behave.
// This is EXPERIMENTAL and subject to change.
type ServiceConfig struct {
	// LB is the load balancer the service providers recommends. The balancer specified
	// via grpc.WithBalancer will override this.
	LB Balancer
	// Methods contains a map for the methods in this service.
	// If there is an exact match for a method (i.e. /service/method) in the map, use the corresponding MethodConfig.
	// If there's no exact match, look for the default config for the service (/service/) and use the corresponding MethodConfig if it exists.
	// Otherwise, the method has no MethodConfig to use.
	Methods map[string]MethodConfig
}

func min(a, b *int) *int {
	if *a < *b {
		return a
	}
	return b
}

func getMaxSize(mcMax, doptMax *int, defaultVal int) *int {
	if mcMax == nil && doptMax == nil {
		return &defaultVal
	}
	if mcMax != nil && doptMax != nil {
		return min(mcMax, doptMax)
	}
	if mcMax != nil {
		return mcMax
	}
	return doptMax
}

// parseDialTarget returns the network and address to pass to dialer
func parseDialTarget(target string) (net string, addr string) {
	//default is tcp network
	net = "tcp"

	m1 := strings.Index(target, ":")
	m2 := strings.Index(target, ":/")

	// handle unix:addr which will fail with url.Parse
	if m1 >= 0 && m2 < 0 {
		if n := target[0:m1]; n == "unix" {
			net = n
			addr = target[m1+1:]
			return net, addr
		}
	}
	if m2 >= 0 {
		t, err := url.Parse(target)
		if err != nil {
			return net, target
		}
		scheme := t.Scheme
		addr = t.Path
		if scheme == "unix" {
			net = scheme
			if addr == "" {
				addr = t.Host
			}
			return net, addr
		}
	}

	return net, target
}

// SupportPackageIsVersion3 is referenced from generated protocol buffer files.
// The latest support package version is 4.
// SupportPackageIsVersion3 is kept for compability. It will be removed in the
// next support package version update.
const SupportPackageIsVersion3 = true

// SupportPackageIsVersion4 is referenced from generated protocol buffer files
// to assert that that code is compatible with this version of the grpc package.
//
// This constant may be renamed in the future if a change in the generated code
// requires a synchronised update of grpc-go and protoc-gen-go. This constant
// should not be referenced from any other code.
const SupportPackageIsVersion4 = true

// Version is the current grpc version.
const Version = "1.6.0-dev"

const grpcUA = "grpc-go/" + Version
