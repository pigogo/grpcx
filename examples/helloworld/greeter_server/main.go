/*
 *
 * Copyright 2015 gRPC authors.
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

package main

import (
	"log"
	"net"
	"time"

	grpc "github.com/pigogo/grpcx"
	pb "github.com/pigogo/grpcx/examples/helloworld/helloworld"
	"golang.org/x/net/context"
)

const (
	port = ":50051"
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

func main() {
	s := grpc.NewServer(grpc.KeepalivePeriod(time.Second * 15))
	pb.RegisterGreeterServer(s, &server{})

	ln, err := net.Listen("tcp4", port)
	if err != nil {
		panic(err)
	}
	if err = s.Serve(ln); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
