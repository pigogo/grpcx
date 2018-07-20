package main

import (
	"fmt"

	"github.com/pigogo/grpcx"
	pb "github.com/pigogo/grpcx/examples/helloworld/helloworld"
	"golang.org/x/net/context"
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest, out *pb.HelloReply) error {
	out.Message = "Struct::Hello " + in.Name
	meta := grpcx.GetMetaFromContext(ctx)
	if meta != nil {
		for key, val := range meta {
			fmt.Println("metadata::key:", key, " value:", string(val))
		}
	}
	return nil
}
