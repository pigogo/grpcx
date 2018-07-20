package main

import (
	"fmt"

	"github.com/pigogo/grpcx"
	pb "github.com/pigogo/grpcx/examples/helloworld/helloworld"
	"golang.org/x/net/context"
)

// SayHello implements helloworld.GreeterServer
func SayHello(ctx context.Context, in *pb.HelloRequest, out *pb.HelloReply) error {
	out.Message = "Raw::Hello " + in.Name
	meta := grpcx.GetMetaFromContext(ctx)
	if meta != nil {
		for key, val := range meta {
			fmt.Println("metadata::key:", key, " value:", string(val))
		}
	}
	return nil
}
