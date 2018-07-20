package main

import (
	"fmt"

	"github.com/pigogo/grpcx"
	pb "github.com/pigogo/grpcx/examples/helloworld/helloworld"
	"golang.org/x/net/context"
)

// OnewaySayHello implements oneway sayhello
func OnewaySayHello(ctx context.Context, in *pb.HelloRequest) error {
	out := &pb.HelloReply{
		Message: "OnewaySayHello::Hello " + in.Name,
	}

	meta := grpcx.GetMetaFromContext(ctx)
	if meta != nil {
		for key, val := range meta {
			fmt.Println("metadata::key:", key, " value:", string(val))
		}
	}

	return srv.SendTo(ctx, "/notify/SayHello", out)
}
