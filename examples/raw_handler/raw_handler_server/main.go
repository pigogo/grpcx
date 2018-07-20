package main

import (
	"log"

	grpc "github.com/pigogo/grpcx"
)

const (
	port = ":50052"
)

var (
	srv *grpc.Server
)

func main() {
	srv = grpc.NewServer()
	if err := srv.RegistHandler("/raw/SayHello", SayHello, true); err != nil {
		panic(err)
	}

	if err := srv.RegistStruct("struct", &server{}); err != nil {
		panic(err)
	}

	if err := srv.RegistHandler("oneway/SayHello", OnewaySayHello, true); err != nil {
		panic(err)
	}

	if err := srv.Serve(port); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
