package main

import (
	"log"
	"os"
	"sync"

	grpc "github.com/pigogo/grpcx"
	pb "github.com/pigogo/grpcx/examples/helloworld/helloworld"
	"golang.org/x/net/context"
)

const (
	address     = "localhost:50052"
	defaultName = "world"
)

var (
	waitNotifyDone = sync.WaitGroup{}
)

// SayHello callback from server notify
func SayHello(ctx context.Context, notify *pb.HelloReply) error {
	log.Printf("Notify Greeting: %s", notify.Message)
	waitNotifyDone.Done()
	return nil
}

func main() {
	// Set up the notify handler before serving
	grpc.RegistNotifyHandler("/notify/SayHello", SayHello)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	// Contact the server and print out its response.
	name := defaultName
	if len(os.Args) > 1 {
		name = os.Args[1]
	}

	reply := &pb.HelloReply{}
	//RawHandler request
	err = conn.Call(context.Background(), "raw/SayHello", &pb.HelloRequest{Name: name}, reply)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Raw Greeting: %s", reply.Message)

	//struct handler request
	err = conn.Call(context.Background(), "struct/SayHello", &pb.HelloRequest{Name: name}, reply)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Struct Greeting: %s", reply.Message)

	//struct handler request
	callctx, err := conn.BackCall(context.Background(), "raw/SayHello", &pb.HelloRequest{Name: name}, reply)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	<-callctx.Done()
	log.Printf("Back Struct Greeting: %s timeusage:%v", reply.Message, callctx.TimePass())

	//oneway handler request
	waitNotifyDone.Add(1)
	err = conn.Send(context.Background(), "oneway/SayHello", &pb.HelloRequest{Name: name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	//wait until get the notify from server
	waitNotifyDone.Wait()
}
