package main

import (
	"context"
	"io"
	"log"
	"time"

	pb "github.com/MRNaveed-stack/rpc-gRPC--practice-with-projects/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewTodoServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Println("Starting Server Streaming RPC...")
	stream, err := client.StreamTasks(ctx, &pb.StreamTasksRequest{IncompleteOnly: false})
	if err != nil {
		log.Fatalf("StreamTasks failed: %v", err)
	}

	// Continuously receive streamed elements until the server closes the stream with EOF
	for {
		task, err := stream.Recv()
		if err == io.EOF {
			log.Println("Server finished streaming all tasks (EOF reached).")
			break
		}
		if err != nil {
			log.Fatalf("Error while receiving stream: %v", err)
		}
		log.Printf("-> [Stream Recv] ID: %s | Title: %s | Completed: %t", task.GetId(), task.GetTitle(), task.GetCompleted())
	}
}