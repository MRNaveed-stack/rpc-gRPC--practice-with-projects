package main

import (
	"context"
	"log"
	"time"

	pb "github.com/MRNaveed-stack/rpc-gRPC--practice-with-projects/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewTodoServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := client.CreateTask(ctx, &pb.CreateTaskRequest{
		Title:       "Unary Task",
		Description: "Created via regular 1-to-1 RPC call",
	})
	if err != nil {
		log.Fatalf("CreateTask failed: %v", err)
	}

	log.Printf("[Unary Success] Task Created: ID=%s, Title=%s", res.Task.Id, res.Task.Title)
}