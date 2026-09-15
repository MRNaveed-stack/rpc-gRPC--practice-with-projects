package main

import (
	"context"
	"log"
	"time"

	pb "github.com/MRNaveed-stack/rpc-gRPC--practice-with-projects/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewTodoServiceClient(conn)

	// TEST 1: Request without token (Expect Failure)
	log.Println("--- Test 1: Sending request without token ---")
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()

	_, err = client.CreateTask(ctx1, &pb.CreateTaskRequest{
		Title:       "Unauthorized Task",
		Description: "Should be blocked by interceptor",
	})
	if err != nil {
		st, _ := status.FromError(err)
		log.Printf("Expected rejection! Code: %s | Message: %s\n", st.Code(), st.Message())
	}

	// TEST 2: Request with proper metadata token (Expect Success)
	log.Println("\n--- Test 2: Sending request with Bearer token ---")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	// Inject HTTP/2 headers into the context
	md := metadata.Pairs("authorization", "Bearer secret-vault-token-123")
	authCtx := metadata.NewOutgoingContext(ctx2, md)

	res, err := client.CreateTask(authCtx, &pb.CreateTaskRequest{
		Title:       "Production Task",
		Description: "Authenticated via gRPC metadata",
	})
	if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}

	log.Printf("Success! Created Task ID: %s | Title: %s\n", res.Task.Id, res.Task.Title)
}