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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	log.Println("Starting Bidirectional Streaming RPC...")
	stream, err := client.TaskChat(ctx)
	if err != nil {
		log.Fatalf("Failed to initiate chat stream: %v", err)
	}

	done := make(chan struct{})

	// Goroutine: Read responses from server
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				log.Println("Server closed read stream.")
				close(done)
				return
			}
			if err != nil {
				log.Printf("Recv error: %v", err)
				close(done)
				return
			}
			log.Printf("[Server Reply] %s: %s", in.GetUser(), in.GetMessage())
		}
	}()

	// Main Goroutine: Stream messages to server
	messages := []string{
		"Hello from Go client",
		"Working on task #1",
		"Completed task #1",
	}

	for _, msg := range messages {
		log.Printf("[Sending] %s", msg)
		if err := stream.Send(&pb.ChatMessage{User: "ClientDev", Message: msg}); err != nil {
			log.Fatalf("Send error: %v", err)
		}
		time.Sleep(600 * time.Millisecond)
	}

	// Close client sending stream
	if err := stream.CloseSend(); err != nil {
		log.Fatalf("CloseSend error: %v", err)
	}

	// Block until the reader goroutine completes
	<-done
	log.Println("Bidirectional stream finished.")
}
