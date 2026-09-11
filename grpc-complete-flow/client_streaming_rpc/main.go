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
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewTodoServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Println("Starting Client Streaming RPC...")
	stream, err := client.UploadTasks(ctx)
	if err != nil {
		log.Fatalf("UploadTasks call failed: %v", err)
	}

	tasksToBatchUpload := []*pb.CreateTaskRequest{
		{Title: "Bulk Item 1", Description: "First batch item"},
		{Title: "Bulk Item 2", Description: "Second batch item"},
		{Title: "Bulk Item 3", Description: "Third batch item"},
	}

	for i, t := range tasksToBatchUpload {
		log.Printf("Streaming task %d to server: %s", i+1, t.Title)
		if err := stream.Send(t); err != nil {
			log.Fatalf("Failed to send task over stream: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Close client sending stream and wait for single server acknowledgment
	summary, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Failed to receive stream summary: %v", err)
	}

	log.Printf("[Summary Response] Successfully uploaded %d tasks! IDs: %v", summary.GetTotalCreated(), summary.GetTaskIds())
}