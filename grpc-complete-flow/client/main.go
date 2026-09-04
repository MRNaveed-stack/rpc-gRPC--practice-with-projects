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
	// Connect to gRPC server using insecure credentials (plaintext HTTP/2 for development)
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewTodoServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Create a task
	created, err := client.CreateTask(ctx, &pb.CreateTaskRequest{
		Title:       "Set up gRPC repository",
		Description: "Configure protobuf compiler, write server and client stubs",
	})
	if err != nil {
		log.Fatalf("Error creating task: %v", err)
	}
	log.Printf("Successfully created task: ID=%s, Title=%s", created.Task.Id, created.Task.Title)

	// 2. Query the task back by ID
	fetched, err := client.GetTask(ctx, &pb.GetTaskRequest{Id: created.Task.Id})
	if err != nil {
		log.Fatalf("Error fetching task: %v", err)
	}
	log.Printf("Fetched Task: %s | Description: %s", fetched.Title, fetched.Description)

	// 3. List all tasks
	list, err := client.ListTasks(ctx, &pb.ListTasksRequest{})
	if err != nil {
		log.Fatalf("Error listing tasks: %v", err)
	}
	log.Printf("Total tasks on server: %d", len(list.Tasks))
	for _, t := range list.Tasks {
		log.Printf(" - [%s] %s (Completed: %t)", t.Id, t.Title, t.Completed)
	}
}