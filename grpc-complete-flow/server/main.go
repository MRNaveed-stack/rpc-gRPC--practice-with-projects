package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/MRNaveed-stack/rpc-gRPC--practice-with-projects/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedTodoServiceServer
	mu    sync.Mutex
	tasks map[string]*pb.Task
	count int
}

func (s *server) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.CreateTaskResponse, error) {
	if req.GetTitle() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "task title cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.count++
	taskID := fmt.Sprintf("TASK-%d", s.count)

	task := &pb.Task{
		Id:          taskID,
		Title:       req.GetTitle(),
		Description: req.GetDescription(),
		Completed:   false,
	}

	s.tasks[taskID] = task
	log.Printf("Created task: %s - %s", task.Id, task.Title)

	return &pb.CreateTaskResponse{Task: task}, nil
}

func (s *server) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[req.GetId()]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "task with ID %s not found", req.GetId())
	}

	return task, nil
}

func (s *server) ListTasks(ctx context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var taskList []*pb.Task
	for _, task := range s.tasks {
		taskList = append(taskList, task)
	}

	return &pb.ListTasksResponse{Tasks: taskList}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	todoServer := &server{
		tasks: make(map[string]*pb.Task),
	}

	pb.RegisterTodoServiceServer(grpcServer, todoServer)

	log.Println("gRPC Server listening on :50051...")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}