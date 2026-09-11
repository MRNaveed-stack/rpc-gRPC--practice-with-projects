package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

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

// 1. Unary RPC
func (s *server) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.CreateTaskResponse, error) {
	if req.GetTitle() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "task title cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.count++
	id := fmt.Sprintf("TASK-%d", s.count)
	task := &pb.Task{
		Id:          id,
		Title:       req.GetTitle(),
		Description: req.GetDescription(),
		Completed:   false,
	}
	s.tasks[id] = task

	log.Printf("[Unary] Created: %s (%s)", id, task.Title)
	return &pb.CreateTaskResponse{Task: task}, nil
}

// 2. Server Streaming RPC
func (s *server) StreamTasks(req *pb.StreamTasksRequest, stream pb.TodoService_StreamTasksServer) error {
	s.mu.Lock()
	tasks := make([]*pb.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		if req.GetIncompleteOnly() && t.Completed {
			continue
		}
		tasks = append(tasks, t)
	}
	s.mu.Unlock()

	log.Printf("[Server Streaming] Sending %d tasks to client...", len(tasks))
	for _, task := range tasks {
		time.Sleep(500 * time.Millisecond) // simulate live streaming interval
		if err := stream.Send(task); err != nil {
			return status.Errorf(codes.Internal, "failed to stream task: %v", err)
		}
	}
	return nil
}

// 3. Client Streaming RPC
func (s *server) UploadTasks(stream pb.TodoService_UploadTasksServer) error {
	var ids []string
	log.Println("[Client Streaming] Ingesting tasks stream from client...")

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.UploadTasksSummary{
				TotalCreated: int32(len(ids)),
				TaskIds:      ids,
			})
		}
		if err != nil {
			return status.Errorf(codes.Unknown, "error reading stream: %v", err)
		}

		s.mu.Lock()
		s.count++
		id := fmt.Sprintf("TASK-%d", s.count)
		s.tasks[id] = &pb.Task{
			Id:          id,
			Title:       req.GetTitle(),
			Description: req.GetDescription(),
			Completed:   false,
		}
		ids = append(ids, id)
		s.mu.Unlock()

		log.Printf("[Client Streaming] Received and saved: %s (%s)", id, req.GetTitle())
	}
}

// 4. Bidirectional Streaming RPC
func (s *server) TaskChat(stream pb.TodoService_TaskChatServer) error {
	log.Println("[BiDi Streaming] Interactive chat channel connected")
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		log.Printf("[BiDi Streaming] Message from %s: %s", in.GetUser(), in.GetMessage())

		// Echo reply back
		reply := &pb.ChatMessage{
			User:    "Server-Bot",
			Message: fmt.Sprintf("Echoing: '%s' from %s", in.GetMessage(), in.GetUser()),
		}

		if err := stream.Send(reply); err != nil {
			return err
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	s := &server{
		tasks: make(map[string]*pb.Task),
	}

	// Seed with a couple tasks
	s.tasks["TASK-1"] = &pb.Task{Id: "TASK-1", Title: "Read gRPC docs", Description: "Basics of HTTP/2", Completed: true}
	s.tasks["TASK-2"] = &pb.Task{Id: "TASK-2", Title: "Practice Streaming", Description: "Implement all 3 streaming methods", Completed: false}
	s.count = 2

	pb.RegisterTodoServiceServer(grpcServer, s)

	log.Println("gRPC Server listening on port :50051...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}