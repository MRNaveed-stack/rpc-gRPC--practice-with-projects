package main

import (
	"context"
	"log"
	"time"

	pb "github.com/MRNaveed-stack/rpc-gRPC--practice-with-projects/proto/pb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	log.Println("=== SCENARIO 1: 1-Second Deadline (Expect Failure) ===")
	ctxTimeout, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel1()

	_, err = client.CreateTask(ctxTimeout, &pb.CreateTaskRequest{
		Title:       "Fast Task",
		Description: "Will run out of time",
	})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.DeadlineExceeded:
				log.Printf("[Client] Got expected DeadlineExceeded error: %s", st.Message())
			case codes.Canceled:
				log.Printf("[Client] Request was canceled: %s", st.Message())
			default:
				log.Printf("[Client] Unexpected code: %s: %s", st.Code(), st.Message())
			}
		} else {
			log.Fatalf("Non-gRPC error: %v", err)
		}
	}

	// -------------------------------------------------------------
	// SCENARIO 2: Sufficient deadline (5s limit, operation takes 3s)
	// -------------------------------------------------------------
	log.Println("\n=== SCENARIO 2: 5-Second Deadline (Expect Success) ===")
	ctxSufficient, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	res, err := client.CreateTask(ctxSufficient, &pb.CreateTaskRequest{
		Title:       "Patient Task",
		Description: "Has plenty of time to finish",
	})
	if err != nil {
		log.Fatalf("[Client] Failed unexpectedly: %v", err)
	}

	log.Printf("[Client] Created Task: %s (%s)", res.Task.Id, res.Task.Title)

	// FIXED: Realigned all blocks inside the single 'if err != nil' conditional block
	if err != nil {
		st, _ := status.FromError(err)
		log.Printf("Expected rejection! Code: %s | Message: %s\n", st.Code(), st.Message())

		for _, detail := range st.Details() {
			switch d := detail.(type) {
			case *errdetails.BadRequest:
				log.Println("Structured Field Violations Found:")
				for _, v := range d.GetFieldViolations() {
					log.Printf("  • Field: '%s' -> Issue: %s", v.GetField(), v.GetDescription())
				}
			default:
				log.Printf("Other detail type: %T", d)
			}
		}
	} else {
		log.Println("Unexpected success: Request without token passed!")
	}

	// TEST 2: Request with proper metadata token (Expect Success)
	log.Println("\n--- Test 2: Sending request with Bearer token ---")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	// Inject HTTP/2 headers into the context
	md := metadata.Pairs("authorization", "Bearer secret-vault-token-123")
	authCtx := metadata.NewOutgoingContext(ctx2, md)

	res, err = client.CreateTask(authCtx, &pb.CreateTaskRequest{
		Title:       "Production Task",
		Description: "Authenticated via gRPC metadata",
	})
	if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}

	log.Printf("Success! Created Task ID: %s | Title: %s\n", res.Task.Id, res.Task.Title)
}
