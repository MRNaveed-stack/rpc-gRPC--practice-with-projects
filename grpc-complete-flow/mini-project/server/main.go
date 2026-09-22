package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net"
	"strings"

	pb "mini-project/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type inventoryServer struct {
	pb.UnimplementedInventoryServiceServer
	db *DB
}

func (s *inventoryServer) AddItem(ctx context.Context, req *pb.AddItemRequest) (*pb.AddItemResponse, error) {
	if req.GetSku() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "sku and name are required")
	}
	if req.GetPrice() < 0 {
		return nil, status.Error(codes.InvalidArgument, "price must be positive")
	}

	created, err := s.db.InsertItem(ctx, ItemModel{
		SKU:      req.GetSku(),
		Name:     req.GetName(),
		Quantity: req.GetQuantity(),
		Price:    req.GetPrice(),
	})
	if err != nil {
		// Detect Postgres unique key violation
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			return nil, status.Errorf(codes.AlreadyExists, "item SKU '%s' already exists", req.GetSku())
		}
		return nil, status.Errorf(codes.Internal, "database error: %v", err)
	}

	return &pb.AddItemResponse{
		Item: &pb.Item{
			Id:       created.ID,
			Sku:      created.SKU,
			Name:     created.Name,
			Quantity: created.Quantity,
			Price:    created.Price,
		},
	}, nil
}

func (s *inventoryServer) GetItem(ctx context.Context, req *pb.GetItemRequest) (*pb.Item, error) {
	if req.GetSku() == "" {
		return nil, status.Error(codes.InvalidArgument, "sku is required")
	}

	item, err := s.db.GetItemBySKU(ctx, req.GetSku())
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "item with SKU '%s' not found", req.GetSku())
		}
		return nil, status.Errorf(codes.Internal, "database error: %v", err)
	}

	return &pb.Item{
		Id:       item.ID,
		Sku:      item.SKU,
		Name:     item.Name,
		Quantity: item.Quantity,
		Price:    item.Price,
	}, nil
}

func (s *inventoryServer) UpdateStock(ctx context.Context, req *pb.UpdateStockRequest) (*pb.Item, error) {
	updated, err := s.db.AdjustStock(ctx, req.GetSku(), req.GetQuantityDelta())
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "SKU '%s' does not exist", req.GetSku())
		}
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}

	return &pb.Item{
		Id:       updated.ID,
		Sku:      updated.SKU,
		Name:     updated.Name,
		Quantity: updated.Quantity,
		Price:    updated.Price,
	}, nil
}

func main() {
	// Standard PostgreSQL connection URL
	dsn := "postgres://postgres:postgres@localhost:5432/inventory_db?sslmode=disable"

	database, err := InitDB(dsn)
	if err != nil {
		log.Fatalf("PostgreSQL init error: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen on :50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterInventoryServiceServer(grpcServer, &inventoryServer{db: database})

	log.Println("gRPC PostgreSQL Inventory Service running on port :50051...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}