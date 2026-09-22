package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "mini-project/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connection failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewInventoryServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Dynamic SKU per run
	testSKU := fmt.Sprintf("M3-PRO-%d", time.Now().Unix()%10000)

	// 1. Add Item into PostgreSQL
	log.Println("--- 1. Inserting Item to PostgreSQL ---")
	addResp, err := client.AddItem(ctx, &pb.AddItemRequest{
		Sku:      testSKU,
		Name:     "MacBook Pro 16 M3 Max",
		Quantity: 5,
		Price:    3499.99,
	})
	if err != nil {
		log.Fatalf("AddItem failed: %v", err)
	}
	log.Printf("Inserted into Postgres -> ID: %d | SKU: %s | Quantity: %d\n",
		addResp.Item.Id, addResp.Item.Sku, addResp.Item.Quantity)

	// 2. Query Item by SKU
	log.Println("\n--- 2. Querying PostgreSQL via gRPC ---")
	item, err := client.GetItem(ctx, &pb.GetItemRequest{Sku: testSKU})
	if err != nil {
		log.Fatalf("GetItem failed: %v", err)
	}
	log.Printf("Fetched: %s (Current Stock in DB: %d)\n", item.Name, item.Quantity)

	// 3. Adjust Stock (-2 units)
	log.Println("\n--- 3. Selling 2 Units (Atomic DB Transaction) ---")
	sold, err := client.UpdateStock(ctx, &pb.UpdateStockRequest{
		Sku:           testSKU,
		QuantityDelta: -2,
	})
	if err != nil {
		log.Fatalf("UpdateStock failed: %v", err)
	}
	log.Printf("Sale complete! Updated PostgreSQL Stock: %d\n", sold.Quantity)

	// 4. Over-Sell Test (-10 units when only 3 remain)
	log.Println("\n--- 4. Attempting Over-Sale (Should Fail) ---")
	_, err = client.UpdateStock(ctx, &pb.UpdateStockRequest{
		Sku:           testSKU,
		QuantityDelta: -10,
	})
	if err != nil {
		st, _ := status.FromError(err)
		log.Printf("Caught expected error -> Code: %s | Message: %s\n", st.Code(), st.Message())
	}
}