package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type DB struct {
	conn *sql.DB
}

type ItemModel struct {
	ID       int64
	SKU      string
	Name     string
	Quantity int32
	Price    float64
}

func InitDB(connStr string) (*DB, error) {
	// Driver name registered by pgx/v5/stdlib is "pgx"
	conn, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	// Configure connection pool for production
	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(5 * time.Minute)

	// Verify database connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	// Create Postgres schema
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS items (
		id BIGSERIAL PRIMARY KEY,
		sku VARCHAR(64) UNIQUE NOT NULL,
		name VARCHAR(255) NOT NULL,
		quantity INT NOT NULL CHECK (quantity >= 0),
		price NUMERIC(10, 2) NOT NULL
	);`

	if _, err := conn.ExecContext(ctx, createTableQuery); err != nil {
		return nil, fmt.Errorf("failed creating schema: %w", err)
	}

	log.Println("PostgreSQL connection established and table verified.")
	return &DB{conn: conn}, nil
}

func (d *DB) InsertItem(ctx context.Context, item ItemModel) (*ItemModel, error) {
	// In PostgreSQL, use $1, $2 and RETURNING id instead of LastInsertId()
	query := `
		INSERT INTO items (sku, name, quantity, price) 
		VALUES ($1, $2, $3, $4) 
		RETURNING id;
	`
	err := d.conn.QueryRowContext(ctx, query, item.SKU, item.Name, item.Quantity, item.Price).Scan(&item.ID)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (d *DB) GetItemBySKU(ctx context.Context, sku string) (*ItemModel, error) {
	query := `SELECT id, sku, name, quantity, price FROM items WHERE sku = $1;`
	row := d.conn.QueryRowContext(ctx, query, sku)

	var it ItemModel
	if err := row.Scan(&it.ID, &it.SKU, &it.Name, &it.Quantity, &it.Price); err != nil {
		return nil, err
	}
	return &it, nil
}

func (d *DB) AdjustStock(ctx context.Context, sku string, delta int32) (*ItemModel, error) {
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// 1. Fetch current stock with Row-Level Locking (FOR UPDATE)
	// 'FOR UPDATE' prevents race conditions where two requests adjust stock simultaneously
	var it ItemModel
	query := `SELECT id, sku, name, quantity, price FROM items WHERE sku = $1 FOR UPDATE;`
	row := tx.QueryRowContext(ctx, query, sku)
	if err := row.Scan(&it.ID, &it.SKU, &it.Name, &it.Quantity, &it.Price); err != nil {
		return nil, err
	}

	// 2. Validate inventory count
	newQuantity := it.Quantity + delta
	if newQuantity < 0 {
		return nil, fmt.Errorf("insufficient stock: current %d, requested change %d", it.Quantity, delta)
	}

	// 3. Update stock inside transaction
	updateQuery := `UPDATE items SET quantity = $1 WHERE sku = $2;`
	if _, err := tx.ExecContext(ctx, updateQuery, newQuantity, sku); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	it.Quantity = newQuantity
	return &it, nil
}