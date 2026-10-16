// restaurant-oltp generates dummy OLTP data for a restaurant.
//
// Usage:
//
//	go run . -init -orders 5000   # one-time: create schema, seed reference data, backfill history
//	go run .                      # stream live orders like a real POS feed (Ctrl+C to stop)
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"restaurant-oltp/db"
	"restaurant-oltp/generator"
)

func main() {
	initMode := flag.Bool("init", false, "initialize: create schema, seed reference data, backfill historical orders, then exit")
	orders := flag.Int("orders", 5000, "number of historical orders to backfill in -init mode")
	rate := flag.Float64("rate", 5, "orders per second in stream mode")
	dsn := flag.String("dsn", "", "postgres connection URL (default: $DATABASE_URL or local docker compose default)")
	flag.Parse()

	url := *dsn
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		url = "postgres://restaurant:restaurant@localhost:5433/restaurant?sslmode=disable"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, url)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	log.Println("connected to postgres")

	if err := db.Migrate(ctx, pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	gen := generator.New(pool)

	if *initMode {
		if err := gen.SeedReference(ctx); err != nil {
			log.Fatalf("seed: %v", err)
		}
		if *orders > 0 {
			if err := gen.Backfill(ctx, *orders); err != nil {
				log.Fatalf("backfill: %v", err)
			}
		}
		log.Println("init complete. run without -init to stream live orders.")
		return
	}

	// Stream mode: refuse to run against an empty database.
	var count int64
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM customers").Scan(&count); err != nil {
		log.Fatalf("check data: %v", err)
	}
	if count == 0 {
		fmt.Fprintln(os.Stderr, "database is empty. run `go run . -init` first to seed and backfill data.")
		os.Exit(1)
	}

	log.Printf("streaming orders at ~%.1f/sec (Ctrl+C to stop)", *rate)
	if err := gen.Stream(ctx, *rate); err != nil {
		log.Fatalf("stream: %v", err)
	}
}
