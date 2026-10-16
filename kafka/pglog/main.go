package main

import (
	"context"

	pgl "github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

)

type Event struct {
	Op string
}

func main() {
	ctx := context.Background()

	conn, err := pgconn.Connect(ctx, "postgresql://postgres:secret@127.0.0.1:5433/pglog?sslmode=disable&replication=database")
	if err != nil {
		panic(err)
	}

	_, err = pgl.CreateReplicationSlot(ctx, conn, "my_slot", "pgoutput",
		pgl.CreateReplicationSlotOptions{
			Temporary: true,
			Mode:      pgl.LogicalReplication,
		})
	if err != nil {
		panic(err)
	}

	err = pgl.StartReplication(ctx, conn, "my_slot", 0, 
		pgl.StartReplicationOptions{
			Mode: pgl.LogicalReplication,
	})
	if err != nil {
		panic(err)
	}
}
