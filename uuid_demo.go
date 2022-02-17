package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func setup(db *sql.DB) error {
	fmt.Println("Setting up tables")
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	statements := []string{
		"DROP TABLE IF EXISTS uuid_demo_1",
		"DROP TABLE IF EXISTS uuid_demo_2",
		`CREATE TABLE uuid_demo_1 (
			uuid VARBINARY(16) PRIMARY KEY CLUSTERED,
			c1 VARCHAR(255) NOT NULL
		)`,
		`CREATE TABLE uuid_demo_2 (
			uuid VARBINARY(16) PRIMARY KEY CLUSTERED,
			c1 VARCHAR(255) NOT NULL
		)`,
	}
	for _, stmt := range statements {
		_, err := db.ExecContext(ctx, stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func runTest(wg *sync.WaitGroup, table string) {
	fmt.Printf("Starting test on %s\n", table)

	defer wg.Done()

	db, err := sql.Open("mysql", os.Getenv("DB_DSN"))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	batchSize := 500
	c1 := strings.Repeat("x", 255)
	for i := 1; i < 2000000; i++ {
		ctx, _ := context.WithTimeout(ctx, 5*time.Second)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			panic(err)
		}

		values := []interface{}{}
		placeholders := make([]string, 0)
		for j := 1; j <= batchSize; j++ {
			values = append(values, c1)
			if table == "uuid_demo_1" {
				placeholders = append(placeholders, "(UUID_TO_BIN(UUID(),0), ?)")
			} else {
				placeholders = append(placeholders, "(UUID_TO_BIN(UUID(),1), ?)")
			}
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO "+table+" VALUES"+strings.Join(placeholders, ","), values...)
		if err != nil {
			panic(err)
		}
		tx.Commit()
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	db, err := sql.Open("mysql", os.Getenv("DB_DSN"))
	if err != nil {
		panic(err)
	}

	err = setup(db)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go runTest(&wg, "uuid_demo_1")
	go runTest(&wg, "uuid_demo_2")
	wg.Wait()

	db.Close()
}
