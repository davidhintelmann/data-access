package connect

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/microsoft/go-mssqldb/sharedmemory"
)

func ConnectMSSQL(
	ctx context.Context,
	db *sql.DB,
	driver string,
	server string,
	database string,
	trusted_connection bool,
	encrypt bool) (*sql.DB, error) {
	var err error

	connString := fmt.Sprintf("server=%s;database=%s;TrustServerCertificate=%v;encrypt=%v", server, database, trusted_connection, encrypt)
	db, err = sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: " + err.Error())
	}

	log.Printf("Connected!\n")

	// Ping database to see if it's still alive.
	// Important for handling network issues and long queries.
	// err = db.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database: " + err.Error())
	// }

	// version_start := time.Now()
	// SelectVersion()
	// version_end := time.Now()
	// version_duration := version_end.Sub(version_start)
	// fmt.Printf("Select version duration: %v\n", version_duration)

	return db, nil
}

func ConnectMSSQLTimeit(
	ctx context.Context,
	db *sql.DB,
	driver string,
	server string,
	database string,
	trusted_connection bool,
	encrypt bool) (*sql.DB, error) {
	connect_start := time.Now()
	var err error

	connString := fmt.Sprintf("server=%s;database=%s;TrustServerCertificate=%v;encrypt=%v", server, database, trusted_connection, encrypt)
	db, err = sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: " + err.Error())
	}

	log.Printf("Connected!\n")

	// Ping database to see if it's still alive.
	// Important for handling network issues and long queries.
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal("Error pinging database: " + err.Error())
	}
	connect_end := time.Now()
	connect_duration := connect_end.Sub(connect_start)
	fmt.Printf("Connect to MS SQL Server duration: %v\n", connect_duration)
	return db, nil
}

func ConnectMSSQLVersion(
	ctx context.Context,
	db *sql.DB,
	driver string,
	server string,
	database string,
	trusted_connection bool,
	encrypt bool) (*sql.DB, error) {
	var err error

	connString := fmt.Sprintf("server=%s;database=%s;TrustServerCertificate=%v;encrypt=%v", server, database, trusted_connection, encrypt)
	db, err = sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: " + err.Error())
	}

	log.Printf("Connected!\n")

	// Ping database to see if it's still alive.
	// Important for handling network issues and long queries.
	// err = db.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database: " + err.Error())
	// }

	SelectVersion(ctx, db)

	return db, nil
}

func ConnectMSSQLVersionTimeit(
	ctx context.Context,
	db *sql.DB,
	driver string,
	server string,
	database string,
	trusted_connection bool,
	encrypt bool) (*sql.DB, error) {
	connect_start := time.Now()
	var err error

	connString := fmt.Sprintf("server=%s;database=%s;TrustServerCertificate=%v;encrypt=%v", server, database, trusted_connection, encrypt)
	db, err = sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: " + err.Error())
	}

	log.Printf("Connected!\n")

	// Ping database to see if it's still alive.
	// Important for handling network issues and long queries.
	// err = db.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database: " + err.Error())
	// }

	version_start := time.Now()
	SelectVersion(ctx, db)
	version_end := time.Now()
	version_duration := version_end.Sub(version_start)
	fmt.Printf("Select version duration: %v\n", version_duration)

	connect_end := time.Now()
	connect_duration := connect_end.Sub(connect_start)
	fmt.Printf("Connect to MS SQL Server duration: %v\n", connect_duration)
	return db, nil
}

// Gets and prints SQL Server version
func SelectVersion(ctx context.Context, db *sql.DB) {
	var err error
	// Ping database to see if it's still alive.
	// Important for handling network issues and long queries.
	// err := db.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database: " + err.Error())
	// }

	var result string

	// Run query and scan for result
	err = db.QueryRowContext(ctx, "SELECT @@version").Scan(&result)
	if err != nil {
		log.Fatal("Scan failed:", err.Error())
	}
	fmt.Printf("%s\n", result)
}
