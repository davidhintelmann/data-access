package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/davidhintelmann/data-access/connect"

	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/microsoft/go-mssqldb/sharedmemory"
)

// server, database, driver configuration
var server, database, driver = "lpc:localhost", "AdventureWorks2019", "mssql" // "sqlserver" or "mssql"

// trusted connection, and encryption configuraiton
var trusted_connection, encrypt = true, true

// conn is global variable to pass between functions
var conn *sql.DB

// Use background context globally to pass between functions
var ctx = context.Background()

// How many times to query MS SQL Server
var iterations = 1

func main() {
	log.SetFlags(log.Ldate | log.Lshortfile)

	drivers_list := sql.Drivers()
	fmt.Printf("List of Drivers: %v\n", drivers_list)
	fmt.Println("")

	var times []time.Duration

	for i := 0; i < iterations; i++ {
		times = append(times, TimeProgram(ctx))
	}

	for i := 0; i < iterations; i++ {
		fmt.Println(times[i])
	}
}

func TimeProgram(ctx context.Context) time.Duration {
	start := time.Now()

	conn, _ := connect.ConnectMSSQLVersion(ctx, conn, driver, server, database, trusted_connection, encrypt)
	defer conn.Close()

	if _, err := connect.QueryProtocolTimeit(ctx, conn, database); err != nil {
		log.Fatal("Error while executing query to AdentureWorks DB: " + err.Error())
	}

	if ccount, err := connect.QueryPerson(ctx, conn, database); err != nil {
		log.Fatal("Error while executing query to AdentureWorks DB: " + err.Error())
	} else {
		connect.FormatQueryPerson(ccount)
	}

	end := time.Now()
	duration_main := end.Sub(start)

	// Print duration of main loop
	fmt.Printf("Duration main: %v\n", duration_main)
	return duration_main
}
