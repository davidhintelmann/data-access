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

type CCount struct {
	Country string
	Count   int64
}

type ProtocolQuery struct {
	Session_ID             string
	Most_Recent_Session_ID string
	Net_Transport          string
}

func QueryPerson(ctx context.Context, db *sql.DB) ([]CCount, error) {
	// Check if database is alive.
	// err := db.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database: " + err.Error())
	// }

	query := `SELECT TOP (6) [AdventureWorks2019Go].[Person].[CountryRegion].[Name] AS "Country"
	,COUNT([AdventureWorks2019Go].[Person].[Person].[BusinessEntityID]) AS "Business Sum"
	FROM [AdventureWorks2019Go].[Person].[Person]
	JOIN [AdventureWorks2019Go].[Person].[BusinessEntityAddress] ON [AdventureWorks2019Go].[Person].[BusinessEntityAddress].[BusinessEntityID] = [AdventureWorks2019Go].[Person].[Person].[BusinessEntityID]
	JOIN [AdventureWorks2019Go].[Person].[Address] ON [AdventureWorks2019Go].[Person].[Address].[AddressID] = [AdventureWorks2019Go].[Person].[BusinessEntityAddress].[AddressID]
	JOIN [AdventureWorks2019Go].[Person].[StateProvince] ON [AdventureWorks2019Go].[Person].[StateProvince].[StateProvinceID] = [AdventureWorks2019Go].[Person].[Address].[StateProvinceID]
	JOIN [AdventureWorks2019Go].[Person].[CountryRegion] ON [AdventureWorks2019Go].[Person].[CountryRegion].[CountryRegionCode] = [AdventureWorks2019Go].[Person].[StateProvince].[CountryRegionCode]
	GROUP BY [AdventureWorks2019Go].[Person].[CountryRegion].[Name]
	ORDER BY COUNT([AdventureWorks2019Go].[Person].[Person].[BusinessEntityID]) DESC`

	tsql := fmt.Sprintf(query)

	// Execute query
	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		log.Fatal("Error reading table: " + err.Error())
		return nil, err
	}

	defer rows.Close()

	// var row_count int = 0
	var ccount []CCount

	// Iterate through the result set.
	for rows.Next() {
		// var count, country string
		var cc CCount

		// Get values from row.
		// err := rows.Scan(&count, &country)
		if err := rows.Scan(&cc.Country, &cc.Count); err != nil {
			log.Fatal("Error reading rows: " + err.Error())
			return nil, err
		}
		ccount = append(ccount, cc)

		fmt.Printf("Country: %s, Count: %v\n", cc.Country, cc.Count)
	}

	return ccount, nil
}

func QueryPersonTimeit(ctx context.Context, db *sql.DB) ([]CCount, error) {
	// Check if database is alive.
	// err := db.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database: " + err.Error())
	// }

	query_start := time.Now()

	query := `SELECT TOP (6) [AdventureWorks2019Go].[Person].[CountryRegion].[Name] AS "Country"
	,COUNT([AdventureWorks2019Go].[Person].[Person].[BusinessEntityID]) AS "Business Sum"
	FROM [AdventureWorks2019Go].[Person].[Person]
	JOIN [AdventureWorks2019Go].[Person].[BusinessEntityAddress] ON [AdventureWorks2019Go].[Person].[BusinessEntityAddress].[BusinessEntityID] = [AdventureWorks2019Go].[Person].[Person].[BusinessEntityID]
	JOIN [AdventureWorks2019Go].[Person].[Address] ON [AdventureWorks2019Go].[Person].[Address].[AddressID] = [AdventureWorks2019Go].[Person].[BusinessEntityAddress].[AddressID]
	JOIN [AdventureWorks2019Go].[Person].[StateProvince] ON [AdventureWorks2019Go].[Person].[StateProvince].[StateProvinceID] = [AdventureWorks2019Go].[Person].[Address].[StateProvinceID]
	JOIN [AdventureWorks2019Go].[Person].[CountryRegion] ON [AdventureWorks2019Go].[Person].[CountryRegion].[CountryRegionCode] = [AdventureWorks2019Go].[Person].[StateProvince].[CountryRegionCode]
	GROUP BY [AdventureWorks2019Go].[Person].[CountryRegion].[Name]
	ORDER BY COUNT([AdventureWorks2019Go].[Person].[Person].[BusinessEntityID]) DESC`

	tsql := fmt.Sprintf(query)

	// Execute query
	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		log.Fatal("Error reading table: " + err.Error())
		return nil, err
	}

	query_end := time.Now()
	query_duration := query_end.Sub(query_start)
	fmt.Printf("Query duration: %v\n", query_duration)

	defer rows.Close()

	var ccount []CCount

	// display_start := time.Now()
	// Iterate through the result set.
	for rows.Next() {
		var cc CCount

		// Get values from row.
		if err := rows.Scan(&cc.Country, &cc.Count); err != nil {
			log.Fatal("Error reading rows: " + err.Error())
			return nil, err
		}
		ccount = append(ccount, cc)

		fmt.Printf("Country: %s, Count: %v\n", cc.Country, cc.Count)
	}

	// display_end := time.Now()
	// display_duration := display_end.Sub(display_start)
	// fmt.Printf("Display duration: %v\n", display_duration)

	return ccount, nil
}

func FormatQueryPerson(ccount []CCount) {
	for _, v := range ccount {
		fmt.Printf("Country: %s, Count: %v\n", v.Country, v.Count)
	}
}

func QueryProtocol(ctx context.Context, db *sql.DB) (*ProtocolQuery, error) {
	// Check if database is alive.
	// err := db.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database - SYS: " + err.Error())
	// }

	// query_start := time.Now()

	query := `SELECT session_id, most_recent_session_id, net_transport   
	FROM AdventureWorks2019Go.sys.dm_exec_connections   
	WHERE session_id = @@SPID;`

	tsql := fmt.Sprintf(query)

	// Execute query
	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		log.Fatal("Error reading table - SYS: " + err.Error())
		return nil, err
	}
	// query_end := time.Now()
	// query_duration := query_end.Sub(query_start)
	// fmt.Printf("Query duration - SYS: %v\n", query_duration)

	defer rows.Close()

	// var row_count int = 0
	var protocolq ProtocolQuery

	// display_start := time.Now()
	// Iterate through the result set.
	for rows.Next() {
		// var count, country string
		// var session_id, most_recent_session_id, net_transport string

		// Get values from row.
		err := rows.Scan(&protocolq.Session_ID, &protocolq.Most_Recent_Session_ID, &protocolq.Net_Transport)
		if err != nil {
			log.Fatal("Error reading rows - SYS: " + err.Error())
			return nil, err
		}
		fmt.Printf("Session: %s, Most Recent Session: %s, Protocol: %s\n", protocolq.Session_ID, protocolq.Most_Recent_Session_ID, protocolq.Net_Transport)
	}

	// display_end := time.Now()
	// display_duration := display_end.Sub(display_start)
	// fmt.Printf("Display duration - SYS: %v\n", display_duration)

	return &protocolq, nil
}

func QueryProtocolTimeit(ctx context.Context, db *sql.DB) (*ProtocolQuery, error) {
	// Check if database is alive.
	// err := db.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database - SYS: " + err.Error())
	// }

	query_start := time.Now()

	query := `SELECT session_id, most_recent_session_id, net_transport   
	FROM AdventureWorks2019Go.sys.dm_exec_connections   
	WHERE session_id = @@SPID;`

	tsql := fmt.Sprintf(query)

	// Execute query
	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		log.Fatal("Error reading table - SYS: " + err.Error())
		return nil, err
	}
	query_end := time.Now()
	query_duration := query_end.Sub(query_start)
	fmt.Printf("Query duration - SYS: %v\n", query_duration)

	defer rows.Close()

	var protocolq ProtocolQuery

	// display_start := time.Now()
	// Iterate through the result set.
	for rows.Next() {
		// Get values from row.
		err := rows.Scan(&protocolq.Session_ID, &protocolq.Most_Recent_Session_ID, &protocolq.Net_Transport)
		if err != nil {
			log.Fatal("Error reading rows - SYS: " + err.Error())
			return nil, err
		}
		fmt.Printf("Session: %s, Most Recent Session: %s, Protocol: %s\n", protocolq.Session_ID, protocolq.Most_Recent_Session_ID, protocolq.Net_Transport)
	}

	// display_end := time.Now()
	// display_duration := display_end.Sub(display_start)
	// fmt.Printf("Display duration - SYS: %v\n", display_duration)

	return &protocolq, nil
}
