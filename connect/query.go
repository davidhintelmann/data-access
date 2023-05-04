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

func QueryPerson(ctx context.Context, conn *sql.DB, database string) ([]CCount, error) {
	// Check if database is alive.
	// err := conn.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database: " + err.Error())
	// }

	query := `SELECT TOP (6) [%s].[Person].[CountryRegion].[Name] AS "Country"
	,COUNT([%s].[Person].[Person].[BusinessEntityID]) AS "Business Sum"
	FROM [%s].[Person].[Person]
	JOIN [%s].[Person].[BusinessEntityAddress] ON [%s].[Person].[BusinessEntityAddress].[BusinessEntityID] = [%s].[Person].[Person].[BusinessEntityID]
	JOIN [%s].[Person].[Address] ON [%s].[Person].[Address].[AddressID] = [%s].[Person].[BusinessEntityAddress].[AddressID]
	JOIN [%s].[Person].[StateProvince] ON [%s].[Person].[StateProvince].[StateProvinceID] = [%s].[Person].[Address].[StateProvinceID]
	JOIN [%s].[Person].[CountryRegion] ON [%s].[Person].[CountryRegion].[CountryRegionCode] = [%s].[Person].[StateProvince].[CountryRegionCode]
	GROUP BY [%s].[Person].[CountryRegion].[Name]
	ORDER BY COUNT([%s].[Person].[Person].[BusinessEntityID]) DESC`

	tsql := fmt.Sprintf(query, database, database, database,
		database, database, database, database, database, database, database,
		database, database, database, database, database, database, database)

	// Execute query
	rows, err := conn.QueryContext(ctx, tsql)
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

func QueryPersonTimeit(ctx context.Context, conn *sql.DB, database string) ([]CCount, error) {
	// Check if database is alive.
	// err := conn.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database: " + err.Error())
	// }

	query_start := time.Now()

	query := `SELECT TOP (6) [%s].[Person].[CountryRegion].[Name] AS "Country"
	,COUNT([%s].[Person].[Person].[BusinessEntityID]) AS "Business Sum"
	FROM [%s].[Person].[Person]
	JOIN [%s].[Person].[BusinessEntityAddress] ON [%s].[Person].[BusinessEntityAddress].[BusinessEntityID] = [%s].[Person].[Person].[BusinessEntityID]
	JOIN [%s].[Person].[Address] ON [%s].[Person].[Address].[AddressID] = [%s].[Person].[BusinessEntityAddress].[AddressID]
	JOIN [%s].[Person].[StateProvince] ON [%s].[Person].[StateProvince].[StateProvinceID] = [%s].[Person].[Address].[StateProvinceID]
	JOIN [%s].[Person].[CountryRegion] ON [%s].[Person].[CountryRegion].[CountryRegionCode] = [%s].[Person].[StateProvince].[CountryRegionCode]
	GROUP BY [%s].[Person].[CountryRegion].[Name]
	ORDER BY COUNT([%s].[Person].[Person].[BusinessEntityID]) DESC`

	tsql := fmt.Sprintf(query, database, database, database,
		database, database, database, database, database, database, database,
		database, database, database, database, database, database, database)

	// Execute query
	rows, err := conn.QueryContext(ctx, tsql)
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

func QueryProtocol(ctx context.Context, conn *sql.DB, database string) (*ProtocolQuery, error) {
	// Check if database is alive.
	// err := conn.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database - SYS: " + err.Error())
	// }

	// query_start := time.Now()

	query := `SELECT session_id, most_recent_session_id, net_transport   
	FROM %s.sys.dm_exec_connections   
	WHERE session_id = @@SPID;`

	tsql := fmt.Sprintf(query, database)

	// Execute query
	rows, err := conn.QueryContext(ctx, tsql)
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

func QueryProtocolTimeit(ctx context.Context, conn *sql.DB, database string) (*ProtocolQuery, error) {
	// Check if database is alive.
	// err := conn.PingContext(ctx)
	// if err != nil {
	// 	log.Fatal("Error pinging database - SYS: " + err.Error())
	// }

	query_start := time.Now()

	query := `SELECT session_id, most_recent_session_id, net_transport   
	FROM %s.sys.dm_exec_connections   
	WHERE session_id = @@SPID;`

	tsql := fmt.Sprintf(query, database)

	// Execute query
	rows, err := conn.QueryContext(ctx, tsql)
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
