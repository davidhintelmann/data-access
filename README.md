# Example using Go Lang to Query Microsoft SQL

This repo contains an example of how one can query a local instance of Microsoft SQL using the [Go programming language](https://go.dev/)

## Dependencies and Installation

Using this repo requires a bit of setup:
- Requires Go 1.8 or above
- Download developer version of [Microsoft SQL](https://www.microsoft.com/en-us/sql-server/sql-server-downloads)
	- Requires in [ODBC driver](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16) which should have been installed with MS SQL
- Installing this [pure Go](https://github.com/microsoft/go-mssqldb) driver for Go's `database/sql` package
	- repo above is forked from [denisenkom](https://github.com/denisenkom/go-mssqldb)

**Optional**
- [Download SQL Server Management Studio (SSMS)](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver16)
- Need sample data to query and one can restore a `.bat` backup for the [Adventure Works](https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver16&tabs=ssms) OLTP database
