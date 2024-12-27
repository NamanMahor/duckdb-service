package db

import (
	"database/sql"
	"log"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
)

type DB struct {
	dbConn *sql.DB
}

func Open(dbDir string) (*DB, error) {
	log.Printf("Opening database at %s", filepath.Join(dbDir, "duckdb.db"))
	dbc, err := sql.Open("duckdb", filepath.Join(dbDir, "duckdb.db"))
	if err != nil {
		log.Printf("Error opening database: %v", err)
		return nil, err
	}
	log.Printf("Database opened successfully.")
	return &DB{
		dbConn: dbc,
	}, nil
}

func (db *DB) Close() error {
	log.Printf("Closing database connection.")
	err := db.dbConn.Close()
	if err != nil {
		log.Printf("Error closing database connection: %v", err)
		return err
	}
	log.Printf("Database connection closed successfully.")
	return nil
}

type ExecuteResult struct {
	RowsAffected int64 `json:"rows_affected"`
}

type QueryResult struct {
	Columns []string        `json:"columns,omitempty"`
	Types   []string        `json:"types,omitempty"`
	Values  [][]interface{} `json:"values,omitempty"`
}

func (db *DB) Execute(query string) (*ExecuteResult, error) {
	log.Printf("Executing query: %s", query)
	result := &ExecuteResult{}
	r, err := db.dbConn.Exec(query, nil)
	if err != nil {
		log.Printf("Error executing query: %v", err)
		return nil, err
	}
	ra, err := r.RowsAffected()
	if err != nil {
		log.Printf("Error fetching rows affected: %v", err)
		return nil, err
	}
	result.RowsAffected = ra
	log.Printf("Query executed successfully. Rows affected: %d", ra)
	return result, nil
}

func (db *DB) Query(query string) (*QueryResult, error) {
	log.Printf("Executing query: %s", query)
	rows := &QueryResult{}
	rs, err := db.dbConn.Query(query, nil)
	if err != nil {
		log.Printf("Error executing query: %v", err)
		return nil, err
	}
	defer rs.Close()

	columns, err := rs.Columns()
	if err != nil {
		log.Printf("Error fetching columns: %v", err)
		return nil, err
	}
	rows.Columns = columns
	columnTypes, err := rs.ColumnTypes()
	if err != nil {
		log.Printf("Error fetching column types: %v", err)
		return nil, err
	}

	typeNames := make([]string, len(columnTypes))
	for i, colType := range columnTypes {
		typeNames[i] = colType.DatabaseTypeName()
	}
	rows.Types = typeNames

	for rs.Next() {
		dest := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range dest {
			pointers[i] = &dest[i]
		}

		if err := rs.Scan(pointers...); err != nil {
			log.Printf("Failed to scan row: %v", err)
			return nil, err
		}

		for i, v := range dest {
			if b, ok := v.([]uint8); ok {
				dest[i] = string(b)
			}
		}
		rows.Values = append(rows.Values, dest)
	}

	log.Printf("Query executed successfully with %d rows.", len(rows.Values))
	return rows, err
}
