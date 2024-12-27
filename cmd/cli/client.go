package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// QueryRequest represents the structure of the SQL query request
type QueryRequest struct {
	SQL string `json:"sql"`
}

type Result struct {
	RowsAffected int64           `json:"rows_affected,omitempty"`
	Columns      []string        `json:"columns,omitempty"`
	Types        []string        `json:"types,omitempty"`
	Values       [][]interface{} `json:"values,omitempty"`
}

// QueryResponse represents the structure of the query response
type QueryResponse struct {
	// Assuming the response contains a "result" field
	Result Result `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
	Took   int64  `json:"took,omitempty"`
}

// sendPostRequest handles sending a POST request with a SQL query to the given URL
func sendPostRequest(url string, query string) (*QueryResponse, error) {
	// Prepare the request payload
	payload := QueryRequest{SQL: query}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	// Create a new POST request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	// Set the necessary headers
	req.Header.Set("Content-Type", "application/json")

	// Initialize a custom HTTP client with a timeout and redirect policy
	client := &http.Client{
		Timeout: 30 * time.Second,
		// Custom redirect policy
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Reset the body for the redirected request
			if len(via) > 0 {
				req.Body = io.NopCloser(bytes.NewReader(body))
				req.Method = "POST" // Ensure the method stays as POST
			}
			return nil
		},
	}

	// Send the HTTP request
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read and parse the response body
	responseData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Check the response status code and handle errors if necessary
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-OK status code: %d, response: %s", resp.StatusCode, string(responseData))
	}

	// Parse the response into the QueryResponse struct
	var queryResponse QueryResponse
	err = json.Unmarshal(responseData, &queryResponse)
	if err != nil {
		return nil, err
	}

	// Return the parsed response
	return &queryResponse, nil
}

func main() {
	type url_query struct {
		url   string
		query string
	}
	// List of SQL queries to be executed
	urls_queries := []url_query{
		{url: "http://localhost:9301/db/execute?pretty", query: "CREATE TABLE abc (id integer not null primary key, name text)"},
		{url: "http://localhost:9303/db/execute?pretty", query: "CREATE TABLE def (id integer not null primary key, name text)"},
		{url: "http://localhost:9305/db/execute?pretty", query: "CREATE TABLE ghi (id integer not null primary key, name text)"},
		{url: "http://localhost:9301/db/execute?pretty", query: "INSERT INTO def(id,name) VALUES(1,'def')"},
		{url: "http://localhost:9303/db/execute?pretty", query: "INSERT INTO abc(id,name) VALUES(1,'abc')"},
		{url: "http://localhost:9305/db/execute?pretty", query: "INSERT INTO ghi(id,name) VALUES(1,'ghi')"},
		{url: "http://localhost:9301/db/query?pretty", query: "SELECT * FROM def"},
		{url: "http://localhost:9303/db/query?pretty", query: "SELECT * FROM ghi"},
		{url: "http://localhost:9305/db/query?pretty", query: "SELECT * FROM abc"},
	}

	// Execute each query
	for _, url_query := range urls_queries {
		url := url_query.url
		query := url_query.query
		fmt.Printf("Sending to : %s\n", url)
		fmt.Printf("Sending query: %s\n", query)
		response, err := sendPostRequest(url, query)
		if err != nil {
			log.Fatalf("Error executing query: %v\n", err)
		}
		jsonOutput, err := json.MarshalIndent(response, "", "  ")
		if err != nil {
			log.Fatalf("Error converting response to JSON: %v\n", err)
		}
		fmt.Println("Response as JSON: \n", string(jsonOutput))

	}
}
