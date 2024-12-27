package http

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/NamanMahor/duckdb-service/store"
)

type ClientRequest struct {
	SQL string `json:"sql"`
}

type Response struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
	Took   float64     `json:"took,omitempty"`
}

// Service provides HTTP service.
type Service struct {
	addr string       // Bind address of the HTTP service.
	ln   net.Listener // Service listener

	store store.Store // The Raft-backed database store.

	start time.Time // Start up time.
}

// New returns an uninitialized HTTP service.
func New(addr string, store store.Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
		start: time.Now(),
	}
}

// Start starts the service.
func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Printf("Error starting server: %v", err)
		return err
	}
	s.ln = ln

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			log.Printf("Error serving HTTP requests: %v", err)
			panic("Failed to start HTTP server")
		}
	}()

	log.Printf("Service started on %s", s.addr)
	return nil
}

// Close closes the service.
func (s *Service) Close() {
	if err := s.ln.Close(); err != nil {
		log.Printf("Error closing listener: %v", err)
	}
	log.Println("Service stopped")
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received %s request for %s", r.Method, r.URL.Path)

	switch {
	case strings.HasPrefix(r.URL.Path, "/db/execute"):
		s.handleExecute(w, r)
	case strings.HasPrefix(r.URL.Path, "/db/query"):
		s.handleQuery(w, r)
	case strings.HasPrefix(r.URL.Path, "/join"):
		s.handleJoin(w, r)
	case strings.HasPrefix(r.URL.Path, "/status"):
		s.handleStatus(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
		log.Printf("404 Not Found: %s", r.URL.Path)
	}
}

// handleJoin handles cluster-join requests from other nodes.
func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	log.Println("Handling join request")

	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	m := map[string]string{}
	if err := json.Unmarshal(b, &m); err != nil {
		log.Printf("Error unmarshalling JSON: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 2 {
		log.Printf("Invalid join request: expected 2 parameters, got %d", len(m))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		log.Println("Missing 'addr' in join request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	nodeID, ok := m["id"]
	if !ok {
		log.Println("Missing 'id' in join request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.Join(nodeID, remoteAddr); err != nil {
		log.Printf("Error joining node: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Printf("Node %s joined with address %s", nodeID, remoteAddr)
}

// handleStatus returns status on the system.
func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		log.Printf("Invalid method %s for /status", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	log.Println("Handling status request")

	results, err := s.store.Stats()
	if err != nil {
		log.Printf("Error fetching stats: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	httpStatus := map[string]interface{}{
		"addr": s.Addr().String(),
	}

	nodeStatus := map[string]interface{}{
		"start_time": s.start,
		"uptime":     time.Since(s.start).String(),
	}

	// Build the status response.
	status := map[string]interface{}{
		"store": results,
		"http":  httpStatus,
		"node":  nodeStatus,
	}

	pretty, _ := isPretty(r)
	var b []byte
	if pretty {
		b, err = json.MarshalIndent(status, "", "    ")
	} else {
		b, err = json.Marshal(status)
	}
	if err != nil {
		log.Printf("Error marshalling status response: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		_, err = w.Write([]byte(b))
		if err != nil {
			log.Printf("Error writing status response: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// handleExecute handles queries that modify the database.
func (s *Service) handleExecute(w http.ResponseWriter, r *http.Request) {
	log.Println("Handling execute request")

	if r.Method != "POST" {
		log.Printf("Invalid method %s for /db/execute", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		http.Error(w, "Only Post is Allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := Response{}
	start := time.Now()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body.Close()

	var clientRequest ClientRequest
	if err := json.Unmarshal(b, &clientRequest); err != nil {
		log.Printf("Error unmarshalling request body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query := clientRequest.SQL
	if query == "" {
		log.Println("Empty SQL query")
		http.Error(w, "SQL query is empty", http.StatusBadRequest)
		return
	}

	result, err := s.store.Execute(query)
	if err != nil {
		if err == store.ErrNotLeader {
			url := fmt.Sprintf("http://%s%s", s.store.Leader(), r.URL.Path)
			http.Redirect(w, r, url, http.StatusMovedPermanently)
			return
		}
		resp.Error = err.Error()
		log.Printf("Error executing query: %v", err)
	} else {
		resp.Result = result
	}
	resp.Took = float64(time.Since(start).Milliseconds())
	writeResponse(w, r, &resp)
}

// handleQuery handles queries that do not modify the database.
func (s *Service) handleQuery(w http.ResponseWriter, r *http.Request) {
	log.Println("Handling query request")

	if r.Method != "GET" && r.Method != "POST" {
		log.Printf("Invalid method %s for /db/query", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	resp := Response{}
	start := time.Now()

	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	r.Body.Close()

	var clientRequest ClientRequest
	if err := json.Unmarshal(b, &clientRequest); err != nil {
		log.Printf("Error unmarshalling request body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query := clientRequest.SQL
	if query == "" {
		log.Println("Empty SQL query")
		http.Error(w, "SQL query is empty", http.StatusBadRequest)
		return
	}

	result, err := s.store.Query(query)
	if err != nil {
		resp.Error = err.Error()
		log.Printf("Error querying database: %v", err)
	} else {
		resp.Result = result
	}
	resp.Took = float64(time.Since(start).Milliseconds())
	writeResponse(w, r, &resp)
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}

func writeResponse(w http.ResponseWriter, r *http.Request, j *Response) {
	log.Println("Writing response")

	var b []byte
	var err error
	pretty, _ := isPretty(r)

	if pretty {
		b, err = json.MarshalIndent(j, "", "    ")
	} else {
		b, err = json.Marshal(j)
	}

	if err != nil {
		log.Printf("Error marshalling response: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(b)
	if err != nil {
		log.Printf("Error writing response: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// queryParam returns whether the given query param is set to true.
func queryParam(req *http.Request, param string) (bool, error) {
	err := req.ParseForm()
	if err != nil {
		log.Printf("Error parsing form: %v", err)
		return false, err
	}
	if _, ok := req.Form[param]; ok {
		return true, nil
	}
	return false, nil
}

// isPretty returns whether the HTTP response body should be pretty-printed.
func isPretty(req *http.Request) (bool, error) {
	return queryParam(req, "pretty")
}
