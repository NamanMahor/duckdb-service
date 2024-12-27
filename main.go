package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"

	httpd "github.com/NamanMahor/duckdb-service/http"
	"github.com/NamanMahor/duckdb-service/store"
)

var httpAddr string   // http server address host:port
var raftAddr string   // raft communication address host:port
var leaderAddr string // leader address only pass by follower
var nodeID string     // nodeId

func init() {
	flag.StringVar(&httpAddr, "http", "localhost:9301", "HTTP query server bind address")
	flag.StringVar(&raftAddr, "raft", "localhost:9302", "Raft communication bind address")
	flag.StringVar(&leaderAddr, "leader", "", "host:port of leader to join")
	flag.StringVar(&nodeID, "id", "", "Node ID")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n\n", "duckdb service to support read write repilca")
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data directory>\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	// Ensure the data path is set.
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}

	dataPath := flag.Arg(0)

	// Create and open the store.
	basePath, err := filepath.Abs(dataPath)
	if err != nil {
		log.Fatalf("failed to determine absolute data path: %s", err.Error())
	}

	store := store.New(basePath, raftAddr)

	isLeader := (leaderAddr == "")
	serverID := nodeID + "|" + httpAddr
	err = store.Open(isLeader, serverID)
	if err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// If join was specified, make the join request.
	if !isLeader {
		if err := join(leaderAddr, raftAddr, serverID); err != nil {
			log.Fatalf("failed to join node at %s: %s", leaderAddr, err.Error())
		}
	}

	// Create the HTTP query server.
	s := httpd.New(httpAddr, store)
	if err := s.Start(); err != nil {
		log.Fatalf("failed to start HTTP server: %s", err.Error())

	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	if err := store.Close(); err != nil {
		log.Printf("failed to close store: %s", err.Error())
	}
	log.Println("duck-db server stopped")
}

func join(leaderAddr, raftAddr, serverID string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": serverID})
	if err != nil {
		log.Println("Error:", err)
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", leaderAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		log.Println("Error:", err)
		return err
	}
	defer resp.Body.Close()
	return nil
}
