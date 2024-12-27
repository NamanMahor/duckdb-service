package store

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	sql "github.com/NamanMahor/duckdb-service/db"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

var (
	ErrNotLeader = errors.New("not leader")
)

type Store interface {
	Execute(query string) (*sql.ExecuteResult, error)

	Query(query string) (*sql.QueryResult, error)

	Join(nodeID string, addr string) error

	Leader() string // http address of leader

	Stats() (map[string]interface{}, error)
}

// DistributedStore is a DuckDb database, where all changes are made via Raft consensus.
type DistributedStore struct {
	raftDir  string
	raftBind string
	raft     *raft.Raft // The consensus mechanism.

	dbDir string  // Path to database dir
	db    *sql.DB // The underlying duckdb.

	logger *log.Logger
}

func New(basePath, bind string) *DistributedStore {
	dbDir := filepath.Join(basePath, "duckdb")
	raftDir := filepath.Join(basePath, "raft")

	return &DistributedStore{
		raftDir:  raftDir,
		raftBind: bind,
		dbDir:    dbDir,
		logger:   log.New(os.Stdout, "[DistributedStore] ", log.LstdFlags),
	}
}

func (ds *DistributedStore) Open(enableSingle bool, serverID string) error {
	// should be create from snapshot
	if err := os.RemoveAll(ds.dbDir); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := os.MkdirAll(ds.raftDir, 0755); err != nil {
		return err
	}

	if err := os.MkdirAll(ds.dbDir, 0755); err != nil {
		return err
	}

	ds.logger.Println("Opening Duckdb at", ds.dbDir)
	db, err := sql.Open(ds.dbDir)
	if err != nil {
		return err
	}
	ds.db = db
	ds.logger.Println("Opened Duckdb at", ds.dbDir)

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(serverID)

	addr, err := net.ResolveTCPAddr("tcp", ds.raftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(ds.raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(ds.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(ds.raftDir, "raft.db"),
	})
	if err != nil {
		return fmt.Errorf("new bbolt store: %s", err)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, ds, boltDB, boltDB, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	ds.raft = ra
	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

// Close closes the store.
func (ds *DistributedStore) Close() error {
	if err := ds.db.Close(); err != nil {
		return err
	}
	f := ds.raft.Shutdown()
	if f.Error() != nil {
		return f.Error()
	}
	return nil
}

func (ds *DistributedStore) Leader() string {
	_, serverID := ds.raft.LeaderWithID()
	return strings.Split(string(serverID), "|")[1]
}

func (ds *DistributedStore) Stats() (map[string]interface{}, error) {
	dbStatus := map[string]interface{}{
		"path": ds.dbDir,
	}

	stat, err := os.Stat(ds.dbDir)
	if err != nil {
		return nil, err
	}
	dbStatus["size"] = stat.Size()

	status := map[string]interface{}{
		"raft":     ds.raft.Stats(),
		"leader":   ds.Leader(),
		"dbStatus": dbStatus,
	}
	return status, nil
}

type Command struct {
	SQL string `json:"sql"`
}

func (ds *DistributedStore) Execute(query string) (*sql.ExecuteResult, error) {
	if ds.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	c := &Command{
		SQL: query,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := ds.raft.Apply(b, raftTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		return nil, e.Error()
	}

	r := f.Response().(*fsmExecuteResponse)
	return r.result, r.error
}

func (ds *DistributedStore) Query(query string) (*sql.QueryResult, error) {
	r, err := ds.db.Query(query)
	return r, err
}

func (ds *DistributedStore) Join(nodeID string, addr string) error {
	ds.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := ds.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		ds.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				ds.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := ds.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := ds.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, time.Duration(time.Duration(30).Seconds()))
	if f.Error() != nil {
		return f.Error()
	}
	ds.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

type fsmExecuteResponse struct {
	result *sql.ExecuteResult
	error  error
}

// Apply applies a Raft log entry to the database.
func (ds *DistributedStore) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	r, err := ds.db.Execute(c.SQL)
	return &fsmExecuteResponse{result: r, error: err}
}

type fsmSnapshot struct {
	snapshotDir string
}

// raft ensure that Apply and snaphot are not call together
func (ds *DistributedStore) Snapshot() (raft.FSMSnapshot, error) {
	snapshotBaseDir := os.TempDir()
	snapshotDir, err := os.MkdirTemp(snapshotBaseDir, "duckdb_snapshot_*")
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %v", err)
	}
	_, err = ds.db.Query(fmt.Sprintf("EXPORT DATABASE '%s' (FORMAT PARQUET);", snapshotDir))
	if err != nil {
		return nil, fmt.Errorf("failed to export database: %v", err)
	}

	return &fsmSnapshot{snapshotDir: snapshotDir}, nil
}

func (ds *DistributedStore) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	var err error
	tmpDir, err := os.MkdirTemp("", "duckdb_restore_*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tarReader := tar.NewReader(snapshot)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return err
		}

		// Construct the full path for the file/directory
		targetPath := filepath.Join(tmpDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			outFile, err := os.Create(targetPath)
			if err != nil {
				return err
			}
			_, err = io.Copy(outFile, tarReader)
			outFile.Close()
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown type: %v in tar archive", header.Typeflag)
		}
	}

	// Use DuckDB's IMPORT DATABASE command to restore the database from the directory
	_, err = ds.db.Query(fmt.Sprintf("IMPORT DATABASE '%s';", tmpDir))
	if err != nil {
		return fmt.Errorf("failed to import database: %v", err)
	}

	return nil
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	tarWriter := tar.NewWriter(sink)
	defer tarWriter.Close()
	err := filepath.Walk(f.snapshotDir, func(file string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(f.snapshotDir, file)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, relPath)
		if err != nil {
			return err
		}
		header.Name = relPath

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		if !info.IsDir() {
			f, err := os.Open(file)
			if err != nil {
				return err
			}
			defer f.Close()

			_, err = io.Copy(tarWriter, f)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to archive snapshot directory: %v", err)
	}

	return sink.Close()
}

func (f *fsmSnapshot) Release() {
	os.RemoveAll(f.snapshotDir)
}
