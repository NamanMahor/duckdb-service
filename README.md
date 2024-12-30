# DuckDB-Service Project
This project was a significant learning experience for me as it was my first time working with technologies like Go, Raft, and DuckDB. Despite spending only two days developing this project, I managed to implement a functional prototype. However, several aspects still need improvement, which are outlined in the TODO list.

## Details

- **`db/db.go`**: This file opens DuckDB and executes queries on it. We do not need a connection pool because `sql/DB` maintains its own pool.
- **`http/service.go`**: This file defines the endpoints and their handlers. Each handler performs some checks and forwards requests to the store.
- **`store/store.go`**: This file contains the `Store` interface, which is implemented by `DistributedStore`. `DistributedStore` implements both the `Store` interface and the Raft interface. Raft has a main method, `Apply`, which is called whenever a write operation is performed. This operation propagates to every node. Other methods include `Snapshot()`, `Restore()`, etc. Detailed information about `Apply` can be found in the [HashiCorp Raft Apply documentation](https://github.com/hashicorp/raft/blob/main/docs/apply.md).

## Features

- Designed to make DuckDB a robust, fault-tolerant, and distributed system.
- Supports running multiple DuckDB instances that remain synchronized.
- Implements eventual consistency for reads. While strong consistency can be enforced, it requires routing reads through Raft, similar to writes, which could impact performance.
- Scales the cluster to enhance read performance.
- Write operations are performed only on the leader node. However, the server supports request redirection, allowing clients to send write requests to any node, which will redirect them to the leader.
- Utilizes Raft for maintaining logs of write operations. To prevent unbounded log growth, the system snapshots the database state during log truncation, as managed by Raft.

## TODO & Ideas

- Add bulk API support by utilizing database transactions.
- Make the database and Raft configuration configurable.
- Simplify cluster creation by introducing a `-bootstrap-server $HOST1:9301,$HOST2:9301,$HOST3:9301` option. Before starting the Raft server, it would wait for all bootstrap servers to connect, determine the leader, and proceed accordingly. (Alternatively, use etcd or Consul for leader selection.)
- Support node removal. Currently, the system works if a node is down, but Raft repeatedly pings the unreachable node. Implementing a mechanism to remove dead nodes after some time would improve efficiency.
- Optimize the database snapshot process. The current implementation uses the `Export Database` command, but it might be possible to copy the database directory directly. Since Raft ensures no write operations occur during snapshotting, this approach could simplify the process.
- Add unit tests and system tests.
- Implement Multi-Raft ([Dragonboat](https://github.com/lni/dragonboat) or [etcd-raft](https://github.com/etcd-io/raft)) and partitioning to support writes across multiple nodes.
- Handle non-deterministic functions like `now()` and `random()`, which can execute differently on different nodes. Rewrite SQL queries to address this issue.

## Endpoints

The current version of the DuckDB-Service project provides the following endpoints:

### `/db/execute`

- Used for `CREATE`, `INSERT`, and `UPDATE` statements.
- Example:
  ```bash
  curl -v -L --post301 -XPOST 'localhost:9301/db/execute?pretty' \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "INSERT INTO abc(id, name) VALUES (1, \"abc\")"
  }'
   ```

### `/db/query`
- Used for `SELECT` queries.
- Example:
```bash
curl -v -L --post301 'localhost:9301/db/query?pretty' \
-H "Content-Type: application/json" \
-d '{
  "sql": "SELECT * FROM def"
}'
```

### `/join`
- Allows a new node to join the cluster.

### `/status`
- Retrieves the status of the current node.


## Starting the Server
To start the server, use the following commands:

### Node 1
```bash
./main -id node1 -http localhost:9301 -raft localhost:9302 ./.data/node1
```

### Node 2
```bash
./main -id node2 -http localhost:9303 -raft localhost:9304 -leader localhost:9301 ./.data/node2
```

### Node 3
```bash
./main -id node3 -http localhost:9305 -raft localhost:9306 -leader localhost:9301 ./.data/node3
```

## Client 
This client performs the following operations:
- Creates three different tables by calling three separate server addresses.
- Executes three `INSERT` operations by calling different servers.
- Query three `SELECT` operations by calling different servers 
- Print response for each query prints the results.

This is Just to show the write operation no one server is visible on other and we can read from any where

### Running the Client
```bash
./cmd/cli/client
```