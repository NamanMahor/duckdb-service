# DuckDB-Service Project
This project was a significant learning experience for me as it was my first time working with technologies like Go, Raft, and DuckDB. Despite spending only two days to develop this project, I managed to implement a functional prototype. However, there are still several aspects that need improvement, which are outlined in the TODO list.


## Details 
db/db.go in file open duck db and execute and query duck db. we donot need connection pool because sql/DB maintains its own pool.

http/service.go in this we have endpoint and its handler define. each hander do some check and forward request to store

store/store.go in this have Store Interface defined and DistributedStore implement these Store interface and raft interface. raft have 1 main method `Apply`. when ever we to any write operation we will call raft.Apply which will then called on every node. There are other method like Snapshot() Restore() etc. 
Very good detail about Apply is mentioned here [harshicorp-raft-appply](https://github.com/hashicorp/raft/blob/main/docs/apply.md)



## Features
- Designed to make DuckDB a robust, fault-tolerant, and distributed system.
- Supports running multiple DuckDB instances that remain synchronized.
- Implements eventual consistency for reads. While strong consistency can be enforced, it requires routing reads through Raft like writes, which could impact performance.
- Scales the cluster to enhance read performance.
- Write operations are performed only on the leader node. However, the server supports request redirection, allowing clients to send write requests to any node, which will redirect them to the leader.
- Utilizes Raft for maintaining logs of write operations. To prevent unbounded log growth, the system snapshots the database state during log truncation, as managed by Raft.


## TODO & Ideas
- Bulk Api support can be added, we can utilize db transaction for that.
- Make the database and Raft configuration configurable.
- Currently, creating a cluster requires starting one node with specific options to establish it as the leader. We can improve this process by introducing a `-bootstrap-server $HOST1:9301,$HOST2:9301,$HOST3:9301` option. Before starting the Raft server, it would wait for all bootstrap servers to connect, determine the leader, and proceed accordingly. (Alternatively, we could use etcd or Consul for leader selection.)
- Currently, new nodes can be added by running additional instances, but node removal is not supported. The system will continue working if a node is down, but Raft will repeatedly try to ping the unreachable node. Implementing a mechanism to remove dead nodes after some time would improve efficiency.
- The current implementation uses the `Export Database` command to snapshot the database. However, it might be possible to simply copy the database directory (this needs testing). Since Raft ensures no write operations occur during snapshotting, this approach could simplify the process.
- Need to add unit test and system test.         
- Use Multi-Raft([dragonboad](https://github.com/lni/dragonboat) or [etcd-raft](https://github.com/etcd-io/raft)) and partition  to support write accross multiple node 
- Handle non-deterministic Fuction like now() and random() which can execute in different node diffrently. we can rewrite the sql for this issue.



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