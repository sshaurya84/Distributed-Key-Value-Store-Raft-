# Distributed Key-Value Store (Raft)

A distributed key-value store built in Go that keeps data consistent across multiple nodes. If one node goes down, the others keep working and your data stays safe.

It uses the Raft consensus algorithm under the hood — the same approach used by etcd, CockroachDB, and other production systems — to make sure all nodes agree on the same data.

## What it does

- **Store key-value pairs** across a cluster of nodes
- **Survive failures** — kill any minority of nodes and the cluster keeps going
- **Stay consistent** — every read returns the latest written value, no stale data
- **Automatically elect a new leader** if the current one goes down

## How it works

The cluster runs 3 (or 5) nodes. One node gets elected as the leader. All writes go through the leader, which replicates them to the other nodes before confirming. Reads can go to any node.

If the leader dies, the remaining nodes hold a quick election and pick a new leader. Writes resume once the new leader is up.

Nodes talk to each other using gRPC. Clients interact with the cluster through a simple HTTP API.

## Quick start

### Using Docker Compose (recommended)

```bash
docker-compose up --build
```

This starts a 3-node cluster:
- Node 1: http://localhost:8081
- Node 2: http://localhost:8082
- Node 3: http://localhost:8083

### Building from source

```bash
go build -o bin/raft-node ./cmd/node
```

Then start each node in a separate terminal:

```bash
# Terminal 1
./bin/raft-node --id=node1 --grpc-port=50051 --http-port=8081 \
  --peers=localhost:50052,localhost:50053

# Terminal 2
./bin/raft-node --id=node2 --grpc-port=50052 --http-port=8082 \
  --peers=localhost:50051,localhost:50053

# Terminal 3
./bin/raft-node --id=node3 --grpc-port=50053 --http-port=8083 \
  --peers=localhost:50051,localhost:50052
```

## Using the API

### Store a value
```bash
curl -X PUT http://localhost:8081/key/username \
  -d '{"value": "sshaurya"}'
```

### Read a value
```bash
curl http://localhost:8081/key/username
```

### Delete a value
```bash
curl -X DELETE http://localhost:8081/key/username
```

### Check node status
```bash
curl http://localhost:8081/status
```

## Testing fault tolerance

1. Start the cluster with `docker-compose up --build`
2. Write some data: `curl -X PUT http://localhost:8081/key/test -d '{"value": "hello"}'`
3. Kill the leader: `docker-compose stop node1`
4. Wait a second for re-election
5. Read the data from another node: `curl http://localhost:8082/key/test`
6. Your data is still there

## Running tests

```bash
go test ./... -v
```

## Project structure

```
cmd/node/        → entry point, wires everything together
raft/            → Raft consensus engine (election, replication, state)
rpc/             → gRPC service definitions and client/server
kv/              → in-memory key-value store
storage/         → file-based persistence for raft state
client/          → HTTP API server
tests/           → unit and integration tests
```

## Tech stack

- Go
- gRPC + Protocol Buffers (node-to-node communication)
- HTTP (client API)
- Docker Compose (running the cluster)
