.PHONY: build run test clean proto docker-up docker-down

build:
	go build -o bin/raft-node ./cmd/node

run: build
	./bin/raft-node --id=node1 --grpc-port=50051 --http-port=8081 --peers=localhost:50052,localhost:50053

test:
	go test ./... -v

clean:
	rm -rf bin/ data/

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		rpc/proto/raft.proto

docker-up:
	docker-compose up --build -d

docker-down:
	docker-compose down -v
