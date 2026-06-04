package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/client"
	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/kv"
	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/raft"
	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/rpc"
	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/storage"
)

func main() {
	nodeID := flag.String("id", "", "node ID")
	grpcPort := flag.Int("grpc-port", 50051, "gRPC port")
	httpPort := flag.Int("http-port", 8080, "HTTP port")
	peers := flag.String("peers", "", "comma-separated peer gRPC addresses (host:port)")
	peerHTTP := flag.String("peer-http", "", "comma-separated peer HTTP mappings (id=port)")
	dataDir := flag.String("data-dir", "./data", "data directory for persistence")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("node ID is required (--id)")
	}

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	peerHTTPMap := make(map[string]int)
	if *peerHTTP != "" {
		for _, mapping := range strings.Split(*peerHTTP, ",") {
			parts := strings.SplitN(mapping, "=", 2)
			if len(parts) == 2 {
				var port int
				fmt.Sscanf(parts[1], "%d", &port)
				peerHTTPMap[parts[0]] = port
			}
		}
	}

	store, err := storage.NewStorage(fmt.Sprintf("%s/%s", *dataDir, *nodeID))
	if err != nil {
		log.Fatalf("failed to create storage: %v", err)
	}

	kvStore := kv.NewStore()
	peerClient := rpc.NewPeerClient()

	cfg := raft.Config{
		ID:       *nodeID,
		Peers:    peerList,
		GRPCPort: *grpcPort,
		HTTPPort: *httpPort,
		DataDir:  *dataDir,
	}

	raftNode := raft.NewRaftNode(cfg, kvStore, store, peerClient)

	grpcServer := grpc.NewServer()
	raftServer := rpc.NewRaftServer(raftNode)
	rpc.RegisterRaftServiceServer(grpcServer, raftServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		log.Fatalf("failed to listen on gRPC port: %v", err)
	}

	go func() {
		log.Printf("gRPC server listening on :%d", *grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	httpServer := client.NewHTTPServer(raftNode, kvStore, *httpPort, peerHTTPMap)
	go func() {
		if err := httpServer.Start(); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	if err := raftNode.Start(); err != nil {
		log.Fatalf("failed to start raft node: %v", err)
	}

	log.Printf("node %s started (grpc=:%d, http=:%d)", *nodeID, *grpcPort, *httpPort)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("shutting down node %s...", *nodeID)
	raftNode.Stop()
	grpcServer.GracefulStop()
	peerClient.Close()
}
