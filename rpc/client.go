package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerClient struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

func NewPeerClient() *PeerClient {
	return &PeerClient{
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (pc *PeerClient) getConn(addr string) (*grpc.ClientConn, error) {
	pc.mu.RLock()
	conn, ok := pc.conns[addr]
	pc.mu.RUnlock()
	if ok {
		return conn, nil
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if conn, ok := pc.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	pc.conns[addr] = conn
	return conn, nil
}

func (pc *PeerClient) RequestVote(addr string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	conn, err := pc.getConn(addr)
	if err != nil {
		return nil, err
	}

	client := NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	return client.RequestVote(ctx, req)
}

func (pc *PeerClient) AppendEntries(addr string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	conn, err := pc.getConn(addr)
	if err != nil {
		return nil, err
	}

	client := NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	return client.AppendEntries(ctx, req)
}

func (pc *PeerClient) Close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, conn := range pc.conns {
		conn.Close()
	}
}
