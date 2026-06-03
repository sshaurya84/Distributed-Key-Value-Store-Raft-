package rpc

import (
	"context"
)

type RaftHandler interface {
	HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse
	HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
}

type RaftServer struct {
	UnimplementedRaftServiceServer
	handler RaftHandler
}

func NewRaftServer(handler RaftHandler) *RaftServer {
	return &RaftServer{handler: handler}
}

func (s *RaftServer) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	return s.handler.HandleRequestVote(req), nil
}

func (s *RaftServer) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return s.handler.HandleAppendEntries(req), nil
}
