package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/kv"
	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/rpc"
	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/storage"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

const (
	MinElectionTimeout = 150 * time.Millisecond
	MaxElectionTimeout = 300 * time.Millisecond
	HeartbeatInterval  = 50 * time.Millisecond
)

type Config struct {
	ID       string
	Peers    []string
	GRPCPort int
	HTTPPort int
	DataDir  string
}

type RaftNode struct {
	mu sync.Mutex

	id   string
	role Role

	currentTerm int
	votedFor    string
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  map[string]int
	matchIndex map[string]int

	peers     []string
	leaderID  string
	grpcPort  int
	httpPort  int

	kvStore   *kv.Store
	storage   *storage.Storage
	rpcClient *rpc.PeerClient

	resetElectionTimer chan struct{}
	stopCh             chan struct{}
}

func NewRaftNode(cfg Config, kvStore *kv.Store, store *storage.Storage, client *rpc.PeerClient) *RaftNode {
	rn := &RaftNode{
		id:                 cfg.ID,
		role:               Follower,
		peers:              cfg.Peers,
		grpcPort:           cfg.GRPCPort,
		httpPort:           cfg.HTTPPort,
		kvStore:            kvStore,
		storage:            store,
		rpcClient:          client,
		nextIndex:          make(map[string]int),
		matchIndex:         make(map[string]int),
		resetElectionTimer: make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
	}
	return rn
}

func (rn *RaftNode) Start() error {
	state, err := rn.storage.LoadState()
	if err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}
	rn.currentTerm = state.CurrentTerm
	rn.votedFor = state.VotedFor

	entries, err := rn.storage.LoadLog()
	if err != nil {
		return fmt.Errorf("failed to load log: %w", err)
	}
	rn.log = make([]LogEntry, len(entries))
	for i, e := range entries {
		rn.log[i] = LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Type:  CommandType(e.Type),
			Key:   e.Key,
			Value: e.Value,
		}
	}

	go rn.electionLoop()
	log.Printf("[%s] started as %s (term=%d, log=%d entries)", rn.id, rn.role, rn.currentTerm, len(rn.log))
	return nil
}

func (rn *RaftNode) Stop() {
	close(rn.stopCh)
}

func (rn *RaftNode) GetRole() Role {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.role
}

func (rn *RaftNode) GetLeaderID() string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.leaderID
}

func (rn *RaftNode) GetLeaderHTTPPort() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.httpPort
}

func randomElectionTimeout() time.Duration {
	return MinElectionTimeout + time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout)))
}

func (rn *RaftNode) electionLoop() {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-rn.resetElectionTimer:
			timer.Reset(randomElectionTimeout())
		case <-timer.C:
			rn.mu.Lock()
			if rn.role != Leader {
				rn.mu.Unlock()
				rn.startElection()
			} else {
				rn.mu.Unlock()
			}
			timer.Reset(randomElectionTimeout())
		}
	}
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.role = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.leaderID = ""
	currentTerm := rn.currentTerm
	lastLogIndex, lastLogTerm := rn.lastLogInfo()
	rn.persist()
	rn.mu.Unlock()

	log.Printf("[%s] starting election for term %d", rn.id, currentTerm)

	votes := 1
	total := len(rn.peers) + 1
	needed := total/2 + 1

	if votes >= needed {
		rn.mu.Lock()
		if rn.role == Candidate && rn.currentTerm == currentTerm {
			rn.becomeLeader()
		}
		rn.mu.Unlock()
		return
	}

	var voteMu sync.Mutex
	done := make(chan struct{}, 1)

	for _, peer := range rn.peers {
		go func(addr string) {
			resp, err := rn.rpcClient.RequestVote(addr, &rpc.RequestVoteRequest{
				Term:         int32(currentTerm),
				CandidateId:  rn.id,
				LastLogIndex: int32(lastLogIndex),
				LastLogTerm:  int32(lastLogTerm),
			})
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if int(resp.Term) > rn.currentTerm {
				rn.becomeFollower(int(resp.Term))
				return
			}

			if resp.VoteGranted && rn.role == Candidate && rn.currentTerm == currentTerm {
				voteMu.Lock()
				votes++
				if votes >= needed {
					voteMu.Unlock()
					select {
					case done <- struct{}{}:
					default:
					}
					return
				}
				voteMu.Unlock()
			}
		}(peer)
	}

	select {
	case <-done:
		rn.mu.Lock()
		if rn.role == Candidate && rn.currentTerm == currentTerm {
			rn.becomeLeader()
		}
		rn.mu.Unlock()
	case <-time.After(randomElectionTimeout()):
	case <-rn.stopCh:
	}
}

func (rn *RaftNode) becomeFollower(term int) {
	rn.role = Follower
	rn.currentTerm = term
	rn.votedFor = ""
	rn.persist()
	log.Printf("[%s] became follower (term=%d)", rn.id, term)
}

func (rn *RaftNode) becomeLeader() {
	rn.role = Leader
	rn.leaderID = rn.id
	log.Printf("[%s] became leader (term=%d)", rn.id, rn.currentTerm)

	for _, peer := range rn.peers {
		rn.nextIndex[peer] = len(rn.log) + 1
		rn.matchIndex[peer] = 0
	}

	go rn.heartbeatLoop()
}

func (rn *RaftNode) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	rn.sendHeartbeats()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
			rn.mu.Lock()
			if rn.role != Leader {
				rn.mu.Unlock()
				return
			}
			rn.mu.Unlock()
			rn.sendHeartbeats()
		}
	}
}

func (rn *RaftNode) sendHeartbeats() {
	rn.mu.Lock()
	term := rn.currentTerm
	rn.mu.Unlock()

	for _, peer := range rn.peers {
		go func(addr string) {
			rn.mu.Lock()
			prevLogIndex := rn.nextIndex[addr] - 1
			prevLogTerm := 0
			if prevLogIndex > 0 && prevLogIndex <= len(rn.log) {
				prevLogTerm = rn.log[prevLogIndex-1].Term
			}

			var entries []*rpc.Entry
			if rn.nextIndex[addr] <= len(rn.log) {
				for i := rn.nextIndex[addr] - 1; i < len(rn.log); i++ {
					entries = append(entries, &rpc.Entry{
						Term:  int32(rn.log[i].Term),
						Index: int32(rn.log[i].Index),
						Type:  int32(rn.log[i].Type),
						Key:   rn.log[i].Key,
						Value: rn.log[i].Value,
					})
				}
			}
			commitIndex := rn.commitIndex
			rn.mu.Unlock()

			resp, err := rn.rpcClient.AppendEntries(addr, &rpc.AppendEntriesRequest{
				Term:         int32(term),
				LeaderId:     rn.id,
				PrevLogIndex: int32(prevLogIndex),
				PrevLogTerm:  int32(prevLogTerm),
				Entries:      entries,
				LeaderCommit: int32(commitIndex),
			})
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if int(resp.Term) > rn.currentTerm {
				rn.becomeFollower(int(resp.Term))
				return
			}

			if resp.Success {
				if len(entries) > 0 {
					rn.nextIndex[addr] = int(entries[len(entries)-1].Index) + 1
					rn.matchIndex[addr] = int(entries[len(entries)-1].Index)
				}
			} else {
				if rn.nextIndex[addr] > 1 {
					rn.nextIndex[addr]--
				}
			}
		}(peer)
	}
}

func (rn *RaftNode) HandleRequestVote(req *rpc.RequestVoteRequest) *rpc.RequestVoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &rpc.RequestVoteResponse{
		Term:        int32(rn.currentTerm),
		VoteGranted: false,
	}

	if int(req.Term) < rn.currentTerm {
		return resp
	}

	if int(req.Term) > rn.currentTerm {
		rn.becomeFollower(int(req.Term))
	}

	lastLogIndex, lastLogTerm := rn.lastLogInfo()
	logOK := int(req.LastLogTerm) > lastLogTerm ||
		(int(req.LastLogTerm) == lastLogTerm && int(req.LastLogIndex) >= lastLogIndex)

	if (rn.votedFor == "" || rn.votedFor == req.CandidateId) && logOK {
		rn.votedFor = req.CandidateId
		rn.persist()
		resp.VoteGranted = true

		select {
		case rn.resetElectionTimer <- struct{}{}:
		default:
		}
	}

	resp.Term = int32(rn.currentTerm)
	return resp
}

func (rn *RaftNode) HandleAppendEntries(req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &rpc.AppendEntriesResponse{
		Term:    int32(rn.currentTerm),
		Success: false,
	}

	if int(req.Term) < rn.currentTerm {
		return resp
	}

	if int(req.Term) > rn.currentTerm || rn.role == Candidate {
		rn.becomeFollower(int(req.Term))
	}

	rn.leaderID = req.LeaderId

	select {
	case rn.resetElectionTimer <- struct{}{}:
	default:
	}

	if req.PrevLogIndex > 0 {
		if int(req.PrevLogIndex) > len(rn.log) {
			return resp
		}
		if rn.log[req.PrevLogIndex-1].Term != int(req.PrevLogTerm) {
			rn.log = rn.log[:req.PrevLogIndex-1]
			rn.persistLog()
			return resp
		}
	}

	for _, entry := range req.Entries {
		idx := int(entry.Index)
		newEntry := LogEntry{
			Term:  int(entry.Term),
			Index: idx,
			Type:  CommandType(entry.Type),
			Key:   entry.Key,
			Value: entry.Value,
		}
		if idx-1 < len(rn.log) {
			if rn.log[idx-1].Term != int(entry.Term) {
				rn.log = rn.log[:idx-1]
				rn.log = append(rn.log, newEntry)
			}
		} else {
			rn.log = append(rn.log, newEntry)
		}
	}

	if len(req.Entries) > 0 {
		rn.persistLog()
	}

	if int(req.LeaderCommit) > rn.commitIndex {
		lastNewIndex := len(rn.log)
		if len(req.Entries) > 0 {
			lastNewIndex = int(req.Entries[len(req.Entries)-1].Index)
		}
		if int(req.LeaderCommit) < lastNewIndex {
			rn.commitIndex = int(req.LeaderCommit)
		} else {
			rn.commitIndex = lastNewIndex
		}
		go rn.applyCommitted()
	}

	resp.Success = true
	return resp
}

func (rn *RaftNode) lastLogInfo() (index int, term int) {
	if len(rn.log) == 0 {
		return 0, 0
	}
	last := rn.log[len(rn.log)-1]
	return last.Index, last.Term
}

func (rn *RaftNode) persist() {
	rn.storage.SaveState(storage.PersistentState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
	})
}

func (rn *RaftNode) persistLog() {
	entries := make([]storage.LogEntry, len(rn.log))
	for i, e := range rn.log {
		entries[i] = storage.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Type:  int(e.Type),
			Key:   e.Key,
			Value: e.Value,
		}
	}
	rn.storage.SaveLog(entries)
}

func (rn *RaftNode) applyCommitted() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied-1]
		switch entry.Type {
		case CommandPut:
			rn.kvStore.Put(entry.Key, entry.Value)
			log.Printf("[%s] applied PUT %s=%s (index=%d)", rn.id, entry.Key, entry.Value, entry.Index)
		case CommandDelete:
			rn.kvStore.Delete(entry.Key)
			log.Printf("[%s] applied DELETE %s (index=%d)", rn.id, entry.Key, entry.Index)
		}
	}
}

type ProposalResult struct {
	Success bool
	Error   string
}

func (rn *RaftNode) Propose(cmdType CommandType, key, value string) ProposalResult {
	rn.mu.Lock()
	if rn.role != Leader {
		rn.mu.Unlock()
		return ProposalResult{Success: false, Error: "not leader"}
	}

	newIndex := len(rn.log) + 1
	entry := LogEntry{
		Term:  rn.currentTerm,
		Index: newIndex,
		Type:  cmdType,
		Key:   key,
		Value: value,
	}
	rn.log = append(rn.log, entry)
	rn.persistLog()
	rn.mu.Unlock()

	rn.sendHeartbeats()

	committed := rn.waitForCommit(newIndex, 3*time.Second)
	if !committed {
		return ProposalResult{Success: false, Error: "failed to replicate"}
	}

	return ProposalResult{Success: true}
}

func (rn *RaftNode) waitForCommit(index int, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return false
		case <-ticker.C:
			rn.mu.Lock()
			rn.updateCommitIndex()
			if rn.commitIndex >= index {
				rn.mu.Unlock()
				rn.applyCommitted()
				return true
			}
			rn.mu.Unlock()
		}
	}
}

func (rn *RaftNode) updateCommitIndex() {
	total := len(rn.peers) + 1

	for n := len(rn.log); n > rn.commitIndex; n-- {
		if rn.log[n-1].Term != rn.currentTerm {
			continue
		}

		count := 1
		for _, peer := range rn.peers {
			if rn.matchIndex[peer] >= n {
				count++
			}
		}

		if count > total/2 {
			rn.commitIndex = n
			return
		}
	}
}
