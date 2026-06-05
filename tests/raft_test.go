package tests

import (
	"testing"
	"time"

	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/kv"
	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/raft"
	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/rpc"
	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/storage"
)

func setupNode(t *testing.T, id string, peers []string) (*raft.RaftNode, *kv.Store) {
	t.Helper()

	dir := t.TempDir()
	store, err := storage.NewStorage(dir)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	kvStore := kv.NewStore()
	client := rpc.NewPeerClient()

	cfg := raft.Config{
		ID:       id,
		Peers:    peers,
		GRPCPort: 0,
		HTTPPort: 0,
		DataDir:  dir,
	}

	node := raft.NewRaftNode(cfg, kvStore, store, client)
	return node, kvStore
}

func TestNewNodeStartsAsFollower(t *testing.T) {
	node, _ := setupNode(t, "node1", nil)
	defer node.Stop()

	if node.GetRole() != raft.Follower {
		t.Fatalf("expected Follower, got %s", node.GetRole())
	}
}

func TestSingleNodeElection(t *testing.T) {
	node, _ := setupNode(t, "node1", nil)

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	time.Sleep(500 * time.Millisecond)

	if node.GetRole() != raft.Leader {
		t.Fatalf("single node should become leader, got %s", node.GetRole())
	}
}

func TestSingleNodePutGet(t *testing.T) {
	node, kvStore := setupNode(t, "node1", nil)

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	time.Sleep(500 * time.Millisecond)

	result := node.Propose(raft.CommandPut, "hello", "world")
	if !result.Success {
		t.Fatalf("proposal failed: %s", result.Error)
	}

	val, ok := kvStore.Get("hello")
	if !ok {
		t.Fatal("expected key to exist after proposal")
	}
	if val != "world" {
		t.Fatalf("expected 'world', got '%s'", val)
	}
}

func TestSingleNodeDelete(t *testing.T) {
	node, kvStore := setupNode(t, "node1", nil)

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	time.Sleep(500 * time.Millisecond)

	node.Propose(raft.CommandPut, "key", "value")

	result := node.Propose(raft.CommandDelete, "key", "")
	if !result.Success {
		t.Fatalf("delete proposal failed: %s", result.Error)
	}

	_, ok := kvStore.Get("key")
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestTermIncrementsOnElection(t *testing.T) {
	node, _ := setupNode(t, "node1", nil)

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	time.Sleep(500 * time.Millisecond)

	term := node.GetCurrentTerm()
	if term < 1 {
		t.Fatalf("expected term >= 1 after election, got %d", term)
	}
}

func TestStoragePersistence(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewStorage(dir)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	state := storage.PersistentState{CurrentTerm: 5, VotedFor: "node2"}
	if err := store.SaveState(state); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}

	loaded, err := store.LoadState()
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if loaded.CurrentTerm != 5 || loaded.VotedFor != "node2" {
		t.Fatalf("state mismatch: got term=%d votedFor=%s", loaded.CurrentTerm, loaded.VotedFor)
	}
}

func TestLogPersistence(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewStorage(dir)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	entries := []storage.LogEntry{
		{Term: 1, Index: 1, Type: 0, Key: "a", Value: "1"},
		{Term: 1, Index: 2, Type: 0, Key: "b", Value: "2"},
	}
	if err := store.SaveLog(entries); err != nil {
		t.Fatalf("failed to save log: %v", err)
	}

	loaded, err := store.LoadLog()
	if err != nil {
		t.Fatalf("failed to load log: %v", err)
	}

	if len(loaded) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(loaded))
	}
	if loaded[0].Key != "a" || loaded[1].Key != "b" {
		t.Fatal("log entries don't match")
	}
}
