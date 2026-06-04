package raft

import (
	"log"

	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/storage"
)

func (rn *RaftNode) RestoreState(store *storage.Storage) error {
	state, err := store.LoadState()
	if err != nil {
		return err
	}
	rn.currentTerm = state.CurrentTerm
	rn.votedFor = state.VotedFor

	entries, err := store.LoadLog()
	if err != nil {
		return err
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

	for i := range rn.log {
		entry := rn.log[i]
		switch entry.Type {
		case CommandPut:
			rn.kvStore.Put(entry.Key, entry.Value)
		case CommandDelete:
			rn.kvStore.Delete(entry.Key)
		}
		rn.lastApplied = entry.Index
		rn.commitIndex = entry.Index
	}

	log.Printf("[%s] restored state: term=%d, log=%d entries, applied=%d",
		rn.id, rn.currentTerm, len(rn.log), rn.lastApplied)
	return nil
}

func (rn *RaftNode) GetCurrentTerm() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm
}

func (rn *RaftNode) GetCommitIndex() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.commitIndex
}

func (rn *RaftNode) GetLogLength() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return len(rn.log)
}
