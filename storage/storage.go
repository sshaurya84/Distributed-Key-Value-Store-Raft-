package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type PersistentState struct {
	CurrentTerm int    `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

type LogEntry struct {
	Term  int    `json:"term"`
	Index int    `json:"index"`
	Type  int    `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Storage struct {
	mu      sync.Mutex
	dataDir string
}

func NewStorage(dataDir string) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	return &Storage{dataDir: dataDir}, nil
}

func (s *Storage) SaveState(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.dataDir, "state.json"), data, 0644)
}

func (s *Storage) LoadState() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var state PersistentState
	data, err := os.ReadFile(filepath.Join(s.dataDir, "state.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return PersistentState{}, nil
		}
		return state, err
	}
	err = json.Unmarshal(data, &state)
	return state, err
}

func (s *Storage) SaveLog(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.dataDir, "log.json"), data, 0644)
}

func (s *Storage) LoadLog() ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var entries []LogEntry
	data, err := os.ReadFile(filepath.Join(s.dataDir, "log.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return []LogEntry{}, nil
		}
		return nil, err
	}
	err = json.Unmarshal(data, &entries)
	return entries, err
}
