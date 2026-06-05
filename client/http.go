package client

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/kv"
	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/raft"
)

type HTTPServer struct {
	raftNode *raft.RaftNode
	kvStore  *kv.Store
	port     int
	peers    map[string]int // nodeID -> httpPort
}

func NewHTTPServer(raftNode *raft.RaftNode, kvStore *kv.Store, port int, peers map[string]int) *HTTPServer {
	return &HTTPServer{
		raftNode: raftNode,
		kvStore:  kvStore,
		port:     port,
		peers:    peers,
	}
}

type Response struct {
	Success bool   `json:"success"`
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
	Leader  string `json:"leader,omitempty"`
}

func (s *HTTPServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/key/", s.handleKey)
	mux.HandleFunc("/status", s.handleStatus)

	addr := fmt.Sprintf(":%d", s.port)
	log.Printf("HTTP server listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}

func (s *HTTPServer) handleKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/key/"):]
	if key == "" {
		writeJSON(w, http.StatusBadRequest, Response{Error: "key is required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, key)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "method not allowed"})
	}
}

func (s *HTTPServer) handleGet(w http.ResponseWriter, key string) {
	val, ok := s.kvStore.Get(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, Response{Error: "key not found", Key: key})
		return
	}
	writeJSON(w, http.StatusOK, Response{Success: true, Key: key, Value: val})
}

func (s *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	if s.raftNode.GetRole() != raft.Leader {
		s.redirectToLeader(w, r)
		return
	}

	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid request body"})
		return
	}

	result := s.raftNode.Propose(raft.CommandPut, key, body.Value)
	if !result.Success {
		writeJSON(w, http.StatusInternalServerError, Response{Error: result.Error})
		return
	}

	writeJSON(w, http.StatusOK, Response{Success: true, Key: key, Value: body.Value})
}

func (s *HTTPServer) handleDelete(w http.ResponseWriter, key string) {
	if s.raftNode.GetRole() != raft.Leader {
		s.redirectToLeader(w, nil)
		return
	}

	result := s.raftNode.Propose(raft.CommandDelete, key, "")
	if !result.Success {
		writeJSON(w, http.StatusInternalServerError, Response{Error: result.Error})
		return
	}

	writeJSON(w, http.StatusOK, Response{Success: true, Key: key})
}

func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"role":         s.raftNode.GetRole().String(),
		"term":         s.raftNode.GetCurrentTerm(),
		"leader":       s.raftNode.GetLeaderID(),
		"commit_index": s.raftNode.GetCommitIndex(),
		"log_length":   s.raftNode.GetLogLength(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *HTTPServer) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leaderID := s.raftNode.GetLeaderID()
	if leaderID == "" {
		writeJSON(w, http.StatusServiceUnavailable, Response{Error: "no leader elected"})
		return
	}

	if port, ok := s.peers[leaderID]; ok {
		leaderURL := fmt.Sprintf("http://localhost:%d%s", port, r.URL.Path)
		http.Redirect(w, r, leaderURL, http.StatusTemporaryRedirect)
		return
	}

	writeJSON(w, http.StatusServiceUnavailable, Response{
		Error:  "leader known but address unavailable",
		Leader: leaderID,
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
