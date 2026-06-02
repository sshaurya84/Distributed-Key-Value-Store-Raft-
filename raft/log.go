package raft

type CommandType int

const (
	CommandPut CommandType = iota
	CommandDelete
)

type LogEntry struct {
	Term  int         `json:"term"`
	Index int         `json:"index"`
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value string      `json:"value"`
}
