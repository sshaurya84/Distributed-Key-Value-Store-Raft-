package tests

import (
	"testing"

	"github.com/sshaurya84/Distributed-Key-Value-Store-Raft-/kv"
)

func TestPutAndGet(t *testing.T) {
	store := kv.NewStore()

	store.Put("name", "alice")
	val, ok := store.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if val != "alice" {
		t.Fatalf("expected 'alice', got '%s'", val)
	}
}

func TestGetMissing(t *testing.T) {
	store := kv.NewStore()

	_, ok := store.Get("missing")
	if ok {
		t.Fatal("expected key to not exist")
	}
}

func TestDelete(t *testing.T) {
	store := kv.NewStore()

	store.Put("key", "value")
	store.Delete("key")

	_, ok := store.Get("key")
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestOverwrite(t *testing.T) {
	store := kv.NewStore()

	store.Put("key", "v1")
	store.Put("key", "v2")

	val, ok := store.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if val != "v2" {
		t.Fatalf("expected 'v2', got '%s'", val)
	}
}

func TestMultipleKeys(t *testing.T) {
	store := kv.NewStore()

	store.Put("a", "1")
	store.Put("b", "2")
	store.Put("c", "3")

	tests := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, expected := range tests {
		val, ok := store.Get(k)
		if !ok {
			t.Fatalf("expected key '%s' to exist", k)
		}
		if val != expected {
			t.Fatalf("key '%s': expected '%s', got '%s'", k, expected, val)
		}
	}
}
