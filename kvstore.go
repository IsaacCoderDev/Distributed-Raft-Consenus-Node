package raft

import (
	"bytes"
	"encoding/json"
	"fmt"

	pb "github.com/quant/raft-go/proto"
)

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KVStore struct {
	store   map[string]string
	applyCh <-chan *pb.LogEntry
}

func NewKVStore(applyCh <-chan *pb.LogEntry) *KVStore {

	return &KVStore{
		store:   make(map[string]string),
		applyCh: applyCh,
	}
}

func (kv *KVStore) Start() {
	fmt.Println("Key-Value State Machine started.")

	for entry := range kv.applyCh {

		var cmd Command

		if err := json.NewDecoder(bytes.NewReader(entry.Command)).Decode(&cmd); err != nil {
			fmt.Printf("Warning: Failed to decode committed command at index %d\n", entry.Index)
			continue
		}

		switch cmd.Op {

		case "SET":
			kv.store[cmd.Key] = cmd.Value
			fmt.Printf("[STATE MACHINE] Applied SET: %s = %s (Index: %d)\n", cmd.Key, cmd.Value, entry.Index)

		case "DELETE":
			delete(kv.store, cmd.Key)
			fmt.Printf("[STATE MACHINE] Applied DELETE: %s (Index: %d)\n", cmd.Key, entry.Index)
		}
	}
}

func (kv *KVStore) Read(key string) (string, bool) {
	val, ok := kv.store[key]

	return val, ok
}
