package raft

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type HTTPServer struct {
	node *RaftNode
	kv   *KVStore
}

func (s *HTTPServer) StartAPI(port string) {
	http.HandleFunc("/set", s.handleSet)
	http.HandleFunc("/get", s.handleGet)

	fmt.Printf("REST API listening on %s\n", port)
	http.ListenAndServe(port, nil)
}

func (s *HTTPServer) handleSet(w http.ResponseWriter, r *http.Request) {

	if s.node.state != Leader {

		http.Error(w, "Node is not the leader", http.StatusServiceUnavailable)

		return
	}

	var cmd Command

	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {

		http.Error(w, "Bad request", http.StatusBadRequest)

		return
	}

	cmdBytes, _ := json.Marshal(cmd)

	s.node.Propose(cmdBytes)

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Command proposed to cluster"))
}

func (s *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	if key == "" {
		http.Error(w, "Missing key", http.StatusBadRequest)
		return
	}

	val, found := s.kv.Read(key)

	if !found {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	w.Write([]byte(val))
}
