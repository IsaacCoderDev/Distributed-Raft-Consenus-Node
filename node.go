package raft

import (
	pb "github.com/quant/raft-go/proto"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// Wrapper structs to pass gRPC requests into the single-threaded event loop
type voteMsg struct {
	req  *pb.VoteRequest
	resp chan *pb.VoteResponse
}

type appendMsg struct {
	req  *pb.AppendRequest
	resp chan *pb.AppendResponse
}

type RaftNode struct {
	id    uint32
	state NodeState

	// Persistent State (Must be written to disk before responding to RPCs)
	currentTerm uint64
	votedFor    uint32
	log         []*pb.LogEntry

	// Volatile State
	commitIndex uint64
	lastApplied uint64

	// Channels for routing network traffic to the event loop
	voteCh   chan voteMsg
	appendCh chan appendMsg

	// Control channels
	shutdownCh chan struct{}
}

func NewRaftNode(id uint32) *RaftNode {
	return &RaftNode{
		id:         id,
		state:      Follower,
		voteCh:     make(chan voteMsg),
		appendCh:   make(chan appendMsg),
		shutdownCh: make(chan struct{}),
	}
}
