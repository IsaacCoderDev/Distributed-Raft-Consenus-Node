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

type voteMsg struct {
	req  *pb.VoteRequest
	resp chan *pb.VoteResponse
}

type appendMsg struct {
	req  *pb.AppendRequest
	resp chan *pb.AppendResponse
}

type RaftNode struct {
	id          uint32
	state       NodeState
	currentTerm uint64
	votedFor    uint32
	log         []*pb.LogEntry

	commitIndex uint64
	lastApplied uint64

	voteCh   chan voteMsg
	appendCh chan appendMsg

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
