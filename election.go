package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/quant/raft-go/proto"
)

func (n *RaftNode) handleRequestVote(req *pb.VoteRequest) *pb.VoteResponse {
	if req.Term < n.currentTerm {
		return &pb.VoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = 0
	}

	var myLastLogTerm uint64
	var myLastLogIndex uint64

	if len(n.log) > 0 {
		myLastLogIndex = uint64(len(n.log) - 1)
		myLastLogTerm = n.log[myLastLogIndex].Term
	}

	isUpToDate := (req.LastLogTerm > myLastLogTerm) ||
		(req.LastLogTerm == myLastLogTerm && req.LastLogIndex >= myLastLogIndex)

	if (n.votedFor == 0 || n.votedFor == req.CandidateId) && isUpToDate {
		n.votedFor = req.CandidateId

		return &pb.VoteResponse{Term: n.currentTerm, VoteGranted: true}
	}

	return &pb.VoteResponse{Term: n.currentTerm, VoteGranted: false}
}

func (n *RaftNode) becomeCandidate() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id

	fmt.Printf("Node %d becoming Candidate for Term %d\n", n.id, n.currentTerm)

	termAtElection := n.currentTerm

	var lastLogTerm, lastLogIndex uint64

	if len(n.log) > 0 {
		lastLogIndex = uint64(len(n.log) - 1)
		lastLogTerm = n.log[lastLogIndex].Term
	}

	req := &pb.VoteRequest{
		Term:         termAtElection,
		CandidateId:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	go n.startVoteCollection(req)
}

func (n *RaftNode) startVoteCollection(req *pb.VoteRequest) {
	votesReceived := 1
	votesNeeded := (len(n.peers) / 2) + 1 // Quorum majority

	var mu sync.Mutex
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)

	defer cancel()

	for _, peer := range n.peers {
		wg.Add(1)
		go func(client pb.RaftServiceClient) {
			defer wg.Done()

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				return
			}

			mu.Lock()
			defer mu.Unlock()

			if resp.VoteGranted {
				votesReceived++

				if votesReceived == votesNeeded {
					n.winElectionCh <- struct{}{}
				}
			}
		}(peer)
	}

	// Wait for all RPCs to finish or timeout
	wg.Wait()
}
