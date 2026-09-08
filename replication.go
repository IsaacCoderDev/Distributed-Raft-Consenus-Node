package raft

import (
	"math"

	pb "github.com/quant/raft-go/proto"
)

func (n *RaftNode) handleAppendEntries(req *pb.AppendRequest) *pb.AppendResponse {

	if req.Term < n.currentTerm {
		return &pb.AppendResponse{Term: n.currentTerm, Success: false}
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = 0
	}
	n.state = Follower

	if req.PrevLogIndex > 0 {
		if uint64(len(n.log)) <= req.PrevLogIndex {

			return &pb.AppendResponse{Term: n.currentTerm, Success: false}
		}
		if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {

			n.log = n.log[:req.PrevLogIndex]

			return &pb.AppendResponse{Term: n.currentTerm, Success: false}
		}
	}

	for i, entry := range req.Entries {
		logIndex := req.PrevLogIndex + 1 + uint64(i)

		if uint64(len(n.log)) > logIndex {
			if n.log[logIndex].Term != entry.Term {
				n.log = n.log[:logIndex]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		n.commitIndex = uint64(math.Min(float64(req.LeaderCommit), float64(lastNewIndex)))

		// Signal the application state machine to apply new committed logs
		// n.applyCh <- struct{}{}
	}

	return &pb.AppendResponse{Term: n.currentTerm, Success: true}
}
