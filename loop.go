package raft

import (
	"fmt"
	"math/rand"
	"time"

	pb "github.com/quant/raft-go/proto"
)

func (n *RaftNode) Run() {
	fmt.Printf("Node %d starting as Follower\n", n.id)

	electionTimer := time.NewTimer(n.randomizedElectionTimeout())

	for {
		select {
		case <-n.shutdownCh:
			fmt.Println("Node shutting down...")
			electionTimer.Stop()

			return

		// --------------------------------------------------------------------
		// TIMER EVENTS
		// --------------------------------------------------------------------
		case <-electionTimer.C:
			if n.state != Leader {
				n.becomeCandidate()
			}

			electionTimer.Reset(n.randomizedElectionTimeout())

		// --------------------------------------------------------------------
		// NETWORK RPC EVENTS (Routed from the gRPC server)
		// --------------------------------------------------------------------
		case msg := <-n.appendCh:
			resp := n.handleAppendEntries(msg.req)

			if resp.Success || msg.req.Term >= n.currentTerm {
				electionTimer.Reset(n.randomizedElectionTimeout())
			}

			msg.resp <- resp

		case msg := <-n.voteCh:
			resp := n.handleRequestVote(msg.req)

			if resp.VoteGranted {
				electionTimer.Reset(n.randomizedElectionTimeout())
			}

			msg.resp <- resp
		}
	}
}

func (n *RaftNode) randomizedElectionTimeout() time.Duration {
	ms := 150 + rand.Intn(150)

	return time.Duration(ms) * time.Millisecond
}

func (n *RaftNode) becomeCandidate() {}
func (n *RaftNode) handleAppendEntries(req *pb.AppendRequest) *pb.AppendResponse {
	return &pb.AppendResponse{}
}
func (n *RaftNode) handleRequestVote(req *pb.VoteRequest) *pb.VoteResponse { return &pb.VoteResponse{} }
