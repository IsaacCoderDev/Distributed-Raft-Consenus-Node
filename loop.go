package raft

import (
	"fmt"
	"math/rand"
	"sort"
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

		case msg := <-n.winElectionCh:

			if n.state == Candidate {
				fmt.Printf("Node %d won the election! Becoming Leader.\n", n.id)
				n.state = Leader

				n.broadcastHeartbeat()
			}

		case msg := <-n.appendResponseCh:
			if msg.resp.Term > n.currentTerm {
				n.currentTerm = msg.resp.Term
				n.state = Follower
				n.votedFor = 0
				continue
			}

			if msg.resp.Success {
				matchIndex[msg.peerID] = msg.req.PrevLogIndex + uint64(len(msg.req.Entries))
				nextIndex[msg.peerID] = matchIndex[msg.peerID] + 1

				matches := make([]int, 0, len(n.peerIDs)+1)
				matches = append(matches, len(n.log)-1) // include ourselves

				for _, pID := range n.peerIDs {
					matches = append(matches, int(matchIndex[pID]))
				}
				sort.Ints(matches)
				majorityIndex := uint64(matches[len(matches)/2])

				if majorityIndex > n.commitIndex && n.log[majorityIndex].Term == n.currentTerm {
					n.commitIndex = majorityIndex
					fmt.Printf("Leader %d committed log up to index %d\n", n.id, n.commitIndex)
					// n.applyCh <- struct{}{} // Inform KV application
				}
			} else {

				if nextIndex[msg.peerID] > 1 {
					nextIndex[msg.peerID]--
				}
			}
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
func (n *RaftNode) handleRequestVote(req *pb.VoteRequest) *pb.VoteResponse {
	return &pb.VoteResponse{}
}
