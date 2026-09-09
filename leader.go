package raft

import (
	"context"
	"time"

	pb "github.com/quant/raft-go/proto"
)

var nextIndex map[uint32]uint64
var matchIndex map[uint32]uint64

func (n *RaftNode) broadcastAppendEntries() {

	for _, peerID := range n.peerIDs {

		go func(id uint32) {
			prevLogIndex := nextIndex[id] - 1

			var prevLogTerm uint64

			if prevLogIndex > 0 && prevLogIndex < uint64(len(n.log)) {
				prevLogTerm = n.log[prevLogIndex].Term
			}

			entries := n.log[nextIndex[id]:]

			req := &pb.AppendRequest{
				Term:         n.currentTerm,
				LeaderId:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.commitIndex,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)

			defer cancel()

			resp, err := n.peers[id].AppendEntries(ctx, req)
			if err != nil {
				return
			}

			n.appendResponseCh <- appendResponseMsg{peerID: id, req: req, resp: resp}

		}(peerID)
	}
}

func (n *RaftNode) applyCommittedLogs() {

	for n.commitIndex > n.lastApplied {
		n.lastApplied++
		entry := n.log[n.lastApplied]

		n.applyCh <- entry
	}
}
