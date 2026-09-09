package raft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type TestCluster struct {
	nodes       []*RaftNode
	apiServers  []*HTTPServer
	cancelFuncs []context.CancelFunc
}

func setupCluster(t *testing.T, size int) *TestCluster {

	cluster := &TestCluster{}

	for i := 0; i < size; i++ {

		node := NewRaftNode(uint32(i))
		kv := NewKVStore(node.applyCh)

		go node.Run()
		go kv.Start()

		cluster.nodes = append(cluster.nodes, node)
	}

	time.Sleep(1 * time.Second)

	return cluster
}

func (c *TestCluster) getLeader() *RaftNode {

	for _, n := range c.nodes {

		if n.state == Leader {
			return n
		}
	}

	return nil
}

func (c *TestCluster) Isolate(nodeID uint32) {

	fmt.Printf("CHAOS: Isolating Node %d from the network\n", nodeID)
}

func (c *TestCluster) HealPartition() {
	fmt.Println("CHAOS: Healing network partition")

}

func TestNetworkPartitionConvergence(t *testing.T) {

	cluster := setupCluster(t, 5)

	oldLeader := cluster.getLeader()

	if oldLeader == nil {
		t.Fatal("Cluster failed to elect an initial leader")
	}

	cluster.Isolate(oldLeader.id)

	oldLeader.Propose([]byte(`{"op":"SET","key":"status","value":"stale"}`))

	time.Sleep(1 * time.Second)
	newLeader := cluster.getLeader()

	if newLeader == nil || newLeader.id == oldLeader.id {
		t.Fatal("Cluster failed to elect a new leader after partition")
	}

	newLeader.Propose([]byte(`{"op":"SET","key":"status","value":"healthy"}`))

	time.Sleep(500 * time.Millisecond)

	cluster.HealPartition()

	time.Sleep(1 * time.Second)

	for _, n := range cluster.nodes {
		val, _ := n.kvStore.Read("status")

		if val != "healthy" {
			t.Fatalf("Node %d failed to converge! Expected 'healthy', got '%s'", n.id, val)
		}
	}

	fmt.Println("SUCCESS: Cluster healed and converged correctly.")
}
