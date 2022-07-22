// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/pkg/errors"
)

// coordinator is used to decide if the shards need to be scheduled.
type coordinator struct {
	// Mutex is to ensure only one coordinator can run at the same time.
	sync.Mutex
	cluster *Cluster
}

func (c *coordinator) Run(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()
	if err := c.scatterShard(ctx); err != nil {
		return errors.Wrap(err, "coordinator Run")
	}
	return nil
}

// TODO: consider ReplicationFactor
func (c *coordinator) scatterShard(ctx context.Context) error {
	if !(int(c.cluster.metaData.cluster.MinNodeCount) >= len(c.cluster.metaData.nodes) &&
		c.cluster.metaData.clusterTopology.State == clusterpb.ClusterTopology_EMPTY) {
		return nil
	}

	// TODO: consider data race
	c.Lock()
	defer c.Unlock()

	shardTotal := int(c.cluster.metaData.cluster.ShardTotal)
	minNodeCount := int(c.cluster.metaData.cluster.MinNodeCount)
	perNodeShardCount := shardTotal / minNodeCount
	shards := make([]*clusterpb.Shard, shardTotal)
	nodeList := make([]*clusterpb.Node, len(c.cluster.metaData.nodes))
	for _, v := range c.cluster.metaData.nodes {
		nodeList = append(nodeList, v)
	}

	nodeIndex := 0
	for i := 0; i < minNodeCount; i++ {
		for j := 0; j < perNodeShardCount; j++ {
			if i*perNodeShardCount+j < shardTotal {
				for ; nodeIndex < len(nodeList); nodeIndex++ {
					// TODO: consider nodesCache state
					shards = append(shards, &clusterpb.Shard{
						Id:        uint32(i*perNodeShardCount + j),
						ShardRole: clusterpb.ShardRole_LEADER,
						Node:      nodeList[nodeIndex].GetName(),
					})
				}
			}
		}
	}
	c.cluster.metaData.clusterTopology.ShardView = shards
	if err := c.cluster.storage.PutClusterTopology(ctx, c.cluster.clusterID, c.cluster.metaData.clusterTopology); err != nil {
		return errors.Wrap(err, "coordinator scatterShard")
	}
	if err := c.cluster.Load(ctx); err != nil {
		return errors.Wrap(err, "coordinator scatterShard")
	}
	return nil
}
