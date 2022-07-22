// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

type Cluster struct {
	// RWMutex is used to project following fields
	lock         sync.RWMutex
	metaData     *metaData
	clusterID    uint32
	shardsCache  map[uint32]*Shard   // shard_id -> shard
	schemasCache map[string]*Schema  // schema_name -> schema
	nodesCache   map[string][]uint32 // node_name -> shard_ids

	storage     storage.Storage
	coordinator *coordinator
}

func NewCluster(cluster *clusterpb.Cluster, storage storage.Storage) *Cluster {
	return &Cluster{
		clusterID:    cluster.GetId(),
		storage:      storage,
		metaData:     &metaData{cluster: cluster},
		shardsCache:  make(map[uint32]*Shard),
		schemasCache: make(map[string]*Schema),
		nodesCache:   make(map[string][]uint32),
	}
}

func (c *Cluster) Name() string {
	return c.metaData.cluster.Name
}

func (c *Cluster) Load(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if err := c.loadClusterTopologyLocked(ctx); err != nil {
		return errors.Wrap(err, "clusters Load")
	}

	if err := c.loadShardTopologyLocked(ctx, c.metaData.shardIDs); err != nil {
		return errors.Wrap(err, "clusters Load")
	}

	if err := c.loadSchemaLocked(ctx); err != nil {
		return errors.Wrap(err, "clusters Load")
	}

	if err := c.loadTableLocked(ctx); err != nil {
		return errors.Wrap(err, "clusters Load")
	}

	if err := c.updateCacheLocked(ctx); err != nil {
		return errors.Wrap(err, "clusters Load")
	}
	return nil
}

func (c *Cluster) updateCacheLocked(ctx context.Context) error {
	for schemaName, tables := range c.metaData.tables {
		for _, table := range tables {
			_, ok := c.schemasCache[schemaName]
			if ok {
				c.schemasCache[schemaName].tableMap[table.GetName()] = &Table{
					schema: c.metaData.schemas[schemaName],
					meta:   table,
				}
			} else {
				c.schemasCache[schemaName] = &Schema{
					meta: c.metaData.schemas[schemaName],
					tableMap: map[string]*Table{table.GetName(): {
						schema: c.metaData.schemas[schemaName],
						meta:   table,
					}},
				}
			}
		}
	}

	for shardID, shardTopology := range c.metaData.shardTopologies {
		tables := make(map[uint64]*Table, len(shardTopology.TableIds))

		for _, tableID := range shardTopology.TableIds {
			for schemaName, tableMap := range c.metaData.tables {
				table, ok := tableMap[tableID]
				if ok {
					tables[tableID] = &Table{
						schema: c.metaData.schemas[schemaName],
						meta:   table,
					}
				}
			}
		}
		// TODO: assert shardID
		// TODO: check shard not found by shardID
		shardMetaList := c.metaData.shards[shardID]
		var nodes []*clusterpb.Node
		for _, shardMeta := range shardMetaList {
			nodes = append(nodes, c.metaData.nodes[shardMeta.Node])
		}
		c.shardsCache[shardID] = &Shard{
			meta:    c.metaData.shards[shardID],
			nodes:   nodes,
			tables:  tables,
			version: 0,
		}
	}

	for shardID, shards := range c.metaData.shards {
		for _, shard := range shards {
			if _, ok := c.nodesCache[shard.GetNode()]; ok {
				c.nodesCache[shard.GetNode()] = append(c.nodesCache[shard.GetNode()], shardID)
			} else {
				c.nodesCache[shard.GetNode()] = []uint32{shardID}
			}
		}
	}

	return nil
}

func (c *Cluster) updateSchemaCacheLocked(schemaPb *clusterpb.Schema) *Schema {
	schema := &Schema{meta: schemaPb}
	c.schemasCache[schemaPb.GetName()] = schema
	return schema
}

func (c *Cluster) updateTableCacheLocked(schema *Schema, tablePb *clusterpb.Table) *Table {
	table := &Table{meta: tablePb, schema: schema.meta}
	schema.tableMap[tablePb.GetName()] = table
	c.shardsCache[tablePb.GetShardId()].tables[table.GetID()] = table
	return table
}

func (c *Cluster) GetTables(ctx context.Context, shardIDs []uint32, node string) (map[uint32]*ShardTablesWithRole, error) {
	shardTables := make(map[uint32]*ShardTablesWithRole, len(shardIDs))
	for _, shardID := range shardIDs {
		c.lock.RLock()
		shard, ok := c.shardsCache[shardID]
		c.lock.RUnlock()
		if !ok {
			return nil, ErrShardNotFound.WithCausef("shardID:%d", shardID)
		}

		shardRole := clusterpb.ShardRole_FOLLOWER
		found := false
		for i, n := range shard.nodes {
			if node == n.GetName() {
				found = true
				shardRole = shard.meta[i].ShardRole
				break
			}
		}
		if !found {
			return nil, ErrNodeNotFound.WithCausef("node not found in current shard, shardID:%d, node:%s", shardID, node)
		}

		tables := make([]*Table, len(shard.tables))
		for _, table := range shard.tables {
			tables = append(tables, table)
		}
		shardTables[shardID] = &ShardTablesWithRole{shardRole: shardRole, tables: tables, version: shard.version}
	}

	return shardTables, nil
}

func (c *Cluster) DropTable(ctx context.Context, schemaName, tableName string, tableID uint64) error {
	schema, exists := c.GetSchema(schemaName)
	if exists {
		return ErrSchemaNotFound.WithCausef("schemaName:%s", schemaName)
	}
	if err := c.storage.DeleteTables(ctx, c.clusterID, schema.GetID(), []uint64{tableID}); err != nil {
		return errors.Wrapf(err, "clusters DropTable, clusterID:%d, schema:%v, tableID:%d",
			c.clusterID, schema, tableID)
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	schema.dropTableLocked(tableName)
	for _, shard := range c.shardsCache {
		shard.dropTableLocked(tableID)
	}
	return nil
}

func (c *Cluster) CreateSchema(ctx context.Context, schemaName string, schemaID uint32) (*Schema, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	// check if exists
	schema, exists := c.GetSchema(schemaName)
	if exists {
		return schema, nil
	}

	// persist
	schemaPb := &clusterpb.Schema{Id: schemaID, Name: schemaName, ClusterId: c.clusterID}
	err := c.storage.CreateSchema(ctx, c.clusterID, schemaPb)
	if err != nil {
		return nil, errors.Wrap(err, "clusters CreateSchema")
	}

	// cache
	schema = c.updateSchemaCacheLocked(schemaPb)
	return schema, nil
}

func (c *Cluster) CreateTable(ctx context.Context, schemaName string, shardID uint32,
	tableName string, tableID uint64,
) (*Table, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check provided schema if exists.
	schema, exists := c.GetSchema(schemaName)
	if !exists {
		return nil, ErrSchemaNotFound.WithCausef("schemaName", schemaName)
	}

	// Save table in storage.
	tablePb := &clusterpb.Table{Id: tableID, Name: tableName, SchemaId: schema.GetID(), ShardId: shardID}
	err := c.storage.CreateTable(ctx, c.clusterID, schema.GetID(), tablePb)
	if err != nil {
		return nil, errors.Wrap(err, "clusters CreateTable")
	}

	// Update tableCache in memory.
	table := c.updateTableCacheLocked(schema, tablePb)
	return table, nil
}

func (c *Cluster) loadClusterTopologyLocked(ctx context.Context) error {
	clusterTopology, err := c.storage.GetClusterTopology(ctx, c.clusterID)
	if err != nil {
		return errors.Wrap(err, "clusters loadClusterTopologyLocked")
	}
	c.metaData.clusterTopology = clusterTopology

	if c.metaData.clusterTopology == nil {
		return ErrClusterTopologyNotFound.WithCausef("clusters:%v", c)
	}

	shardMap := make(map[uint32][]*clusterpb.Shard)
	for _, shard := range c.metaData.clusterTopology.ShardView {
		shardMap[shard.Id] = append(shardMap[shard.Id], shard)
	}
	c.metaData.shards = shardMap

	shardIDs := make([]uint32, len(shardMap))
	for id := range shardMap {
		shardIDs = append(shardIDs, id)
	}

	c.metaData.shardIDs = shardIDs
	return nil
}

func (c *Cluster) loadShardTopologyLocked(ctx context.Context, shardIDs []uint32) error {
	topologies, err := c.storage.ListShardTopologies(ctx, c.clusterID, shardIDs)
	if err != nil {
		return errors.Wrap(err, "clusters loadShardTopologyLocked")
	}
	shardTopologyMap := make(map[uint32]*clusterpb.ShardTopology, len(c.metaData.shardIDs))
	for i, topology := range topologies {
		shardTopologyMap[c.metaData.shardIDs[i]] = topology
	}
	c.metaData.shardTopologies = shardTopologyMap
	return nil
}

func (c *Cluster) loadSchemaLocked(ctx context.Context) error {
	schemas, err := c.storage.ListSchemas(ctx, c.clusterID)
	if err != nil {
		return errors.Wrap(err, "clusters loadSchemaLocked")
	}
	schemaMap := make(map[string]*clusterpb.Schema, len(schemas))
	for _, schema := range schemas {
		schemaMap[schema.Name] = schema
	}
	c.metaData.schemas = schemaMap
	return nil
}

func (c *Cluster) loadTableLocked(ctx context.Context) error {
	for _, schema := range c.metaData.schemas {
		tables, err := c.storage.ListTables(ctx, c.clusterID, schema.Id)
		if err != nil {
			return errors.Wrap(err, "clusters loadTableLocked")
		}
		for _, table := range tables {
			if t, ok := c.metaData.tables[schema.GetName()]; ok {
				t[table.GetId()] = table
			} else {
				c.metaData.tables[schema.GetName()] = map[uint64]*clusterpb.Table{table.GetId(): table}
			}
		}
	}
	return nil
}

func (c *Cluster) GetSchema(schemaName string) (*Schema, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	schema, ok := c.schemasCache[schemaName]
	return schema, ok
}

func (c *Cluster) GetTable(ctx context.Context, schemaName, tableName string) (*Table, bool, error) {
	c.lock.RLock()
	schema, ok := c.schemasCache[schemaName]
	c.lock.RUnlock()
	if !ok {
		return nil, false, ErrSchemaNotFound.WithCausef("schemaName", schemaName)
	}

	table, exists := schema.getTable(tableName)
	if exists {
		return table, true, nil
	}
	// find in storage
	tablePb, exists, err := c.storage.GetTable(ctx, c.clusterID, schema.GetID(), tableName)
	if err != nil {
		return nil, false, errors.Wrap(err, "clusters GetTable")
	}
	if exists {
		c.lock.Lock()
		defer c.lock.Unlock()
		table := c.updateTableCacheLocked(schema, tablePb)
		return table, true, nil
	}

	return nil, false, nil
}

func (c *Cluster) GetShardIDs(node string) ([]uint32, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	shardIDs, ok := c.nodesCache[node]
	if !ok {
		return nil, ErrNodeNotFound.WithCausef("clusters GetShardIDs, node:%s", c, node)
	}
	return shardIDs, nil
}

func (c *Cluster) RegisterNode(ctx context.Context, node string, lease uint32) error {
	nodePb := &clusterpb.Node{NodeStats: &clusterpb.NodeStats{Lease: lease}, Name: node}
	nodePb1, err := c.storage.CreateOrUpdateNode(ctx, c.clusterID, nodePb)
	if err != nil {
		return errors.Wrapf(err, "clusters manager RegisterNode, node:%s", node)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.metaData.nodes[node] = nodePb1
	return nil
}

type metaData struct {
	cluster         *clusterpb.Cluster
	clusterTopology *clusterpb.ClusterTopology
	shardIDs        []uint32
	shards          map[uint32][]*clusterpb.Shard          // shard_id -> shard
	shardTopologies map[uint32]*clusterpb.ShardTopology    // shard_id -> shardTopology
	schemas         map[string]*clusterpb.Schema           // schema_name -> schema
	tables          map[string]map[uint64]*clusterpb.Table // schema_name-> (table_id -> table)
	nodes           map[string]*clusterpb.Node             // node_name -> node
}
