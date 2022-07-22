// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"
	"math"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type Options struct {
	// MaxScanLimit is the max limit of the number of keys in a scan.
	MaxScanLimit int
	// MinScanLimit is the min limit of the number of keys in a scan.
	MinScanLimit int
}

// MetaStorageImpl is the base underlying storage endpoint for all other upper
// specific storage backends. It should define some common storage interfaces and operations,
// which provIDes the default implementations for all kinds of storages.
type MetaStorageImpl struct {
	KV

	opts Options
}

// NewMetaStorageImpl creates a new base storage endpoint with the given KV and encryption key manager.
// It should be embedded insIDe a storage backend.
func NewMetaStorageImpl(
	kv KV,
	opts Options,
) *MetaStorageImpl {
	return &MetaStorageImpl{kv, opts}
}

// newEtcdBackend is used to create a new etcd backend.
func newEtcdStorage(client *clientv3.Client, rootPath string, opts Options) *MetaStorageImpl {
	return NewMetaStorageImpl(
		NewEtcdKV(client, rootPath), opts)
}

func (s *MetaStorageImpl) ListClusters(ctx context.Context) ([]*clusterpb.Cluster, error) {
	return nil, nil
}

func (s *MetaStorageImpl) CreateCluster(ctx context.Context, cluster *clusterpb.Cluster) (*clusterpb.Cluster, error) {
	return nil, nil
}

func (s *MetaStorageImpl) GetCluster(ctx context.Context, clusterID uint32) (*clusterpb.Cluster, error) {
	return nil, nil
}

func (s *MetaStorageImpl) PutCluster(ctx context.Context, clusterID uint32, meta *clusterpb.Cluster) error {
	return nil
}

func (s *MetaStorageImpl) CreateClusterTopology(ctx context.Context, clusterTopology *clusterpb.ClusterTopology) error {
	return nil
}

func (s *MetaStorageImpl) GetClusterTopology(ctx context.Context, clusterID uint32) (*clusterpb.ClusterTopology, error) {
	return nil, nil
}

func (s *MetaStorageImpl) PutClusterTopology(ctx context.Context, clusterID uint32, clusterMetaData *clusterpb.ClusterTopology) error {
	return nil
}

func (s *MetaStorageImpl) CreateSchema(ctx context.Context, clusterID uint32, schema *clusterpb.Schema) error {
	return nil
}

func (s *MetaStorageImpl) ListSchemas(ctx context.Context, clusterID uint32) ([]*clusterpb.Schema, error) {
	schemas := make([]*clusterpb.Schema, 0)
	nextID := uint32(0)
	endKey := makeSchemaKey(clusterID, math.MaxUint32)

	rangeLimit := s.opts.MaxScanLimit
	for {
		startKey := makeSchemaKey(clusterID, nextID)
		_, res, err := s.Scan(ctx, startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= s.opts.MinScanLimit {
				continue
			}
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			schema := &clusterpb.Schema{}
			if err := proto.Unmarshal([]byte(r), schema); err != nil {
				return nil, ErrMetaGetSchemas.WithCausef("proto parse err:%v", err)
			}
			schemas = append(schemas, schema)
			if schema.GetId() == math.MaxUint32 {
				log.Warn("list schemas schema_id has reached max value", zap.Uint32("schema-id", schema.GetId()))
				return schemas, nil
			}
			nextID = schema.GetId() + 1
		}

		if len(res) < rangeLimit {
			return schemas, nil
		}
	}
}

func (s *MetaStorageImpl) PutSchemas(ctx context.Context, clusterID uint32, schemas []*clusterpb.Schema) error {
	return nil
}

func (s *MetaStorageImpl) CreateTable(ctx context.Context, clusterID uint32, schemaID uint32, table *clusterpb.Table) error {
	return nil
}

func (s *MetaStorageImpl) GetTable(ctx context.Context, clusterID uint32, schemaID uint32, tableName string) (*clusterpb.Table, bool, error) {
	return nil, false, nil
}

func (s *MetaStorageImpl) ListTables(ctx context.Context, clusterID uint32, schemaID uint32) ([]*clusterpb.Table, error) {
	return nil, nil
}

func (s *MetaStorageImpl) PutTables(ctx context.Context, clusterID uint32, schemaID uint32, tables []*clusterpb.Table) error {
	return nil
}

func (s *MetaStorageImpl) DeleteTables(ctx context.Context, clusterID uint32, schemaID uint32, tableIDs []uint64) error {
	return nil
}

func (s *MetaStorageImpl) ListShardTopologies(ctx context.Context, clusterID uint32, shardID []uint32) ([]*clusterpb.ShardTopology, error) {
	return nil, nil
}

func (s *MetaStorageImpl) PutShardTopologies(ctx context.Context, clusterID uint32, shardID []uint32, shardTableInfo []*clusterpb.ShardTopology) error {
	return nil
}

func (s *MetaStorageImpl) ListNodes(ctx context.Context, clusterID uint32) ([]*clusterpb.Node, error) {
	return nil, nil
}

func (s *MetaStorageImpl) PutNodes(ctx context.Context, clusterID uint32, node []*clusterpb.Node) error {
	return nil
}

func (s *MetaStorageImpl) CreateOrUpdateNode(ctx context.Context, clusterID uint32, node *clusterpb.Node) (*clusterpb.Node, error) {
	return nil, nil
}
