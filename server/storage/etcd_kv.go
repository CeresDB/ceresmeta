// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"path"
	"strings"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

const (
	DefaultRequestTimeout = 10 * time.Second

	delimiter = "/"
)

type etcdKVBase struct {
	client   *clientv3.Client
	rootPath string
}

// NewEtcdKVBase creates a new etcd kv.
//nolint
func NewEtcdKVBase(client *clientv3.Client, rootPath string) *etcdKVBase {
	return &etcdKVBase{
		client:   client,
		rootPath: rootPath,
	}
}

func (kv *etcdKVBase) Load(key string) (string, error) {
	key = path.Join(kv.rootPath, key)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	defer cancel()

	resp, err := kv.client.Get(ctx, key)
	if err != nil {
		return "", etcdutil.ErrEtcdKVGet.WithCause(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return "", nil
	} else if n > 1 {
		return "", etcdutil.ErrEtcdKVGetResponse.WithCausef("%v", resp.Kvs)
	}
	return string(resp.Kvs[0].Value), nil
}

func (kv *etcdKVBase) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
	key = strings.Join([]string{kv.rootPath, key}, delimiter)
	endKey = strings.Join([]string{kv.rootPath, endKey}, delimiter)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	defer cancel()

	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(int64(limit))
	resp, err := kv.client.Get(ctx, key, withRange, withLimit)
	if err != nil {
		return nil, nil, etcdutil.ErrEtcdKVGet.WithCause(err)
	}
	keys := make([]string, 0, len(resp.Kvs))
	values := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		keys = append(keys, strings.TrimPrefix(strings.TrimPrefix(string(item.Key), kv.rootPath), delimiter))
		values = append(values, string(item.Value))
	}
	return keys, values, nil
}

func (kv *etcdKVBase) Save(key, value string) error {
	key = strings.Join([]string{kv.rootPath, key}, delimiter)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	defer cancel()
	_, err := kv.client.Put(ctx, key, value)
	if err != nil {
		e := etcdutil.ErrEtcdKVPut.WithCause(err)
		log.Error("save to etcd meet error", zap.String("key", key), zap.String("value", value), zap.Error(e))
		return e
	}

	return nil
}

func (kv *etcdKVBase) Remove(key string) error {
	key = strings.Join([]string{kv.rootPath, key}, delimiter)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	defer cancel()
	_, err := kv.client.Delete(ctx, key)
	if err != nil {
		err = etcdutil.ErrEtcdKVDelete.WithCause(err)
		log.Error("remove from etcd meet error", zap.String("key", key), zap.Error(err))
		return err
	}

	return nil
}