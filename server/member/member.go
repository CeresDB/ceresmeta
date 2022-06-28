// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package member

import (
	"context"
	"fmt"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const leaderCheckInterval = time.Duration(50) * time.Millisecond

// Member manages the
type Member struct {
	ID               uint64
	Name             string
	rootPath         string
	leaderKey        string
	clusterKV        etcdutil.ClusterKV
	etcdLeaderGetter etcdutil.EtcdLeaderGetter
	leader           *metapb.Member
	rpcTimeout       time.Duration
}

func formatLeaderKey(rootPath string) string {
	return fmt.Sprintf("%s/members/leader", rootPath)
}

func NewMember(rootPath string, id uint64, name string, clusterKV etcdutil.ClusterKV, etcdLeaderGetter etcdutil.EtcdLeaderGetter, rpcTimeout time.Duration) *Member {
	leaderKey := formatLeaderKey(rootPath)
	return &Member{
		ID:               id,
		Name:             name,
		rootPath:         rootPath,
		leaderKey:        leaderKey,
		clusterKV:        clusterKV,
		etcdLeaderGetter: etcdLeaderGetter,
		leader:           nil,
		rpcTimeout:       rpcTimeout,
	}
}

// GetLeader gets the leader of the cluster.
// GetLeaderResp.Leader == nil if no leader found.
func (m *Member) GetLeader(ctx context.Context) (*GetLeaderResp, error) {
	ctx, cancel := context.WithTimeout(ctx, m.rpcTimeout)
	defer cancel()
	resp, err := m.clusterKV.Get(ctx, m.leaderKey)
	if err != nil {
		return nil, ErrGetLeader.WithCause(err)
	}
	if len(resp.Kvs) > 1 {
		return nil, ErrMultipleLeader
	}
	if len(resp.Kvs) == 0 {
		return &GetLeaderResp{}, nil
	}
	leaderKv := resp.Kvs[0]
	leader := &metapb.Member{}
	err = proto.Unmarshal(leaderKv.Value, leader)
	if err != nil {
		return nil, ErrInvalidLeaderValue.WithCause(err)
	}
	return &GetLeaderResp{Leader: leader, Revision: leaderKv.ModRevision}, nil
}

func (m *Member) ResetLeader(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, m.rpcTimeout)
	defer cancel()
	if _, err := m.clusterKV.Delete(ctx, m.leaderKey); err != nil {
		return ErrResetLeader.WithCause(err)
	}
	return nil
}

func (m *Member) WaitForLeaderChange(ctx context.Context, watcher clientv3.Watcher, revision int64) {
	defer func() {
		if err := watcher.Close(); err != nil {
			log.Error("close watcher failed", zap.Error(err))
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		wch := watcher.Watch(ctx, m.leaderKey, clientv3.WithRev(revision))
		for resp := range wch {
			// meet compacted error, use the compact revision.
			if resp.CompactRevision != 0 {
				log.Warn("required revision has been compacted, use the compact revision",
					zap.Int64("required-revision", revision),
					zap.Int64("compact-revision", resp.CompactRevision))
				revision = resp.CompactRevision
				break
			}

			if resp.Canceled {
				log.Error("watcher is cancelled", zap.Int64("revision", revision), zap.String("leader-key", m.leaderKey))
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Info("current leader is deleted", zap.String("leader-key", m.leaderKey))
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (m *Member) CampaignAndKeepLeader(ctx context.Context, rawLease clientv3.Lease, leaseTTLSec int64) error {
	leaderVal, err := m.Marshal()
	if err != nil {
		return err
	}

	newLease := newLease(rawLease, leaseTTLSec)
	if err := newLease.Grant(ctx); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, m.rpcTimeout)
	defer cancel()
	// The leader key must not exist, so the CreateRevision is 0.
	cmp := clientv3.Compare(clientv3.CreateRevision(m.leaderKey), "=", 0)
	resp, err := m.clusterKV.
		Txn(ctx).
		If(cmp).
		Then(clientv3.OpPut(m.leaderKey, leaderVal, clientv3.WithLease(newLease.ID))).
		Commit()
	if err != nil {
		closeErr := newLease.Close(ctx)
		if closeErr != nil {
			log.Error("close lease failed after txn put leader", zap.Error(closeErr))
		}
		return ErrTxnPutLeader.WithCause(err)
	}
	if !resp.Succeeded {
		closeErr := newLease.Close(ctx)
		if closeErr != nil {
			log.Error("close lease failed after txn put leader", zap.Error(closeErr))
		}
		return ErrTxnPutLeader.WithCausef("txn put leader failed, resp:%v", resp)
	}

	log.Info("succeed to set leader", zap.String("leader-key", m.leaderKey), zap.String("leader", m.Name))

	// keep the leadership after success in campaigning leader.
	go newLease.KeepAlive(ctx)

	// check the leadership periodically and exit if it changes.
	leaderCheckTicker := time.NewTicker(leaderCheckInterval)
	defer leaderCheckTicker.Stop()

	for {
		select {
		case <-leaderCheckTicker.C:
			if !newLease.IsLeader() {
				log.Info("no longer a leader because lease has expired")
				return nil
			}
			etcdLeader := m.etcdLeaderGetter.GetLeader()
			if etcdLeader != m.ID {
				log.Info("etcd leader changed and should re-assign the leadership", zap.String("old-leader", m.Name))
				return nil
			}
		case <-ctx.Done():
			log.Info("server is closed")
			return nil
		}
	}
}

func (m *Member) Marshal() (string, error) {
	memPb := &metapb.Member{
		Name: m.Name,
		Id:   m.ID,
	}
	bs, err := proto.Marshal(memPb)
	if err != nil {
		return "", ErrMarshalMember.WithCause(err)
	}

	return string(bs), nil
}

type GetLeaderResp struct {
	Leader   *metapb.Member
	Revision int64
}
