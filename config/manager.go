package config

import (
	"context"

	"encoding/json"
	"fmt"

	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type (
	Value struct {
		CreateRevision int64
		ModifyRevision int64
		Version        int64
		Bytes          string
	}

	ConfigMap = map[string]interface{}

	Manager interface {
		ListHistory(context.Context, Name, int64) ([]*Value, error)
		Get(context.Context, Name, int64) (*Value, error)
		Set(context.Context, Name, string, int64) (*Value, error)
		SetNX(context.Context, Name, string) (*Value, error)
		SetObject(context.Context, Name, interface{}, int64) (*Value, error)
		Delete(context.Context, Name, int64) (*Value, error)
		Rollback(context.Context, Name, int64) (*Value, error)
		Compact(context.Context, int64) error
		Fetch(context.Context, Name) (ConfigMap, error)
	}

	EtcdConfigManager struct {
		Client  *etcd.Client
		Timeout time.Duration
	}
)

func NewDeletedValue(rev int64) *Value {
	return &Value{ModifyRevision: rev}
}

func NewValue(kv *mvccpb.KeyValue) *Value {
	return &Value{
		CreateRevision: kv.CreateRevision,
		ModifyRevision: kv.ModRevision,
		Version:        kv.Version,
		Bytes:          string(kv.Value),
	}
}

func (v *Value) IsDelete() bool {
	return v.CreateRevision == 0
}

func (v *Value) IsCreate() bool {
	return v.CreateRevision == v.ModifyRevision
}

func (v *Value) IsModify() bool {
	return !v.IsDelete() && v.CreateRevision != v.ModifyRevision
}

func NewEtcdConfigManager(cli *etcd.Client, timeout time.Duration) Manager {
	c := &EtcdConfigManager{Client: cli, Timeout: timeout}
	return c
}

func (c *EtcdConfigManager) ListHistory(ctx context.Context, name Name, revision int64) ([]*Value, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	if 0 == revision {
		resp, err := c.Client.Get(ctx, name.Key(), etcd.WithKeysOnly())
		if err != nil || resp == nil {
			return nil, fmt.Errorf("failed to get config history from %s. error = %v", name.Key(), err)
		}
		if 0 == len(resp.Kvs) {
			return []*Value{}, nil
		}
		revision = resp.Kvs[0].CreateRevision
	}

	wch := c.Client.Watch(ctx, name.Key(), etcd.WithRev(revision))
	rsp, ok := <-wch
	if !ok {
		return nil, fmt.Errorf("failed to wait watch response. key = %s", name.Key())
	}
	if rsp.Canceled {
		return nil, fmt.Errorf("the watch is cancelled. key = %s", name.Key())
	}

	result := make([]*Value, 0, 10)
	for _, e := range rsp.Events {
		var value *Value
		if etcd.EventTypeDelete == e.Type {
			value = NewDeletedValue(rsp.Header.Revision)
		} else {
			value = NewValue(e.Kv)
		}
		result = append(result, value)
	}
	return result, nil
}

func (c *EtcdConfigManager) Get(ctx context.Context, name Name, revision int64) (*Value, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	resp, err := c.Client.Get(ctx, name.Key(), etcd.WithRev(revision))
	if err != nil || resp == nil {
		return nil, fmt.Errorf("failed to get config from %s. revision = %d, error = %v",
			name.Key(), revision, err)
	}
	switch len(resp.Kvs) {
	case 0:
		return nil, fmt.Errorf("failed to get config from %s. revision = %d", name.Key(), revision)
	case 1:
		return NewValue(resp.Kvs[0]), nil
	default:
		return nil, fmt.Errorf("multiple configs from %s", name.Key())
	}
}

func (c *EtcdConfigManager) Set(ctx context.Context, name Name, value string, revision int64) (*Value, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	txn := c.Client.Txn(ctx)
	if 0 != revision {
		txn = txn.If(etcd.Compare(etcd.ModRevision(name.Key()), "=", revision))
	}
	resp, err := txn.Then(etcd.OpPut(name.Key(), value), etcd.OpGet(name.Key())).Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to put config. key = %s, value = %s, revision = %d, error = %v",
			name.Key(), string(value), revision, err)
	}

	if !resp.Succeeded {
		return nil, fmt.Errorf("failed to put config. key = %s, value = %s, revision = %d",
			name.Key(), string(value), revision)

	}

	kv := resp.Responses[1].GetResponseRange().Kvs[0]
	return NewValue(kv), nil
}

func (c *EtcdConfigManager) SetNX(ctx context.Context, name Name, value string) (*Value, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	resp, err := c.Client.Txn(ctx).
		If(etcd.Compare(etcd.Version(name.Key()), "=", 0)).
		Then(etcd.OpPut(name.Key(), value), etcd.OpGet(name.Key())).
		Else(etcd.OpGet(name.Key())).
		Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to put config. key = %s, value = %s, error = %v",
			name.Key(), string(value), err)
	}

	if resp.Succeeded {
		kv := resp.Responses[1].GetResponseRange().Kvs[0]
		return NewValue(kv), nil
	} else {
		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		return NewValue(kv), nil
	}
}

func (c *EtcdConfigManager) SetObject(ctx context.Context, name Name, obj interface{}, revision int64) (*Value, error) {
	var err error
	val := make([]byte, 0, 10)
	if _, ok := obj.(*[]byte); ok {
		val = *obj.(*[]byte)
	} else {
		val, err = json.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal config. error = %v, value = %#v", err, obj)
		}
	}

	return c.Set(ctx, name, string(val), revision)
}

func (c *EtcdConfigManager) Delete(ctx context.Context, name Name, revision int64) (*Value, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	if 0 == revision {
		resp, err := c.Client.Delete(ctx, name.Key())
		if err != nil {
			return nil, fmt.Errorf("failed to delete config. key = %s, revision = %d, error = %v",
				name.Key(), revision, err)
		}
		return NewDeletedValue(resp.Header.Revision), nil
	} else {
		resp, err := c.Client.Txn(ctx).
			If(etcd.Compare(etcd.ModRevision(name.Key()), "=", revision)).
			Then(etcd.OpDelete(name.Key())).
			Commit()
		if err != nil {
			return nil, fmt.Errorf("failed to delete config. key = %s, revision = %d, error = %v",
				name.Key(), revision, err)
		}

		if !resp.Succeeded {
			return nil, fmt.Errorf("failed to delete config. key = %s, revision = %d", name.Key(), revision)
		}
		return NewDeletedValue(resp.Header.Revision), nil
	}
}

func (c *EtcdConfigManager) Rollback(ctx context.Context, name Name, revision int64) (*Value, error) {
	if 0 == revision {
		return nil, fmt.Errorf("missing revision")
	}

	val, err := c.Get(ctx, name, revision)
	if err != nil {
		return nil, fmt.Errorf("failed to rollback %s to revision-%d. error = %v", name.Key(), revision, err)
	}

	val, err = c.Set(ctx, name, val.Bytes, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to rollback %s to revision-%d. error = %v", name.Key(), revision, err)
	}
	return val, nil
}

func (c *EtcdConfigManager) Compact(ctx context.Context, revision int64) error {
	if 0 == revision {
		return fmt.Errorf("missing revision")
	}

	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	_, err := c.Client.Compact(ctx, revision, etcd.WithCompactPhysical())
	if err != nil {
		return fmt.Errorf("failed to compact ETCD to revision-%d. error = %v", revision, err)
	}
	return nil
}

func (c *EtcdConfigManager) Fetch(ctx context.Context, name Name) (ConfigMap, error) {
	svc := NewEtcdConfigService(c.Client, name)

	o := ConfigMap{}
	if e := svc.Fetch(ctx, &o); e != nil {
		return nil, e
	}
	return o, nil
}
