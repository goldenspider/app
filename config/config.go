package config

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/imdario/mergo"
	"go.uber.org/zap"
)

var (
	envEtcdNodes   = "ETCD_NODES"
	envEtcdTimeout = "ETCD_TIMEOUT"

	keyShare          = "/conf/public"
	keyFormatService  = "/conf/%s/public"
	keyFormatCluster  = "/conf/%s/%s/public"
	keyFormatInstance = "/conf/%s/%s/%s"
)

type (
	Name struct {
		Service  string
		Cluster  string
		Instance string
	}

	WatchFunc = func(interface{})

	Service interface {
		Fetch(context.Context, interface{}) error
		AddWatcher(WatchFunc)
		Watch(context.Context, interface{}) error
	}

	EtcdConfigService struct {
		Client   *etcd.Client
		name     Name
		lock     sync.RWMutex
		watchers []WatchFunc
	}
)

func NewName(service, cluster, instance string) Name {
	return Name{Service: service, Cluster: cluster, Instance: instance}
}

func (n *Name) Key() string {
	if "" == n.Service {
		return keyShare
	} else {
		if "" == n.Cluster {
			return fmt.Sprintf(keyFormatService, n.Service)
		} else {
			if "" == n.Instance {
				return fmt.Sprintf(keyFormatCluster, n.Service, n.Cluster)
			} else {
				return fmt.Sprintf(keyFormatInstance, n.Service, n.Cluster, n.Instance)
			}
		}
	}
	return keyShare
}

func (n *Name) ShareKey() string {
	return keyShare
}

func (n *Name) ServiceKey() string {
	return fmt.Sprintf(keyFormatService, n.Service)
}

func (n *Name) ClusterKey() string {
	return fmt.Sprintf(keyFormatCluster, n.Service, n.Cluster)
}

func (n *Name) InstanceKey() string {
	return fmt.Sprintf(keyFormatInstance, n.Service, n.Cluster, n.Instance)
}

func (n *Name) ServiceName() Name {
	return Name{Service: n.Service}
}

func (n *Name) ClusterName() Name {
	return Name{Service: n.Service, Cluster: n.Cluster}
}

func GetDefaultEtcdConfig() etcd.Config {
	cfg := etcd.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	}

	nodes := strings.Split(os.Getenv(envEtcdNodes), ",")
	if "" != nodes[0] {
		cfg.Endpoints = nodes
		zap.S().Infof("load env. %s = %v", envEtcdNodes, nodes)
	}

	timeout, _ := strconv.Atoi(os.Getenv(envEtcdTimeout))
	if 0 != timeout {
		cfg.DialTimeout = time.Duration(timeout) * time.Second
		zap.S().Infof("load env. %s = %d", envEtcdTimeout, timeout)
	}
	return cfg
}

func NewEtcdConfigService(cli *etcd.Client, name Name) *EtcdConfigService {
	c := &EtcdConfigService{Client: cli, name: name, lock: sync.RWMutex{}, watchers: []WatchFunc{}}
	return c
}

func (c *EtcdConfigService) Fetch(ctx context.Context, cfg interface{}) error {
	if err := c.fetchConfig(ctx, c.name.InstanceKey(), true, cfg); err != nil {
		return err
	}

	if err := c.fetchConfig(ctx, c.name.ClusterKey(), true, cfg); err != nil {
		return err
	}

	if err := c.fetchConfig(ctx, c.name.ServiceKey(), false, cfg); err != nil {
		return err
	}

	if err := c.fetchConfig(ctx, c.name.ShareKey(), true, cfg); err != nil {
		return err
	}

	return nil
}

func (c *EtcdConfigService) AddWatcher(w WatchFunc) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.watchers = append(c.watchers, w)
}

func (c *EtcdConfigService) Watch(ctx context.Context, cfg interface{}) error {
	ch1 := c.Client.Watch(ctx, c.name.InstanceKey())
	ch2 := c.Client.Watch(ctx, c.name.ClusterKey())
	ch3 := c.Client.Watch(ctx, c.name.ServiceKey())
	ch4 := c.Client.Watch(ctx, c.name.ShareKey())
	for {
		select {
		case <-ch1:
			if err := c.fireUpdate(cfg); err != nil {
				return err
			}
		case <-ch2:
			if err := c.fireUpdate(cfg); err != nil {
				return err
			}
		case <-ch3:
			if err := c.fireUpdate(cfg); err != nil {
				return err
			}
		case <-ch4:
			if err := c.fireUpdate(cfg); err != nil {
				return err
			}
		}
	}
}

func (c *EtcdConfigService) fetchConfig(ctx context.Context, key string, optional bool, cfg interface{}) error {
	e := reflect.New(reflect.TypeOf(cfg).Elem()).Interface()
	if err := c.get(ctx, key, e, optional); err != nil {
		return fmt.Errorf("failed to get config. error = %v, key = %s, dst = %T, src = %T", err, key, cfg, e)
	}

	if err := mergo.Merge(cfg, e); err != nil {
		return fmt.Errorf("failed to merge config. error = %v, key = %s, dst = %T, src = %T", err, key, cfg, e)
	}
	return nil
}

func checkJSON(bytes []byte) []byte {
	str := string(bytes)
	str = strings.TrimSpace(str)
	if 0 == len(str) {
		str = "{}"
	}
	return []byte(str)
}

func (c *EtcdConfigService) unmarshal(bytes []byte, cfg interface{}) error {
	if _, ok := cfg.(*string); ok {
		cfg = string(checkJSON(bytes))
	} else {
		if err := json.Unmarshal(checkJSON(bytes), cfg); err != nil {
			return fmt.Errorf("failed to unmarshal value. error = %v, value = %s", err, string(bytes))
		}
	}
	return nil
}

func (c *EtcdConfigService) get(ctx context.Context, key string, cfg interface{}, optional bool) error {
	resp, err := c.Client.Get(ctx, key)
	if err != nil || resp == nil {
		return fmt.Errorf("failed to get config from %s. error = %v", key, err)
	}
	switch len(resp.Kvs) {
	case 0:
		if optional {
			return nil
		}
		return fmt.Errorf("failed to get config from %s", key)
	case 1:
		kv := resp.Kvs[0]
		return c.unmarshal(kv.Value, cfg)
	default:
		return fmt.Errorf("multiple configs from %s", key)
	}
}

func (c *EtcdConfigService) fireUpdate(cfg interface{}) error {
	v := reflect.New(reflect.TypeOf(cfg).Elem()).Interface()
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	if err := c.Fetch(ctx, v); err != nil {
		return err
	}

	c.lock.RLock()
	defer c.lock.RUnlock()
	for _, w := range c.watchers {
		w(v)
	}
	return nil
}

func GetConfigJSON(cfg interface{}) string {
	bytes, e := json.MarshalIndent(cfg, "", "  ")
	if e != nil {
		panic(e)
	}
	return string(bytes)
}

func CheckConfigJSON(path string, cfg interface{}) {
	bytes, e := ioutil.ReadFile(path)
	if e != nil {
		panic(e)
	}

	if e := json.Unmarshal(bytes, cfg); e != nil {
		panic(e)
	}
	fmt.Printf("%#v\n", cfg)
}
