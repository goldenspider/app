package config

import (
	"context"
	"encoding/json"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
)

type Embedded struct {
	EmbeddedInt    int    `json:"EmbeddedInt,omitempty"`
	EmbeddedString string `json:"EmbeddedString,omitempty"`
}

type Config struct {
	IntValue     int            `json:"IntValue,omitempty"`
	StringValue  string         `json:"StringValue,omitempty"`
	IntArray     []int          `json:"IntArray,omitempty"`
	MapValue     map[string]int `json:"MapValue,omitempty"`
	EmbeddedType Embedded       `json:"EmbeddedType,omitempty"`
	ShareValue   string         `json:"ShareValue,omitempty"`
}

func TestEtcdConfigService_LoadConfig(t *testing.T) {
	etcdConf := GetDefaultEtcdConfig()
	cli, err := clientv3.New(etcdConf)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close() // make sure to close the client

	name := Name{Service: "svc", Cluster: "set", Instance: "instance"}
	conf := NewEtcdConfigService(cli, name)
	manager := NewEtcdConfigManager(cli, 5*time.Second)

	share := &Config{
		ShareValue:  "ShareStringValue",
		StringValue: "ShareString",
	}
	if _, err = manager.SetObject(context.TODO(), Name{}, share, 0); err != nil {
		log.Fatal(err)
	}

	service := &Config{
		IntValue:    10,
		StringValue: "service",
		IntArray:    []int{0, 1, 2, 3},
		MapValue: map[string]int{
			"MapKey": 10,
		},
		EmbeddedType: Embedded{
			EmbeddedInt: 100,
		},
	}
	if _, err = manager.SetObject(context.TODO(), name.ServiceName(), service, 0); err != nil {
		log.Fatal(err)
	}

	cluster := &Config{
		StringValue:  "cluster",
		IntArray:     []int{6, 7},
		MapValue:     map[string]int{"NewKey": 15},
		EmbeddedType: Embedded{EmbeddedString: "ClusterEmbeddedString"},
	}
	if _, err = manager.SetObject(context.TODO(), name.ClusterName(), cluster, 0); err != nil {
		log.Fatal(err)
	}

	instance := &Config{
		StringValue:  "instance",
		IntArray:     []int{8, 9},
		MapValue:     map[string]int{"NewKey": 20},
		EmbeddedType: Embedded{EmbeddedString: "InstanceEmbeddedString"},
	}
	if _, err = manager.SetObject(context.TODO(), name, instance, 0); err != nil {
		log.Fatal(err)
	}

	merged := &Config{}
	if err = conf.Fetch(context.TODO(), merged); err != nil {
		log.Panic(err)
	}

	bytes := make([]byte, 0)
	bytes, err = json.Marshal(merged)
	if err != nil {
		log.Fatal(err)
	}

	actual := string(bytes)
	expect := `{"IntValue":10,"StringValue":"instance","IntArray":[8,9],"MapValue":{"MapKey":10,"NewKey":20},"EmbeddedType":{"EmbeddedInt":100,"EmbeddedString":"InstanceEmbeddedString"},"ShareValue":"ShareStringValue"}`
	if actual != expect {
		t.Errorf("Wrong static config. expect = %s, actual = %s.", expect, actual)
	}
}

type FakeWatcher struct {
	Actual chan *Config
}

func (w *FakeWatcher) OnConfigUpdated(cfg interface{}) {
	c := cfg.(*Config)
	w.Actual <- c
}

func (w *FakeWatcher) Check(t *testing.T, expect *Config, timeout time.Duration) {
	select {
	case actual := <-w.Actual:
		if !reflect.DeepEqual(expect, actual) {
			t.Fatalf("wrong change. expect = %#v, actual = %#v", expect, actual)
		}

	case <-time.After(timeout):
		t.Fatalf("failed to receive change. expect = %#v", expect)
	}
}

func (w *FakeWatcher) ExpectEmpty(t *testing.T, timeout time.Duration) {
	time.Sleep(timeout)
	select {
	case actual := <-w.Actual:
		t.Fatalf("Expect no changes. actual = %#v", actual)

	default:
		return
	}
}

func TestEtcdConfigService_FetchAndWatchDynamicConfig(t *testing.T) {
	etcdConf := GetDefaultEtcdConfig()
	cli, err := clientv3.New(etcdConf)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close() // make sure to close the client

	name := Name{Service: "svc", Cluster: "set", Instance: "instance"}
	conf := NewEtcdConfigService(cli, name)
	manager := NewEtcdConfigManager(cli, 5*time.Second)

	if _, err = manager.SetObject(context.TODO(), Name{}, &Config{IntValue: 13}, 0); err != nil {
		log.Fatal(err)
	}
	if _, err = manager.SetObject(context.TODO(), name.ServiceName(), &Config{IntValue: 12}, 0); err != nil {
		log.Fatal(err)
	}
	if _, err = manager.SetObject(context.TODO(), name.ClusterName(), &Config{IntValue: 11}, 0); err != nil {
		log.Fatal(err)
	}
	if _, err = manager.SetObject(context.TODO(), name, &Config{IntValue: 10}, 0); err != nil {
		log.Fatal(err)
	}

	dyn := &Config{}
	if err = conf.Fetch(context.TODO(), dyn); err != nil {
		log.Fatal(err)
	}
	if 10 != dyn.IntValue {
		t.Errorf("Wrong dynamic config. expect = %d, actual = %d.", 10, dyn.IntValue)
	}

	w := &FakeWatcher{Actual: make(chan *Config, 10)}
	conf.AddWatcher(func(cfg interface{}) {
		w.OnConfigUpdated(cfg)
	})
	go func() {
		ctx, cancel := context.WithTimeout(context.TODO(), 7*time.Second)
		defer cancel()
		if err = conf.Watch(ctx, dyn); err != nil {
			log.Println(err)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	if _, err = manager.SetObject(context.TODO(), name, &Config{IntValue: 20}, 0); err != nil {
		log.Fatal(err)
	}
	w.Check(t, &Config{IntValue: 20}, 1*time.Second)

	if _, err = manager.SetObject(context.TODO(), name, &Config{IntValue: 30}, 0); err != nil {
		log.Fatal(err)
	}
	w.Check(t, &Config{IntValue: 30}, 1*time.Second)

	if _, err = manager.SetObject(context.TODO(), name.ClusterName(), &Config{IntValue: 40}, 0); err != nil {
		log.Fatal(err)
	}
	w.Check(t, &Config{IntValue: 30}, 1*time.Second)

	if _, err = manager.SetObject(context.TODO(), name.ServiceName(), &Config{IntValue: 40}, 0); err != nil {
		log.Fatal(err)
	}
	w.Check(t, &Config{IntValue: 30}, 1*time.Second)

	if _, err = manager.SetObject(context.TODO(), Name{}, &Config{ShareValue: "ShareValue"}, 0); err != nil {
		log.Fatal(err)
	}
	w.Check(t, &Config{IntValue: 30, ShareValue: "ShareValue"}, 1*time.Second)
	time.Sleep(100 * time.Millisecond)
}

func TestEtcdConfigService_EmptyString(t *testing.T) {
	etcdConf := GetDefaultEtcdConfig()
	cli, err := clientv3.New(etcdConf)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close() // make sure to close the client

	name := Name{Service: "svc", Cluster: "set", Instance: "instance"}
	if _, err := cli.Put(context.TODO(), name.ShareKey(), ""); err != nil {
		log.Fatal(err)
	}
	if _, err := cli.Put(context.TODO(), name.ServiceKey(), ""); err != nil {
		log.Fatal(err)
	}
	if _, err := cli.Put(context.TODO(), name.ClusterKey(), ""); err != nil {
		log.Fatal(err)
	}
	if _, err := cli.Put(context.TODO(), name.InstanceKey(), ""); err != nil {
		log.Fatal(err)
	}

	conf := NewEtcdConfigService(cli, name)
	merged := &Config{}
	if err = conf.Fetch(context.TODO(), merged); err != nil {
		log.Panic(err)
	}

	bytes := make([]byte, 0)
	bytes, err = json.Marshal(merged)
	if err != nil {
		log.Fatal(err)
	}

	actual := string(bytes)
	expect := `{"IntValue":0,"StringValue":"","IntArray":null,"MapValue":null,"EmbeddedType":{"EmbeddedInt":0,"EmbeddedString":""},"ShareValue":""}`
	if actual != expect {
		t.Errorf("Wrong static config. expect = %s, actual = %s.", expect, actual)
	}
}
