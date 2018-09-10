package config

import (
	"log"
	"testing"
	"time"

	"context"

	"reflect"

	"encoding/json"

	"github.com/coreos/etcd/clientv3"
)

func TestEtcdConfigManager_ListHistory(t *testing.T) {
	etcdConf := GetDefaultEtcdConfig()
	cli, err := clientv3.New(etcdConf)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close() // make sure to close the client

	name := Name{Service: "svc", Cluster: "set", Instance: "instance"}
	manager := NewEtcdConfigManager(cli, 5*time.Second)

	v0, err := manager.Delete(context.TODO(), name, 0)
	if err != nil {
		t.Fatal(err)
	}
	manager.Compact(context.TODO(), v0.ModifyRevision)

	v1, err := manager.Set(context.TODO(), name, "v1", 0)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("create the first version: %#v\n", v1)

	history, err := manager.ListHistory(context.TODO(), name, v1.CreateRevision)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("list history: %#v\n", history[0])
	if 1 != len(history) {
		t.Fatalf("expected 1 history version, but actual is %d", len(history))
	}
	if !reflect.DeepEqual(history[0], v1) {
		t.Fatalf("list wrong history version. expected = %#v, actual = %#v", v1, history[0])
	}

	v2, err := manager.Set(context.TODO(), name, "v2", 0)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("set the second version: %#v\n", v2)

	history, err = manager.ListHistory(context.TODO(), name, v1.CreateRevision)
	if err != nil {
		t.Fatal(err)
	}
	for i, h := range history {
		log.Printf("list history[%d]: %#v\n", i, h)
	}
	if 2 != len(history) {
		t.Fatalf("expected 2 history versions, but actual is %d", len(history))
	}
	expected := []*Value{v1, v2}
	if !reflect.DeepEqual(history, expected) {
		t.Fatalf("list wrong history versions. expected = %#v, actual = %#v", expected, history)
	}

	v3, err := manager.Rollback(context.TODO(), name, v1.ModifyRevision)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("rollback to v1. %#v\n", v3)
	if v3.Bytes != v1.Bytes {
		t.Fatalf("failed to rollback. expected = %s, actual = %s", v1.Bytes, v3.Bytes)
	}

	v4, err := manager.Set(context.TODO(), name, "v4", v3.ModifyRevision)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("change to v4: %#v\n", v4)

	v5, err := manager.Delete(context.TODO(), name, v4.ModifyRevision)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("deleted v4\n")

	history, err = manager.ListHistory(context.TODO(), name, v1.CreateRevision)
	if err != nil {
		t.Fatal(err)
	}
	for i, h := range history {
		log.Printf("list history[%d]: %#v\n", i, h)
	}
	if 5 != len(history) {
		t.Fatalf("expected 1 history version, but actual is %d", len(history))
	}
	expected = []*Value{v1, v2, v3, v4, {ModifyRevision: v5.ModifyRevision}}
	if !reflect.DeepEqual(history, expected) {
		t.Fatalf("list wrong history versions. expected = %#v, actual = %#v", expected, history)
	}
}

func TestEtcdConfigManager_SetWrongRevision(t *testing.T) {
	etcdConf := GetDefaultEtcdConfig()
	cli, err := clientv3.New(etcdConf)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close() // make sure to close the client

	name := Name{Service: "svc", Cluster: "set", Instance: "instance"}
	manager := NewEtcdConfigManager(cli, 5*time.Second)

	v0, err := manager.Delete(context.TODO(), name, 0)
	if err != nil {
		t.Fatal(err)
	}
	manager.Compact(context.TODO(), v0.ModifyRevision)

	_, err = manager.Set(context.TODO(), name, "v1", 1)
	if err == nil {
		t.Fatalf("expected an error")
	} else {
		log.Printf("expected error: %#v\n", err)
	}
}

func TestEtcdConfigManager_Fetch(t *testing.T) {
	etcdConf := GetDefaultEtcdConfig()
	cli, err := clientv3.New(etcdConf)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close() // make sure to close the client

	name := Name{Service: "svc", Cluster: "set", Instance: "instance"}
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

	merged, err := manager.Fetch(context.TODO(), name)
	if err != nil {
		log.Panic(err)
	}

	bytes := make([]byte, 0)
	bytes, err = json.Marshal(merged)
	if err != nil {
		log.Fatal(err)
	}

	cfg := Config{}
	if err = json.Unmarshal(bytes, &cfg); err != nil {
		log.Fatal(err)
	}
	bytes, err = json.Marshal(&cfg)
	if err != nil {
		log.Fatal(err)
	}

	actual := string(bytes)
	expect := `{"IntValue":10,"StringValue":"instance","IntArray":[8,9],"MapValue":{"MapKey":10,"NewKey":20},"EmbeddedType":{"EmbeddedInt":100,"EmbeddedString":"InstanceEmbeddedString"},"ShareValue":"ShareStringValue"}`
	if actual != expect {
		t.Errorf("Wrong static config. expect = %s, actual = %s.", expect, actual)
	}
}
