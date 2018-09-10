package app

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	sd "github.com/coreos/etcd/clientv3/naming"
	"go.uber.org/zap"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/naming"
)

type (
	OnlineRegister struct {
		*zap.SugaredLogger
		name     string
		addr     string
		isMaster bool
		client   *clientv3.Client
		ttl      int
		session  *concurrency.Session
		online   uint32
	}
)

func newOnlineRegister(l *zap.SugaredLogger, client *clientv3.Client, name string, addr string, ttl int) (*OnlineRegister, error) {
	return &OnlineRegister{
		SugaredLogger: l,
		name:          name,
		addr:          addr,
		client:        client,
		ttl:           ttl,
		session:       nil,
		isMaster:      false,
	}, nil
}

func (r *OnlineRegister) Online() error {
	if !atomic.CompareAndSwapUint32(&r.online, 0, 1) {
		return nil
	}
	go func() {
		l := r.Named(fmt.Sprintf("OnlineRegister"))
		for atomic.CompareAndSwapUint32(&r.online, 1, 1) {
			if err := r.register(l); err != nil {
				time.Sleep(time.Second)
			}
		}
		l.Info("register is exited, name = %s, addr = %s", r.name, r.addr)
	}()
	return nil
}

func (r *OnlineRegister) register(l *zap.SugaredLogger) error {
	var e error
	r.session, e = concurrency.NewSession(r.client, concurrency.WithTTL(r.ttl))
	if e != nil {
		l.Warn("failed to create register session.", zap.Error(e))
		return e
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	resolver := &sd.GRPCResolver{Client: r.session.Client()}
	e = resolver.Update(ctx, r.name, naming.Update{Op: naming.Add, Addr: r.addr}, clientv3.WithLease(r.session.Lease()))
	if e != nil {
		l.Warn(fmt.Sprintf("failed to register name. name = %s, addr = %s", r.name, r.addr), zap.Error(e))
		return e
	}
	l.Infof("%s is registered. name = %s", r.addr, r.name)

	l = l.Named(fmt.Sprintf("RegisterWatchdog-%d", r.session.Lease()))
	l.Info("RegisterWatchdog is started")
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			r.doWatchdog(l, r.session)

		case <-r.session.Done():
			l.Info("RegisterWatchdog is exited")
			ticker.Stop()
			return nil

		case <-r.session.Client().Ctx().Done():
			l.Info("RegisterWatchdog is exited")
			ticker.Stop()
			return nil
		}
	}
	return nil
}

func (r *OnlineRegister) doWatchdog(l *zap.SugaredLogger, session *concurrency.Session) {
	state := session.Client().ActiveConnection().GetState()
	if state != connectivity.Ready {
		l.Warnf("the connection of ETCD isn't ready. state = %s", state)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := r.name + "/" + r.addr
	rsp, e := session.Client().Get(ctx, key, clientv3.WithCountOnly())
	if e != nil {
		l.Warnf("failed to get online key %s in ETCD", key, zap.Error(e))
		return
	}

	if 0 == rsp.Count {
		// Key is not exist, close session
		session.Close()
		return
	}
}

func (r *OnlineRegister) Offline() error {
	if !atomic.CompareAndSwapUint32(&r.online, 1, 0) {
		return nil
	}
	if e := r.session.Close(); e != nil {
		r.Warn("failed to close online session", zap.Error(e))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	resolver := &sd.GRPCResolver{Client: r.session.Client()}
	err := resolver.Update(ctx, r.name, naming.Update{Op: naming.Delete, Addr: r.addr})
	if err != nil {
		r.Warn(fmt.Sprintf("failed to unregister name. name = %s, addr = %s", r.name, r.addr), zap.Error(err))
		return nil
	}
	r.Infof("%s is offline. name = %s", r.addr, r.name)
	return nil
}

func (r *OnlineRegister) IsMaster(ctx context.Context) (bool, error) {
	resp, err := r.session.Client().Get(ctx, r.name+"/", clientv3.WithFirstCreate()...)
	if err != nil {
		return false, fmt.Errorf("failed to list %s from etcd, error = %v", r.name, err)
	}

	if len(resp.Kvs) == 0 {
		return false, fmt.Errorf("not find master service at %s", r.name)
	}

	key := string(resp.Kvs[0].Key)
	endpoint := strings.TrimPrefix(key, r.name+"/")
	isMaster := endpoint == r.addr

	if r.isMaster != isMaster {
		r.Warnf("master state switched from %v to %v.", r.isMaster, isMaster)
	}
	r.isMaster = isMaster
	return isMaster, nil
}
