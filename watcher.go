// Copyright 2017 The go-vgo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// https://github.com/go-vgo/gt/blob/master/LICENSE
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package grpclb

import (
	"context"
	"fmt"

	etcd3 "github.com/coreos/etcd/clientv3"
	// "github.com/coreos/etcd/internal/mvcc/mvccpb"
	"google.golang.org/grpc/naming"
)

// watcher is the implementation of grpc.naming.Watcher
type watcher struct {
	re            *resolver // re: Etcd Resolver
	client        etcd3.Client
	isInitialized bool
	ctx           context.Context
	cancel        context.CancelFunc
}

// newWatcher new watcher
func newWatcher(re *resolver, cli etcd3.Client) naming.Watcher {
	ctx, cancel := context.WithCancel(context.Background())

	w := &watcher{
		re:     re,
		client: cli,
		ctx:    ctx,
		cancel: cancel,
	}

	return w
}

// Close do nothing
func (w *watcher) Close() {
	w.cancel()
}

// Next to return the updates
func (w *watcher) Next() ([]*naming.Update, error) {
	// prefix is the etcd prefix/value to watch
	prefix := fmt.Sprintf("/%s/%s/", Prefix, w.re.serviceName)

	// check if is initialized
	if !w.isInitialized {
		// query addresses from etcd
		resp, err := w.client.Get(w.ctx, prefix, etcd3.WithPrefix())
		w.isInitialized = true
		if err == nil {
			addrs := extractAddrs(resp)
			//if not empty, return the updates or watcher new dir
			if l := len(addrs); l != 0 {
				updates := make([]*naming.Update, l)
				for i := range addrs {
					updates[i] = &naming.Update{Op: naming.Add, Addr: addrs[i]}
				}
				return updates, nil
			}
		}
	}

	// generate etcd Watcher
	rch := w.client.Watch(w.ctx, prefix, etcd3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			// case mvccpb.PUT:
			case etcd3.EventTypePut:
				return []*naming.Update{{Op: naming.Add, Addr: string(ev.Kv.Value)}}, nil
			// case mvccpb.DELETE:
			case etcd3.EventTypeDelete:
				return []*naming.Update{{Op: naming.Delete, Addr: string(ev.Kv.Value)}}, nil
			}
		}
	}
	return nil, nil
}

func extractAddrs(resp *etcd3.GetResponse) []string {
	addrs := []string{}

	if resp == nil || resp.Kvs == nil {
		return addrs
	}

	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			addrs = append(addrs, string(v))
		}
	}

	return addrs
}
