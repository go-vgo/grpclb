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
	"log"
	"strings"
	"time"

	etcd3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
)

var (
	// Prefix should start and end with no slash
	Prefix     = "etcd3_naming"
	client     etcd3.Client
	serviceKey string

	stopSignal = make(chan bool, 1)
)

// Opt registry grpc option
type Opt struct {
	Name     string
	Host     string
	Port     int
	Target   string
	Interval time.Duration
	Ttl      int
}

// Registry gRPC naming and discovery
func Registry(opt Opt, args ...int) error {
	serviceValue := fmt.Sprintf("%s:%d", opt.Host, opt.Port)
	serviceKey = fmt.Sprintf("/%s/%s/%s", Prefix, opt.Name, serviceValue)

	// get endpoints for register dial address
	var (
		err    error
		client *etcd3.Client
	)

	if len(args) > 0 {
		client, err = etcd3.New(etcd3.Config{
			Endpoints: strings.Split(opt.Target, ","),
		})
	} else {
		client, err = etcd3.NewFromURL(opt.Target)
	}

	if err != nil {
		return fmt.Errorf("grpclb: create etcd3 client failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	insertFunc := func() error {
		// minimum lease TTL is ttl-second
		resp, _ := client.Grant(context.TODO(), int64(opt.Ttl))
		// should get first, if not exist, set it
		// context.Background()
		_, err := client.Get(ctx, serviceKey)
		if err != nil {
			if err == rpctypes.ErrKeyNotFound {
				// ctx context.TODO()
				if _, err := client.Put(
					ctx, serviceKey, serviceValue, etcd3.WithLease(resp.ID)); err != nil {
					log.Printf("grpclb: set service '%s' with ttl to etcd3 failed: %s",
						opt.Name, err.Error())
				}
			} else {
				log.Printf("grpclb: service '%s' connect to etcd3 failed: %s",
					opt.Name, err.Error())
			}
		} else {
			// refresh set to true for not notifying the watcher
			// context.Background()
			if _, err := client.Put(
				ctx, serviceKey, serviceValue, etcd3.WithLease(resp.ID)); err != nil {
				log.Printf("grpclb: refresh service '%s' with ttl to etcd3 failed: %s",
					opt.Name, err.Error())
			}
		}
		return nil
	}

	go func() error {
		err := insertFunc()
		if err != nil {
			log.Printf("insertFunc err %v", err)
			return err
		}

		// invoke self-register with ticker
		ticker := time.NewTicker(opt.Interval)

		for {
			select {
			case <-stopSignal:
				cancel()
				return nil
			case <-ticker.C:
				insertFunc()
				// return
			case <-ctx.Done():
				ticker.Stop()
				client.Delete(context.Background(), serviceKey)
				//, &etcd3.DeleteOptions{Recursive: true}
				return nil
			}
		}
	}()

	return nil
}

// Register gRPC naming and discovery
func Register(name string, host string, port int,
	target string, interval time.Duration, ttl int, args ...int) error {
	serviceValue := fmt.Sprintf("%s:%d", host, port)
	serviceKey = fmt.Sprintf("/%s/%s/%s", Prefix, name, serviceValue)

	// get endpoints for register dial address
	var (
		err    error
		client *etcd3.Client
	)

	if len(args) > 0 {
		client, err = etcd3.New(etcd3.Config{
			Endpoints: strings.Split(target, ","),
		})
	} else {
		client, err = etcd3.NewFromURL(target)
	}

	if err != nil {
		return fmt.Errorf("grpclb: create etcd3 client failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	insertFunc := func() error {
		// minimum lease TTL is ttl-second
		resp, _ := client.Grant(context.TODO(), int64(ttl))
		// should get first, if not exist, set it
		// context.Background()
		_, err := client.Get(ctx, serviceKey)
		if err != nil {
			if err == rpctypes.ErrKeyNotFound {
				// ctx context.TODO()
				if _, err := client.Put(
					ctx, serviceKey, serviceValue, etcd3.WithLease(resp.ID)); err != nil {
					log.Printf("grpclb: set service '%s' with ttl to etcd3 failed: %s",
						name, err.Error())
				}
			} else {
				log.Printf("grpclb: service '%s' connect to etcd3 failed: %s",
					name, err.Error())
			}
		} else {
			// refresh set to true for not notifying the watcher
			// context.Background()
			if _, err := client.Put(
				ctx, serviceKey, serviceValue, etcd3.WithLease(resp.ID)); err != nil {
				log.Printf("grpclb: refresh service '%s' with ttl to etcd3 failed: %s",
					name, err.Error())
			}
		}
		return nil
	}

	go func() error {
		err := insertFunc()
		if err != nil {
			log.Printf("insertFunc err %v", err)
			return err
		}

		// invoke self-register with ticker
		ticker := time.NewTicker(interval)

		for {
			select {
			case <-stopSignal:
				cancel()
				return nil
			case <-ticker.C:
				insertFunc()
				// return
			case <-ctx.Done():
				ticker.Stop()
				client.Delete(context.Background(), serviceKey)
				//, &etcd3.DeleteOptions{Recursive: true}
				return nil
			}
		}
	}()

	return nil
}

// UnRegister delete registered service from etcd
func UnRegister() error {
	stopSignal <- true
	stopSignal = make(chan bool, 1) // just a hack to avoid multi UnRegister deadlock
	var err error
	if _, err := client.Delete(context.Background(), serviceKey); err != nil {
		log.Printf("grpclb: deregister '%s' failed: %s", serviceKey, err.Error())
	} else {
		log.Printf("grpclb: deregister '%s' ok.", serviceKey)
	}
	return err
}
