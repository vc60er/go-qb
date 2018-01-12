#!/bin/bash -e

export PATH=$PATH:/Users/chengning/code/etcd/bin

export ETCDCTL_API=3
export ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.1:22379,http://127.0.0.1:32379
etcdctl --endpoints=$ENDPOINTS $@


