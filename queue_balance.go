package qb

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"math"
	"strconv"
	"strings"
	//"sync"
	//	"gopkg.in/ini.v1"
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
	log "github.com/golang/glog"
	"time"
)

type QueueBalance struct {
	cli           *clientv3.Client
	ses           *concurrency.Session
	mtxDispatcher *concurrency.Mutex

	last_trigger_dispatch time.Time

	prefix_queue                        string
	prefix_consumer_require_queue_count string
	prefix_consumer_subscribed          string

	prefix_queue_protected      string
	prefix_queue_status_changed string
	key_dispatcher              string

	queue_ids []string

	local_consumer_id string

	leaseID clientv3.LeaseID

	qm *QueueMgr
}

func NewQueueBalance(endpoints []string, amqp string, pOnMsg QueueOnMsg, queue_ids []string) (*QueueBalance, error) {
	pthis := QueueBalance{}

	var err error = nil
	pthis.cli, err = clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		return nil, err
	}

	pthis.ses, err = concurrency.NewSession(pthis.cli, concurrency.WithTTL(1))
	if err != nil {
		return nil, err
	}

	pthis.qm, err = NewMqMgr(amqp, pOnMsg)
	if err != nil {
		return nil, err
	}

	pthis.mtxDispatcher = concurrency.NewMutex(pthis.ses, "/qb/lock/dispatcher") // TODO: hard code

	pthis.prefix_consumer_subscribed = "/qb/consumer_subscribed/"
	pthis.prefix_consumer_require_queue_count = "/qb/consumer_require_queue_count/"
	pthis.prefix_queue_protected = "/qb/queue_protected/"
	pthis.prefix_queue_status_changed = "/qb/queue_status_changed/"
	pthis.prefix_queue = "/qb/queue/"
	pthis.key_dispatcher = "/qb/dispatcher"

	pthis.queue_ids = queue_ids
	pthis.local_consumer_id = fmt.Sprintf("consumer.%d", time.Now().Unix())
	pthis.leaseID = pthis.ses.Lease()

	log.Info("NewQueueBalance:", " leaseID=", pthis.leaseID, " queue_ids=", queue_ids)

	return &pthis, nil
}

func (pthis *QueueBalance) Close() {
	pthis.ses.Close()
	pthis.cli.Close()
}

func (pthis *QueueBalance) Run() {
	go pthis.dispatcher_run()
	go pthis.executor_run()
}

func (pthis *QueueBalance) consumer_require_queue_count_put(key string, value int) error {
	local := (key == "")
	if local {
		key = pthis.prefix_consumer_require_queue_count + pthis.local_consumer_id
	}

	log.Info("consumer_require_queue_count_put:", " key=", key, " value=", value)

	var op clientv3.OpOption = nil
	if local {
		op = clientv3.WithLease(pthis.leaseID)
	} else {
		op = clientv3.WithIgnoreLease()
	}

	_, err := pthis.cli.Put(context.TODO(), key, strconv.Itoa(value), op)
	if err != nil {
		return err
	}

	return nil
}

func (pthis *QueueBalance) consumer_require_queue_count_del() error {
	key := pthis.prefix_consumer_require_queue_count + pthis.local_consumer_id
	log.Info("consumer_require_queue_count_del:", " key=", key)

	_, err := pthis.cli.Delete(context.TODO(), key)
	if err != nil {
		return err
	}

	return nil
}

func (pthis *QueueBalance) consumer_require_queue_count_get() (int, error) {
	key := pthis.prefix_consumer_require_queue_count + pthis.local_consumer_id
	log.Info("consumer_require_queue_count_get:", " key=", key)

	resp, err := pthis.cli.Get(context.TODO(), key)
	if err != nil {
		return 0, err
	}

	if len(resp.Kvs) == 0 {
		return 0, errors.New("no value")
	}

	return strconv.Atoi(string(resp.Kvs[0].Value))
}

func (pthis *QueueBalance) consumer_subscribed_update() error {
	key := pthis.prefix_consumer_subscribed + pthis.local_consumer_id

	value := strings.Join(pthis.qm.GetSubscribed(), ",")

	log.V(10).Info("consumer_subscribed_update:", " key=", key, " value=", value)
	_, err := pthis.cli.Put(context.TODO(), key, value, clientv3.WithLease(pthis.leaseID))
	if err != nil {
		log.Error("consumer_subscribed_update:", " key=", key, " value=", value, " err=", err)
		return err
	}

	return nil
}

func (pthis *QueueBalance) executor_register() error {
	log.Info("executor_register:")
	return pthis.consumer_require_queue_count_put("", 0)
}

func (pthis *QueueBalance) executor_deregister() error {
	log.Info("executor_deregister")
	return pthis.consumer_require_queue_count_del()
}

func (pthis *QueueBalance) executor_run() error {
	log.Info("executor_run:")

	for {
		resp, err := pthis.cli.Get(context.TODO(), pthis.key_dispatcher)
		log.Info("executor_run:", " resp=", resp, " err=", err)
		if err == nil {
			break
		}

		time.Sleep(time.Second)
	}

	err := pthis.executor_register()
	if err != nil {
		log.Error("executor_run: ", err)
		return err
	}

	defer pthis.executor_deregister()

	key := pthis.prefix_consumer_require_queue_count + pthis.local_consumer_id

	ch_consumer := pthis.cli.Watch(context.TODO(), key)
	for {
		select {
		case wresp := <-ch_consumer:
			for _, ev := range wresp.Events {
				log.Infof("executor_run: %s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)

				require_queue_count, err := strconv.Atoi(string(ev.Kv.Value))
				if err != nil {
					fmt.Errorf("executor_run: %v", err)
					continue
				}

				pthis.executor_check_rebalance(require_queue_count)
			}

		case <-time.Tick(time.Second):
			pthis.consumer_subscribed_update()

		case <-time.Tick(time.Second * 2): //TODO: hard code
			require_queue_count, err := pthis.consumer_require_queue_count_get()
			if err != nil {
				fmt.Errorf("executor_run: %v", err)
				continue
			}

			if len(pthis.qm.queue_list) == require_queue_count {
				pthis.executor_check_rebalance(require_queue_count)
			}
		}
	}

	return nil
}

func (pthis *QueueBalance) executor_check_rebalance(require_queue_count int) {
	sub_ls := pthis.qm.GetSubscribed()
	delta := require_queue_count - len(sub_ls)

	log.Info("executor_check_rebalance:", " delta=", delta, " require_queue_count=", require_queue_count, " sub_ls=", sub_ls)

	for i := 0; i < int(math.Abs(float64(delta))); i++ {
		var err error
		if delta > 0 {
			err = pthis.executor_subscribe()
		} else if delta < 0 {
			err = pthis.executor_unsubscribe()
		}

		if err != nil {
			log.Error("executor_check_rebalance: err=", err)
		}
	}

}

func (pthis *QueueBalance) queue_load() ([]*amqp.Queue, error) {
	log.Info("queue_load:")

	ql := []*amqp.Queue{}
	kvs, err := pthis.get_kvs(pthis.prefix_queue)
	if err != nil {
		return nil, err
	}
	for _, v := range kvs {
		qs := amqp.Queue{}
		err := json.Unmarshal(v.Value, &qs)
		if err != nil {
			log.Errorf("queue_load: %v", err)
			continue
		}

		ql = append(ql, &qs)
	}

	return ql, nil
}

func (pthis *QueueBalance) executor_subscribe() error {
	log.Info("executor_subscribe:")

	queue, err := pthis.queue_request()
	if err != nil {
		log.Error("executor_subscribe:", " queue=", queue, " err=", err)
		return err
	}

	err = pthis.qm.Subscribe(queue)
	if err == nil {
		pthis.queue_status_changed_put(queue, pthis.local_consumer_id+".subscribed")
	}

	return err
}

func (pthis *QueueBalance) executor_unsubscribe() error {
	sub_ls := pthis.qm.GetSubscribed()
	log.Info("executor_unsubscribe:", " sub_ls=", sub_ls)

	if len(sub_ls) == 0 {
		return errors.New("no queue for unsubscribe")
	}

	queue := sub_ls[0]
	err := pthis.qm.Unsubscribe(queue)
	if err == nil {
		pthis.queue_status_changed_put(queue, pthis.local_consumer_id+".unsubscribed")
	}

	return err
}

func (pthis *QueueBalance) dispatcher_run() {
	log.Info("dispatcher_run:")

	pthis.mtxDispatcher.Lock(context.TODO())
	defer pthis.mtxDispatcher.Unlock(context.TODO())

	_, err := pthis.cli.Put(context.TODO(), pthis.key_dispatcher, pthis.local_consumer_id, clientv3.WithLease(pthis.leaseID))
	if err != nil {
		log.Error("dispatcher_run:", err)
		return
	}

	log.Info("dispatcher_run: obtain dispacher lock")

	pthis.queue_update_all()
	pthis.trigger_dispatch()

	ch_queue := pthis.cli.Watch(context.TODO(), pthis.prefix_queue, clientv3.WithPrefix())
	ch_consumer_require_queue_count := pthis.cli.Watch(context.TODO(), pthis.prefix_consumer_require_queue_count, clientv3.WithPrefix())
	ch_queue_status_changed := pthis.cli.Watch(context.TODO(), pthis.prefix_queue_status_changed, clientv3.WithPrefix())
	for {
		select {
		case wresp := <-ch_queue:
			for _, ev := range wresp.Events {
				log.Infof("dispatcher_run: Watch: %s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				if ev.Type == mvccpb.PUT {
					if ev.Kv.Version == 1 {
						pthis.trigger_dispatch()
					} else {
						q := amqp.Queue{}
						err := json.Unmarshal(ev.Kv.Value, &q)
						if err != nil {
							continue
						}

						if q.Consumers != 1 {
							pthis.trigger_dispatch()
						}
					}
				} else if ev.Type == mvccpb.DELETE {
					pthis.trigger_dispatch()
				}
			}
		case wresp := <-ch_consumer_require_queue_count:
			for _, ev := range wresp.Events {
				log.Infof("dispatcher_run: Watch: %s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				if (ev.Type == mvccpb.PUT && ev.Kv.Version == 1) || ev.Type == mvccpb.DELETE {
					pthis.trigger_dispatch()
				}
			}
		case <-time.Tick(time.Second * 2):
			pthis.queue_update_all()
		case wresp := <-ch_queue_status_changed:
			for _, ev := range wresp.Events {
				log.Infof("dispatcher_run: Watch: %s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				key := string(ev.Kv.Key)
				ss := strings.Split(key, "/")
				if len(ss) < 3 || ss[3] == "" {
					log.Error("dispatcher_run: queue_update: failed. key format error")
					continue
				}
				queue := ss[3]
				err := pthis.queue_update(queue)
				if err != nil {
					log.Error("dispatcher_run: queue_update: failed. err=", err)
					continue
				}
			}
		}
	}
}

func (pthis *QueueBalance) get_kvs(prefix string) ([]*mvccpb.KeyValue, error) {
	log.Info("get_kvs:", " prefix=", prefix)

	resp, err := pthis.cli.Get(context.TODO(), prefix, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, err
	}

	return resp.Kvs, nil
}

func (pthis *QueueBalance) trigger_dispatch() error {
	log.Info("trigger_dispatch:")
	//TODO: 延迟执行
	/*
		if time.Now().Sub(pthis.last_trigger_dispatch) < time.Second*3 {
			return nil
		}
	*/

	pthis.last_trigger_dispatch = time.Now()

	queue_list, err := pthis.get_kvs(pthis.prefix_queue)
	if err != nil {
		return err
	}

	consumer_list, err := pthis.get_kvs(pthis.prefix_consumer_require_queue_count)
	if err != nil {
		return err
	}

	if len(consumer_list) == 0 || len(queue_list) == 0 {
		return errors.New("error: emptye consumer list")
	}

	avg := len(queue_list) / len(consumer_list)
	if avg == 0 {
		avg = 1
	}

	mod := len(queue_list) % len(consumer_list)
	if mod == len(queue_list) {
		mod = 0
	}

	log.Info("trigger_dispatch:", " queue_list=", len(queue_list), " consumer_list=", len(consumer_list), " avg=", avg)

	for _, kv := range consumer_list {
		require_queue_count := avg
		if mod > 0 {
			require_queue_count += 1
			mod -= 1
		}

		key := string(kv.Key)
		log.Info("trigger_dispatch:", " key=", key, " require_queue_count=", require_queue_count)

		err := pthis.consumer_require_queue_count_put(key, require_queue_count)
		if err != nil {
			log.Error("trigger_dispatch:", " key=", key, " require_queue_count=", require_queue_count, " err=", err)
			continue
		}
	}

	return nil
}

func (pthis *QueueBalance) queue_put(queue string, qs *amqp.Queue) error {
	key := pthis.prefix_queue + queue
	b, err := json.Marshal(qs)
	if err != nil {
		return err
	}

	value := string(b)

	resp, err := pthis.cli.Put(context.TODO(), key, value, clientv3.WithLease(pthis.leaseID))
	if err != nil {
		return err
	}

	log.Info("queue_put:", " key=", key, " value=", value, " resp=", resp)
	return nil
}

func (pthis *QueueBalance) queue_protected_get(queue string) (string, error) {
	key := pthis.prefix_queue_protected + queue
	log.Info("queue_protected_get:", " key=", key)

	resp, err := pthis.cli.Get(context.TODO(), key)
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", errors.New("no key=" + key)
	}

	return string(resp.Kvs[0].Value), nil
}

func (pthis *QueueBalance) queue_protected_put(queue string, value string) error {
	key := pthis.prefix_queue_protected + queue
	log.Info("queue_protected_put:", " key=", key, " value=", value)

	_, err := pthis.cli.Put(context.TODO(), key, value, clientv3.WithLease(pthis.leaseID))
	if err != nil {
		return err
	}

	return nil
}

func (pthis *QueueBalance) queue_protected_del(queue string) error {
	key := pthis.prefix_queue_protected + queue
	log.Info("queue_protected_del:", " key=", key)

	_, err := pthis.cli.Delete(context.TODO(), key)
	if err != nil {
		return err
	}

	return nil
}

func (pthis *QueueBalance) queue_request() (string, error) {
	log.Info("queue_request:")

	mp, err := pthis.queue_load()
	if err != nil {
		return "", err
	}

	for _, qs := range mp {
		log.Info("queue_request:", " qs=", struct_to_string(qs))

		if qs.Consumers == 0 {
			queue := qs.Name
			_, err := pthis.queue_protected_get(queue)
			log.Info("queue_request: err=", err)
			if err != nil {
				pthis.queue_protected_put(queue, pthis.local_consumer_id)
				return queue, nil
			}
		}
	}

	return "", errors.New("no queue rest")
}

func (pthis *QueueBalance) queue_status_changed_put(queue string, value string) error {
	key := pthis.prefix_queue_status_changed + queue
	log.Info("queue_status_changed_put:", " key=", key, " value=", value)

	_, err := pthis.cli.Put(context.TODO(), key, value, clientv3.WithLease(pthis.leaseID))
	if err != nil {
		return err
	}

	return nil
}

func (pthis *QueueBalance) queue_update(queue string) error {
	log.Info("queue_update:", " queue=", queue)

	qs, err := pthis.qm.Inspect(queue)
	if err != nil {
		return err
	}

	log.Info("queue_update:", " queue=", queue, " qs=", struct_to_string(qs))

	err = pthis.queue_put(qs.Name, &qs)
	if err != nil {
		return err
	}

	if qs.Consumers == 1 {
		pthis.queue_protected_del(queue)
	}

	return nil
}

func (pthis *QueueBalance) queue_update_all() {
	log.Info("queue_update_all:")

	for _, queue := range pthis.queue_ids {
		err := pthis.queue_update(queue)
		if err != nil {
			log.Error(err)
		}
	}
}
