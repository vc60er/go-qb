package qb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
	log "github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/streadway/amqp"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type QueueBalance struct {
	cli       *clientv3.Client
	ses       *concurrency.Session
	mtxKeeper *concurrency.Mutex

	last_trigger_dispatch time.Time

	prefix_queue_status        string
	prefix_queue_subscribed_by string

	prefix_consumer_require_queue_count string
	prefix_consumer_status              string

	key_keeper_api string
	port_keeper    int

	queue_ids []string

	local_consumer_id string
	local_ip          string

	api_update  string
	api_return  string
	api_request string

	leaseID clientv3.LeaseID

	mtx_queue_request *sync.Mutex

	qm *QueueMgr
}

func NewQueueBalance(endpoints []string, amqp string, pOnMsg QueueOnMsg, queue_ids []string, ip string, port int) (*QueueBalance, error) {
	//TODO 参数检查

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

	pthis.mtx_queue_request = &sync.Mutex{}

	pthis.mtxKeeper = concurrency.NewMutex(pthis.ses, "/qb/lock/keeper") //TODO: 可以更优化

	pthis.prefix_consumer_status = "/qb/consumer_status/"
	pthis.prefix_consumer_require_queue_count = "/qb/consumer_require_queue_count/"
	pthis.prefix_queue_status = "/qb/queue_status/"
	pthis.prefix_queue_subscribed_by = "/qb/queue_subscribed_by/"
	pthis.key_keeper_api = "/qb/keeper_api"

	pthis.api_request = "/api/v1/queue/request"
	pthis.api_return = "/api/v1/queue/return"
	pthis.api_update = "/api/v1/queue/update"

	pthis.local_ip = ip      // "127.0.0.1"
	pthis.port_keeper = port // 8800
	pthis.queue_ids = queue_ids

	pthis.leaseID = pthis.ses.Lease()

	pthis.local_consumer_id = fmt.Sprintf("consumer_%s_%d", pthis.local_ip, pthis.leaseID)

	log.Info("NewQueueBalance:",
		" leaseID=", pthis.leaseID,
		" queue_ids=", pthis.queue_ids,
		" local_ip=", pthis.local_ip,
		" port_keeper=", pthis.port_keeper)

	return &pthis, nil
}

func (pthis *QueueBalance) Close() {
	pthis.ses.Close()
	pthis.cli.Close()
}

func (pthis *QueueBalance) Run() {
	go pthis.keeper_run()
	go pthis.consumer_run()
}

func (pthis *QueueBalance) keeper_queue_put(queue string, qs *amqp.Queue) error {
	key := pthis.prefix_queue_status + queue
	b, err := json.Marshal(qs)
	if err != nil {
		return err
	}

	value := string(b)

	resp, err := pthis.cli.Put(context.TODO(), key, value, clientv3.WithLease(pthis.leaseID))
	if err != nil {
		return err
	}

	log.V(10).Info("keeper_queue_put:", " key=", key, " value=", value, " resp=", resp)
	return nil
}

func (pthis *QueueBalance) keeper_queue_status_update(queue string) error {
	log.V(10).Info("keeper_queue_status_update:", " queue=", queue)

	qs, err := pthis.qm.Inspect(queue)
	if err != nil {
		return err
	}

	log.V(10).Info("keeper_queue_status_update:", " queue=", queue, " qs=", Struct_to_string(qs))

	err = pthis.keeper_queue_put(qs.Name, &qs)
	if err != nil {
		return err
	}

	return nil
}

func (pthis *QueueBalance) keeper_queue_status_update_all() {
	log.V(10).Info("keeper_queue_status_update_all:")

	for _, queue := range pthis.queue_ids {
		err := pthis.keeper_queue_status_update(queue)
		if err != nil {
			log.Error(err)
		}
	}
}

func (pthis *QueueBalance) consumer_register() error {
	log.Info("executor_register:")
	return pthis.consumer_require_queue_count_put("", 0)
}

func (pthis *QueueBalance) consumer_deregister() error {
	log.Info("consumer_deregister")
	return pthis.consumer_require_queue_count_del()
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
		return 0, errors.New("consumer_require_queue_count_get: no value")
	}

	return strconv.Atoi(string(resp.Kvs[0].Value))
}

func (pthis *QueueBalance) consumer_status_update() error {
	key := pthis.prefix_consumer_status + pthis.local_consumer_id

	value := strings.Join(pthis.qm.GetSubscribed(), ",")

	log.V(10).Info("consumer_status_update:", " key=", key, " value=", value)
	_, err := pthis.cli.Put(context.TODO(), key, value, clientv3.WithLease(pthis.leaseID))
	if err != nil {
		log.Error("consumer_status_update:", " key=", key, " value=", value, " err=", err)
		return err
	}

	return nil
}

func (pthis *QueueBalance) consumer_run() error {
	log.Info("consumer_run:")

	for {
		// 等待dispathcer运行直至 // TODO 可以优化
		resp, err := pthis.cli.Get(context.TODO(), pthis.key_keeper_api)
		log.Info("consumer_run:", " resp=", resp, " err=", err)
		if err == nil {
			break
		}

		time.Sleep(time.Second)
	}

	err := pthis.consumer_register()
	if err != nil {
		log.Error("consumer_run: ", err)
		return err
	}

	defer pthis.consumer_deregister()

	key := pthis.prefix_consumer_require_queue_count + pthis.local_consumer_id

	ch_consumer_require_queue_count := pthis.cli.Watch(context.TODO(), key)

	for {
		select {
		case <-ch_consumer_require_queue_count:
			pthis.consumer_check_rebalance()

		case <-time.Tick(time.Second): // 为了测试方面，向etcd写入consmer状态信息
			pthis.consumer_status_update()

		case <-time.Tick(time.Second * 2): //TODO: hard code 定期检查
			pthis.consumer_check_rebalance()
		}
	}

	return nil
}

func (pthis *QueueBalance) consumer_check_rebalance() error {
	log.Info("consumer_check_rebalance:")

	require_queue_count, err := pthis.consumer_require_queue_count_get()
	if err != nil {
		fmt.Errorf("consumer_check_rebalance: pthis.consumer_require_queue_count_get %v", err)
		return err
	}

	sub_ls := pthis.qm.GetSubscribed()
	delta := require_queue_count - len(sub_ls)

	log.Info("consumer_check_rebalance:", " delta=", delta, " require_queue_count=", require_queue_count, " sub_ls=", sub_ls)

	for i := 0; i < int(math.Abs(float64(delta))); i++ {
		var err error
		if delta > 0 {
			err = pthis.consumer_subscribe()
		} else if delta < 0 {
			err = pthis.consumer_unsubscribe()
		}

		if err != nil {
			log.Error("consumer_check_rebalance: err=", err)
		}
	}

	return nil
}

func (pthis *QueueBalance) consumer_subscribe() error {
	log.Info("consumer_subscribe:")

	queue, err := pthis.consumer_queue_request()
	if err != nil {
		log.Error("consumer_subscribe:", " queue=", queue, " err=", err)
		return err
	}

	err = pthis.qm.Subscribe(queue)
	if err != nil {
		log.Error("consumer_subscribe:", " queue=", queue, " qm.Subscribe err=", err)
		err := pthis.consumer_queue_return(queue)
		if err != nil {
			log.Error("consumer_subscribe:", " queue=", queue, " consumer_queue_return err=", err)
		}
	} else {
		pthis.consumer_queue_update(queue)
	}

	return err
}

func (pthis *QueueBalance) consumer_unsubscribe() error {
	sub_ls := pthis.qm.GetSubscribed()
	log.Info("consumer_unsubscribe:", " sub_ls=", sub_ls)

	if len(sub_ls) == 0 {
		return errors.New("consumer_unsubscribe: no queue for unsubscribe")
	}

	queue := sub_ls[0]
	err := pthis.qm.Unsubscribe(queue)
	if err != nil {
		log.Error("consumer_unsubscribe:", " queue=", queue, " err=", err)
	} else {
		pthis.consumer_queue_return(queue)
	}

	return err
}

func (pthis *QueueBalance) consumer_keeper_api_base_url() (string, error) {
	resp, err := pthis.cli.Get(context.TODO(), pthis.key_keeper_api)
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", errors.New("key_keeper_api empty")
	}

	url := string(resp.Kvs[0].Value)

	return url, nil
}

func (pthis *QueueBalance) consumer_queue_return(queue string) error {
	log.Info("consumer_queue_return:", " queue=", queue)

	url_base, err := pthis.consumer_keeper_api_base_url()
	if err != nil {
		return err
	}

	url := url_base + pthis.api_return

	data := map[string]string{"queue": queue}

	b, _ := json.Marshal(data)
	resp, err := http.Post(url, "", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	res := ApiResponse{}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&res)
	if err != nil {
		return err
	}

	if res.Errno != 0 {
		return errors.New(fmt.Sprint("url=%, errno=%d", url, res.Errno))
	}

	return nil
}

func (pthis *QueueBalance) consumer_queue_update(queue string) error {
	log.Info("consumer_queue_update:", " queue=", queue)

	url_base, err := pthis.consumer_keeper_api_base_url()
	if err != nil {
		return err
	}

	url := url_base + pthis.api_update

	data := map[string]string{"queue": queue}

	b, _ := json.Marshal(data)
	resp, err := http.Post(url, "", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	res := ApiResponse{}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&res)
	if err != nil {
		return err
	}

	if res.Errno != 0 {
		return errors.New(fmt.Sprint("url=%, errno=%d", url, res.Errno))
	}

	return nil
}

func (pthis *QueueBalance) consumer_queue_request() (string, error) {
	log.Info("consumer_queue_request:")

	url_base, err := pthis.consumer_keeper_api_base_url()
	if err != nil {
		return "", err
	}

	url := url_base + pthis.api_request + "?" + "consumer=" + pthis.local_consumer_id
	log.Info("consumer_queue_request:", " url=", url)

	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	res := ApiResponse{}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&res)
	if err != nil {
		return "", err
	}

	if res.Errno != 0 {
		return "", errors.New(fmt.Sprintf("errno=%d, errmsg=%s", res.Errno, res.Errmsg))
	}

	queue, ok := res.Data["queue"]
	if !ok {
		return "", errors.New("no queue item in data")
	}

	return queue.(string), nil
}

func (pthis *QueueBalance) keeper_run() {
	log.Info("keeper_run:")

	pthis.mtxKeeper.Lock(context.TODO())
	defer pthis.mtxKeeper.Unlock(context.TODO())

	go pthis.keeper_run_api()

	log.Info("dispatcher_run: obtain dispacher lock")

	pthis.keeper_queue_status_update_all()
	pthis.keeper_trigger()

	ch_queue_status := pthis.cli.Watch(context.TODO(), pthis.prefix_queue_status, clientv3.WithPrefix())

	ch_consumer_require_queue_count := pthis.cli.Watch(context.TODO(),
		pthis.prefix_consumer_require_queue_count, clientv3.WithPrefix())

	for {
		select {
		case wresp := <-ch_queue_status:
			pthis.keeper_check_trigger(wresp.Events)

		case wresp := <-ch_consumer_require_queue_count:
			pthis.keeper_check_trigger(wresp.Events)

		case <-time.Tick(time.Second * 2): //TODO // 定时同步mq中的queueStatus
			pthis.keeper_queue_status_update_all()
		}
	}
}

type ApiResponse struct {
	Errno  int                    `json:"errno"`
	Errmsg string                 `json:"errmsg"`
	Data   map[string]interface{} `json:"data"`
}

const (
	ERR_SUCC  int = 0
	ERR_PARAM int = 10000 + iota
	ERR_INTERNAL
)

func write_response(w http.ResponseWriter, errno int, errmsg string, data map[string]interface{}) {
	res := ApiResponse{}
	res.Errno = errno
	res.Errmsg = errmsg
	res.Data = data

	if res.Data == nil {
		res.Data = make(map[string]interface{})
	}

	bts, _ := json.Marshal(&res)

	log.Info("write_response:", string(bts))

	w.Write(bts)
}

// TODO: 缺少错误处理
func (pthis *QueueBalance) keeper_run_api() {
	url_base := fmt.Sprintf("http://%s:%d", pthis.local_ip, pthis.port_keeper)
	addr := fmt.Sprintf("%s:%d", pthis.local_ip, pthis.port_keeper)

	log.Info("keeper_run_api:", " url_base=", url_base, " addr=", addr)

	_, err := pthis.cli.Put(context.TODO(), pthis.key_keeper_api, url_base, clientv3.WithLease(pthis.leaseID))
	if err != nil {
		log.Fatal(err)
	}

	r := mux.NewRouter()
	r.HandleFunc(pthis.api_request, pthis.handle_queue_request).Methods("GET") //.Queries("consumer", "{consumer}") //TODO
	r.HandleFunc(pthis.api_return, pthis.handle_queue_return).Methods("POST")
	r.HandleFunc(pthis.api_update, pthis.handle_queue_update).Methods("POST")

	log.Fatal(http.ListenAndServe(addr, r))
}

func (pthis *QueueBalance) handle_queue_request(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	consumer := req.FormValue("consumer")

	log.Info("handle_queue_request:", " consumer=", consumer)

	queue, err := pthis.keeper_queue_subscribed_request(consumer)
	if err != nil {
		write_response(w, ERR_INTERNAL, fmt.Sprint("keeper_queue_subscribed_request err=", err), nil)
		return
	}

	data := make(map[string]interface{})
	data["queue"] = queue
	write_response(w, ERR_SUCC, "", data)
}

func (pthis *QueueBalance) handle_queue_return(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	log.Info("handle_queue_return:", " body=", string(body))

	data := make(map[string]string)
	err := json.Unmarshal(body, &data)
	if err != nil {
		write_response(w, ERR_PARAM, fmt.Sprint(err), nil)
		return
	}

	queue := data["queue"]
	if len(queue) == 0 {
		write_response(w, ERR_PARAM, "no queue", nil)
		return
	}

	err = pthis.keeper_queue_subscribed_del(queue)
	if err != nil {
		write_response(w, ERR_INTERNAL, fmt.Sprint(err), nil)
		return
	}

	pthis.keeper_queue_status_update(queue)

	write_response(w, ERR_SUCC, "", nil)
}

func (pthis *QueueBalance) handle_queue_update(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	log.Info("handle_queue_update:", " body=", string(body))

	data := make(map[string]string)
	err := json.Unmarshal(body, &data)
	if err != nil {
		write_response(w, ERR_PARAM, fmt.Sprint(err), nil)
		return
	}

	queue := data["queue"]
	if len(queue) == 0 {
		write_response(w, ERR_PARAM, "no queue", nil)
		return
	}

	pthis.keeper_queue_status_update(queue)

	write_response(w, ERR_SUCC, "", nil)
}

func (pthis *QueueBalance) keeper_check_trigger(evs []*clientv3.Event) {
	trigger := false
	for _, ev := range evs {
		log.V(10).Infof("keeper_check_trigger: %s key=%q, value=%q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)

		if strings.Contains(string(ev.Kv.Key), pthis.prefix_queue_status) {
			if ev.Type == mvccpb.PUT {
				if ev.Kv.Version == 1 {
					trigger = true
					break
				} else {
					q := amqp.Queue{}
					err := json.Unmarshal(ev.Kv.Value, &q)
					if err != nil {
						continue
					}

					if q.Consumers != 1 {
						trigger = true
						break
					}
				}
			} else if ev.Type == mvccpb.DELETE {
				trigger = true
				break
			}
		} else if strings.Contains(string(ev.Kv.Key), pthis.prefix_consumer_require_queue_count) {
			if ev.Type == mvccpb.PUT && ev.Kv.Version == 1 {
				trigger = true
				break
			} else if ev.Type == mvccpb.DELETE {
				trigger = true

				// TODO: 结构需要优化
				key := string(ev.Kv.Key)
				ss := strings.Split(key, "/")
				if len(ss) < 3 || len(ss[3]) == 0 {
					//TODO: need logs
					break
				}
				consumer := ss[3]

				queues, err := pthis.keeper_queue_subscribed_find(consumer)
				if err != nil {
					//TODO: need logs
					break
				}

				for _, queue := range queues {
					pthis.keeper_queue_subscribed_del(queue)
				}

				break
			}
		}
	}

	if trigger {
		pthis.keeper_trigger()
	}
}

func (pthis *QueueBalance) get_kvs(prefix string) ([]*mvccpb.KeyValue, error) {
	log.V(10).Info("get_kvs:", " prefix=", prefix)

	resp, err := pthis.cli.Get(context.TODO(), prefix, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, err
	}

	return resp.Kvs, nil
}

func (pthis *QueueBalance) keeper_trigger() error {
	log.Info("keeper_trigger:")
	//TODO: 延迟执行
	/*
		if time.Now().Sub(pthis.last_trigger_dispatch) < time.Second*3 {
			return nil
		}
	*/

	pthis.last_trigger_dispatch = time.Now()

	queue_list, err := pthis.get_kvs(pthis.prefix_queue_status)
	if err != nil {
		return err
	}

	consumer_list, err := pthis.get_kvs(pthis.prefix_consumer_require_queue_count)
	if err != nil {
		return err
	}

	if len(consumer_list) == 0 || len(queue_list) == 0 {
		return errors.New("keeper_trigger: emptye consumer list")
	}

	avg := len(queue_list) / len(consumer_list)
	mod := len(queue_list) % len(consumer_list)

	for i, kv := range consumer_list {
		require_queue_count := avg
		if i < mod {
			require_queue_count++
		}

		key := string(kv.Key)
		log.Info("keeper_trigger:", " key=", key, " require_queue_count=", require_queue_count)

		err := pthis.consumer_require_queue_count_put(key, require_queue_count)
		if err != nil {
			log.Error("keeper_trigger:", " key=", key, " require_queue_count=", require_queue_count, " err=", err)
			continue
		}
	}

	return nil
}

func (pthis *QueueBalance) keeper_queue_load() ([]*amqp.Queue, error) {
	log.Info("keeper_queue_load:")

	ql := []*amqp.Queue{}
	kvs, err := pthis.get_kvs(pthis.prefix_queue_status)
	if err != nil {
		return nil, err
	}
	for _, v := range kvs {
		qs := amqp.Queue{}
		err := json.Unmarshal(v.Value, &qs)
		if err != nil {
			log.Errorf("keeper_queue_load: %v", err)
			continue
		}

		ql = append(ql, &qs)
	}

	return ql, nil
}

func (pthis *QueueBalance) keeper_queue_subscribed_request(consumer string) (string, error) {
	log.Info("keeper_queue_subscribed_request:", " consumer=", consumer)

	pthis.mtx_queue_request.Lock()
	defer pthis.mtx_queue_request.Unlock()

	mp, err := pthis.keeper_queue_load()
	if err != nil {
		return "", err
	}

	for _, qs := range mp {
		log.V(10).Info("keeper_queue_subscribed_request:", " qs=", Struct_to_string(qs))

		if qs.Consumers == 0 {
			queue := qs.Name

			qu, err := pthis.keeper_queue_subscribed_get(queue)
			if err != nil {
				return "", err
			}

			if len(qu) > 0 {
				continue
			}

			err = pthis.keeper_queue_subscribed_put(queue, consumer)
			if err != nil {
				return "", err
			}

			return queue, nil
		}
	}

	return "", errors.New("keeper_queue_subscribed_request: no queue rest")
}

func (pthis *QueueBalance) keeper_queue_subscribed_put(queue string, value string) error {
	log.Info("keeper_queue_subscribed_put:", " queue=", queue, " value=", value)

	key := pthis.prefix_queue_subscribed_by + queue

	_, err := pthis.cli.Put(context.TODO(), key, value, clientv3.WithLease(pthis.leaseID))
	if err != nil {
		return err
	}

	return nil
}

func (pthis *QueueBalance) keeper_queue_subscribed_get(queue string) (string, error) {
	log.Info("keeper_queue_subscribed_get:", " queue=", queue)

	key := pthis.prefix_queue_subscribed_by + queue

	resp, err := pthis.cli.Get(context.TODO(), key)
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", nil
	}

	return string(resp.Kvs[0].Value), nil
}

func (pthis *QueueBalance) keeper_queue_subscribed_del(queue string) error {
	log.Info("keeper_queue_subscribed_del:", " queue=", queue)

	key := pthis.prefix_queue_subscribed_by + queue

	_, err := pthis.cli.Delete(context.TODO(), key)
	if err != nil {
		return err
	}

	return nil
}

func (pthis *QueueBalance) keeper_queue_subscribed_find(consumer string) ([]string, error) {
	log.Info("keeper_queue_subscribed_find:", " consumer=", consumer)

	kvs, err := pthis.get_kvs(pthis.prefix_queue_subscribed_by)
	if err != nil {
		return nil, err
	}

	queues := []string{}
	for _, kv := range kvs {
		value := string(kv.Value)
		if value == consumer {
			key := string(kv.Key)

			ss := strings.Split(key, "/")
			if len(ss) < 3 || len(ss[3]) == 0 {
				//TODO: need logs
				continue
			}
			queue := ss[3]

			queues = append(queues, queue)
		}
	}

	return queues, nil
}
