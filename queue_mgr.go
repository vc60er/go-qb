package qb

import (
	"encoding/json"
	log "github.com/golang/glog"
	"github.com/streadway/amqp"
)

type QueueMgr struct {
	conn       *amqp.Connection
	ch         *amqp.Channel
	pOnMsg     QueueOnMsg
	queue_list []string
}

type QueueOnMsg interface {
	OnMsg(msg []byte)
}

func NewMqMgr(url string, pOnMsg QueueOnMsg) (*QueueMgr, error) {
	mgr := QueueMgr{}

	var err error = nil
	mgr.conn, err = amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	mgr.ch, err = mgr.conn.Channel()
	if err != nil {
		mgr.conn.Close()
		return nil, err
	}

	mgr.pOnMsg = pOnMsg

	mgr.queue_list = []string{}

	return &mgr, nil
}

func (pthis *QueueMgr) Close() {
	pthis.ch.Close()
	pthis.conn.Close()
}

func (pthis *QueueMgr) Subscribe(queue string) error {
	log.Info("Subscribe:", " queue=", queue)

	consumer := queue + ".consumer"

	msgs, err := pthis.ch.Consume(
		queue,    // queue
		consumer, // consumer
		true,     // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	if err != nil {
		return err
	}

	pthis.queue_list = append(pthis.queue_list, queue) // TODO: 需要考虑并发操作
	go func() {
		defer func() {
			for i, v := range pthis.queue_list {
				if v == queue {
					pthis.queue_list = append(pthis.queue_list[:i], pthis.queue_list[i+1:]...)
				}
			}
		}()

		for d := range msgs {
			log.Infof("Received a message: %s", d.Body)
			if pthis.pOnMsg != nil {
				pthis.pOnMsg.OnMsg(d.Body)
			}
		}
	}()

	return err
}

func (pthis *QueueMgr) Unsubscribe(queue string) error {
	log.Info("Unsubscribe:", " queue=", queue)
	consumer := queue + ".consumer"

	return pthis.ch.Cancel(consumer, false)
}

func (pthis *QueueMgr) Inspect(queue string) (amqp.Queue, error) {
	return pthis.ch.QueueInspect(queue)
}

func (pthis *QueueMgr) GetSubscribed() []string {
	return pthis.queue_list
}

func struct_to_string(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
