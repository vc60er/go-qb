package qb

import (
	"fmt"
	"testing"
)

func Test_QumemeMgr(t *testing.T) {

	msg := &MyQueueOnMsg{}
	amqp := "amqp://guest:guest@59.110.154.248:5672/"

	mgr, err := NewMqMgr(amqp, msg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	queue := "queue.1"
	err = mgr.Subscribe(queue)
	if err != nil {
		t.Fatal(err)
	}

	q, err := mgr.Inspect(queue)
	if err != nil {
	}

	fmt.Println(struct_to_string(q))

	err = mgr.Unsubscribe(queue)
	if err != nil {
		t.Fatal(err)
	}

	q, err = mgr.Inspect(queue)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(struct_to_string(q))
}
