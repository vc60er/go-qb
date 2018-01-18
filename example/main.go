package main

import (
	"flag"
	log "github.com/golang/glog"
	"github.com/vc60er/go-qb"
	"os"
	"os/signal"
)

type MyQueueOnMsg struct {
}

func (pthis *MyQueueOnMsg) OnMsg(msg []byte) {
	log.Info("OnMsg:", string(msg))
}

func main() {
	flag.Parse()

	msg := &MyQueueOnMsg{}
	endpoints := []string{"http://127.0.0.1:2379", "http://127.0.0.1:22379", "http://127.0.0.1:32379"}
	amqp := "amqp://guest:guest@59.110.154.248:5672/"
	queue_ids := []string{"queue.1", "queue.2", "queue.3", "queue.4", "queue.5", "queue.6", "queue.7", "queue.8"}
	ip := "127.0.0.1"
	port := 8800

	qb, err := qb.NewQueueBalance(endpoints, amqp, msg, queue_ids, ip, port)
	failOnError(err, "NewQueueBalance")

	defer qb.Close()

	qb.Run()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	s := <-c
	log.Info(s)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
