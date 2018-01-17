package qb

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"testing"
)

type MyQueueOnMsg struct {
}

func (pthis *MyQueueOnMsg) OnMsg(msg []byte) {
	fmt.Println("OnMsg:", string(msg))
}

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

	fmt.Println(Struct_to_string(q))

	err = mgr.Unsubscribe(queue)
	if err != nil {
		t.Fatal(err)
	}

	q, err = mgr.Inspect(queue)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(Struct_to_string(q))
}

func Test_publish_msg(t *testing.T) {

	uri := "amqp://guest:guest@59.110.154.248:5672/"
	exchangeName := "test.direct"     // flag.String("exchange", "test-exchange", "Durable AMQP exchange name")
	exchangeType := "direct"          // flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	routingKey := "1"                 // flag.String("key", "test-key", "AMQP routing key")
	body := "body" + "." + routingKey // flag.String("body", "foobar", "Body of message")
	reliable := false                 // flag.Bool("reliable", true, "Wait for the publisher confirmation before exiting")

	if err := publish(uri, exchangeName, exchangeType, routingKey, body, reliable); err != nil {
		log.Fatalf("%s", err)
	}
	log.Printf("published %dB OK", len(body))
}

func publish(amqpURI, exchange, exchangeType, routingKey, body string, reliable bool) error {

	// This function dials, connects, declares, publishes, and tears down,
	// all in one go. In a real service, you probably want to maintain a
	// long-lived connection as state, and publish against that.

	log.Printf("dialing %q", amqpURI)
	connection, err := amqp.Dial(amqpURI)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}
	defer connection.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	log.Printf("got Channel, declaring %q Exchange (%q)", exchangeType, exchange)
	if err := channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	if reliable {
		log.Printf("enabling publishing confirms.")
		if err := channel.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmOne(confirms)
	}

	log.Printf("declared Exchange, publishing %dB body (%q)", len(body), body)
	if err = channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	return nil
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}
