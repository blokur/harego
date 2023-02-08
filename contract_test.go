package harego_test

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/blokur/harego/v2"
)

// ExampleHandlerFunc_ack instructs the consumer to drop the message
// immediately.
func ExampleHandlerFunc_ack() {
	var fn harego.HandlerFunc = func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
		// logic for working with msg.Body goes here.
		return harego.AckTypeAck, 0
	}
	got, delay := fn(&amqp.Delivery{})
	fmt.Printf("Got %s and will delay for %s", got, delay)

	// Output:
	// Got AckTypeAck and will delay for 0s
}

// ExampleHandlerFunc_reject instructs the consumer to reject the message after
// 100ms. This will cause the consumer to sleep, therefore you need to make sure
// there are enough workers to respond to other tasks.
func ExampleHandlerFunc_reject() {
	var fn harego.HandlerFunc = func(*amqp.Delivery) (harego.AckType, time.Duration) {
		// logic for working with msg.Body goes here.
		return harego.AckTypeReject, 100 * time.Millisecond
	}
	got, delay := fn(&amqp.Delivery{})
	fmt.Printf("Got %s and will delay for %s", got, delay)

	// Output:
	// Got AckTypeReject and will delay for 100ms
}

// ExampleHandlerFunc_requeue instructs the consumer to put the messaage at the
// end of the queue after 1 second.
func ExampleHandlerFunc_requeue() {
	var fn harego.HandlerFunc = func(*amqp.Delivery) (harego.AckType, time.Duration) {
		// logic for working with msg.Body goes here.
		return harego.AckTypeRequeue, time.Second
	}
	got, delay := fn(&amqp.Delivery{})
	fmt.Printf("Got %s and will delay for %s", got, delay)

	// Output:
	// Got AckTypeRequeue and will delay for 1s
}
