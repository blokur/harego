// Package harego contains the logic for communicating with RabbitMQ.
/*

# Publisher

A Publisher wraps an exchange in a concurrent safe manner for publishing
messages to RabbitMQ. Zero value is not usable therefore a Publisher should be
constructed with NewPublisher() function.

The only requirement for a Publisher to operate is a connector that returns a
connection to the broker. There is also a helper function that can create a new
connection from the address of the broker. The Publisher will create 1 worker
by default for publishing messages. The default exchange of the Publisher is
"default" and it is set with the "topic" type. The default delivery method is
persistent. You can use provided ConfigFunc functions to change the Publisher's
behaviour.

You should call the Close() method when you are done with this object,
otherwise it will leak goroutines.

# NewPublisher

NewPublisher returns a Publisher instance. You can configure the object by
providing ConfigFunc functions. See ConfigFunc documentation for more
information.

	pub, err := harego.NewPublisher(harego.URLConnector("amqp://"),
		harego.Workers(6),
	)
	// handle error

# Publish

If the publisher is setup with multiple workers, each Publish call will go
through the next available worker.

# Consumer

A Consumer wraps a queue in a concurrent safe manner for receiving messages
from RabbitMQ. If the Consumer doesn't have a queue name set, it will not
create a queue. Zero value is not usable, therefore a Consumer should be
constructed with NewConsumer() function.

The only requirement for a Consumer to operate is a connector that returns a
connection to the broker. There is also a helper function that can create a new
connection from the address of the broker. The Consumer will create 1 worker by
default for consuming messages. The default delivery method is persistent. You
can use provided ConfigFunc functions to change the Consumer's behaviour.

Make sure to provide a queue name. The Consumer binds the queue to the given
exchange.

You should call the Close() method when you are done with this object,
otherwise it will leak goroutines.

# NewConsumer

NewConsumer returns an Consumer instance. You can configure the object by
providing ConfigFunc functions. See ConfigFunc documentation for more
information.

	cons, err := harego.NewConsumer(harego.URLConnector("amqp://"),
		harego.Workers(6),
	)
	// handle error

# Consume

Consume calls the handler with the next available message from the next
available worker. It stops handling messages when the context is done or the
consumer is closed. Consume internally creates a Publisher for requeueing
messages. By default messages are consumed with false autoAck.

Make sure you have enough workers so the Consume is not clogged on delays.
*/
package harego
