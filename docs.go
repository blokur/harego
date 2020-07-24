// Package harego contains the logic for communicating with RabbitMQ.
//
// Exchange
//
// Exchange creates concurrent safe exchange and a queue for managing all
// communications with RabbitMQ. You should call the Close() method when you are
// done with this object, otherwise you will leak goroutines. An Exchange that
// has been used for publishing can not be used for consuming. Zero value is not
// usable.
//
// The only requirement for an Exchange to operate is an *amqp.Connection. The
// Exchange will create 1 worker by default for consuming messages. The default
// Exchange is "default" and it is set up on the "topic". The default delivery
// method is persistent. You can use provided ConfigFunc functions to change the
// Exchange's behaviour.
//
// NewExchange
//
// NewExchange returns an Exchange instance. You can configure the object with
// passing provided ConfigFunc functions.
//	r, err = amqp.Dial("amqp://")
//	// handle error
//
//	e, err := harego.NewExchange(r)
//	// handle error
//
// Publish
//
// Publish sends the msg to the queue. If the connection is not ready, it will
// retry until it's successful.
//
// Consume
//
// Consume calls the handler on each message and stops handling messages when
// the context is done. See HandlerFunc documentation for more information.
// Consume internally creates another worker for requeueing messages. By default
// messages are consumed with false autoAck.
//
// Make sure you have enough workers so the Consume is not clogged on delays.
//
//
// Close
//
// Close closes the channel and the connection. The Exchange is useless after
// close.
package harego
