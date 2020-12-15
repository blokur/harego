// Package harego contains the logic for communicating with RabbitMQ.
//
// Client
//
// A Client wraps an exchange and a queue in a concurrent safe manner for
// managing all communications with RabbitMQ. An Client that has been used for
// publishing can not be used for consuming. Zero value is not usable.
//
// The only requirement for an Client to operate is the address to the broker.
// The Client will create 1 worker by default for consuming messages. The
// default Client is "default" and it is set up on the "topic". The default
// delivery method is persistent. You can use provided ConfigFunc functions to
// change the Client's behaviour.
//
// You should call the Close() method when you are done with this object,
// otherwise you will leak goroutines.
//
// NewClient
//
// NewClient returns an Client instance. You can configure the object with
// passing provided ConfigFunc functions. See HandlerFunc documentation for
// more information.
//
//	e, err := harego.NewClient("amqp://", harego.Workers(6))
//	// handle error
//
// Publish
//
// Publish sends the msg to the queue. If the connection is not ready, it will
// retry until it's successful.
//
// Consume
//
// Consume calls the handler with the next available message on the next
// available worker, and stops handling messages when the context is done or
// the client is closed. Consume internally creates another worker for
// requeueing messages. By default messages are consumed with false autoAck.
//
// Make sure you have enough workers so the Consume is not clogged on delays.
//
// Close
//
// Close closes the channel and the connection. The Client is useless after
// close.
package harego
