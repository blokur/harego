// Package harego contains the logic for communicating with RabbitMQ.
//
// Client
//
// A Client wraps an exchange and a queue in a concurrent safe manner for
// managing all communications with RabbitMQ. If the Client doesn't have a
// queue name set, it will not create a queue. Zero value is not usable,
// therefore a Client should be constructed with NewClient() function.
//
// The only requirement for an Client to operate is a connector that returns a
// connection to the broker. There is also a helper function that can create a
// new connection from the address of the broker. The Client will create 1
// worker by default for publishing and consuming messages. The default
// exchange of the Client is "default" and it is set woth the "topic" type. The
// default delivery method is persistent. You can use provided ConfigFunc
// functions to change the Client's behaviour.
//
// You should call the Close() method when you are done with this object,
// otherwise it leaks goroutines.
//
// NewClient
//
// NewClient returns an Client instance. You can configure the object by
// providing ConfigFunc functions. See ConfigFunc documentation for more
// information.
//
//	client, err := harego.NewClient(harego.URLConnector("amqp://"),
//		harego.Workers(6),
// 	)
//	// handle error
//
// Publish
//
// Queue names are not needed for publishing messages. If the client is setup
// with multiple workers, each Publish call will go through the next available
// worker.
//
// Consume
//
// To use the Consume method, you need to provide a queue name. The Client will
// bind the queue to the exchange, then Consume calls the handler with the next
// available message on the next available worker. It stops handling messages
// when the context is done or the client is closed. Consume internally creates
// another worker for requeueing messages. By default messages are consumed
// with false autoAck.
//
// Make sure you have enough workers so the Consume is not clogged on delays.
package harego
