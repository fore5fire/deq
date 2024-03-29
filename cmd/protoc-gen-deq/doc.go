/*
Command protoc-gen-deq is a protoc plugin that generates DEQ integration code for event types and
service definitions.

protoc-gen-deq is not normally called directly, but instead is used with the --deq_out option to
protoc:
	protoc --go_out=. --deq_out=. example/greeter/greeter.proto

Each service defined in the .proto passed to protoc will generate an event server and client, which
can communicate by publishing and subscribing to DEQ. Along with communicating via the established
rpc's in the service, the server can handle events without responding, and the client can publish
events without listening for a response, as well as read events and set up custom subscriptions.

Each message defined in the .proto will generate a DEQ event wrapper objects and event iterators to
be used by event servers and clients. It can be useful to use the DEQ protoc plugin to generate
DEQ event wrappers for other services .proto files to consume even if no services are defined by the
file itself.

The go package used to reference types generated by go_out (or equivalent ) of an imported .proto
file can be changed by passing M arguments, similar to the go protoc plugin. Multiple M arguments
can be passed. For example:
	protoc --go_out=Mdependency/dependency.proto=github.com/example/dependency:. --deq_out=dependency/dependency.proto=github.com/example/dependency:. example/import.proto

The go packaged used to reference types generated by deq_out of an imported .proto file can be
changed by passing D arguments, using the same syntax as an M argument.
	protoc --go_out=. --deq_out=Ddependency/dependency.proto=github.com/example/dependency.proto example/import.proto
*/
package documentation
