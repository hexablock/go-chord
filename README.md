# Go Chord
This package provides a Golang implementation of the Chord protocol.
Chord is used to organize nodes along a ring in a consistent way. It can be
used to distribute work, build a key/value store, or serve as the underlying
organization for a ring overlay topology.

The protocol is separated from the implementation of an underlying network
transport or RPC mechanism. Instead Chord relies on a transport implementation.
A GRPCTransport has been provided.  A tcp based transport can be found in the
original repo [armon/go-chord](http://github.com/armon/go-chord).

#### GRPCTransport
The GRPCTransport uses gRPC and protocol buffers to perform RPC operations.  This
allows to register multiple services against the same gRPC server.  Other projects
can implement their own services and register them against the same server allowing
multiple services to run on the same listener.

# Development
When changes need to be made to the protocol - add the appropriate code to the
net.proto file, and re-generate the code using:

	make protoc

To import protobuf structures in other projects using protofbufs, add the following
import to the protofile:

	import "github.com/euforia/go-chord/net.proto"

To generate the code (assuming both projects are in the same $GOPATH) add the
following include:

	 -I ../../../

##### Example
If you had a `rpc.proto` file in a `./rpc` directory which uses this library, the
command would look as follows:

	protoc -I ../../../ -I ./rpc ./rpc/rpc.proto --go_out=plugins=grpc:rpc

#### Requirements

- Go > 1.6.3
- protoc
- grpc

# Acknowledgements

The original chord implementation is based on Armon's code available
[here](http://github.com/armon/go-chord).
