# Go Chord
This package provides a Golang implementation of the Chord protocol.
Chord is used to organize nodes along a ring in a consistent way. It can be
used to distribute work, build a key/value store, or serve as the underlying
organization for a ring overlay topology.

The protocol is separated from the implementation of an underlying network
transport or RPC mechanism. Instead Chord relies on a transport implementation.

## Transport Interfaces

- GRPCTransport
- UTPTransport

# Acknowledgements

The original chord implementation is based on Armon's code available
[here](http://github.com/armon/go-chord).
