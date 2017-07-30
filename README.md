# Go Chord [(https://travis-ci.org/hexablock/go-chord.svg?branch=master)](https://travis-ci.org/hexablock/go-chord)

This package provides a Golang implementation of the Chord protocol.
Chord is used to organize nodes along a ring in a consistent way. It can be
used to distribute work, build a key/value store, or serve as the underlying
organization for a ring overlay topology.

The protocol is separated from the implementation of an underlying network
transport or RPC mechanism. Instead Chord relies on a transport implementation.
A GRPCTransport implementation as been provided.

## Additions
The following features have been added on top of the standard Chord protocol:

- Vivaldi coordinate tracking to measure inter-vnode latency
- Additional binary metadata field for each vnode to allow a user application to define
custom properties

### Acknowledgements
Code has been used from the following libraries:

- https://github.com/armon/go-chord
- https://github.com/hashicorp/serf/coordinate
