# Go Chord
This package provides a Golang implementation of the Chord protocol.
Chord is used to organize nodes along a ring in a consistent way. It can be
used to distribute work, build a key/value store, or serve as the underlying
organization for a ring overlay topology.

The protocol is separated from the implementation of an underlying network
transport or RPC mechanism. Instead Chord relies on a transport implementation.
A GRPCTransport implementation as been provided.

# Acknowledgements
Code has been used from the following libraries:

- https://github.com/armon/go-chord
- https://github.com/hashicorp/serf/coordinate
