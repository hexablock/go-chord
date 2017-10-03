//
// Package chord is used to provide an implementation of the Chord network protocol.
//
package chord

import (
	"crypto/sha1"
	"fmt"
	"hash"
	"sync"
	"time"

	"github.com/hexablock/go-chord/coordinate"
)

// Transport implements the methods needed for a Chord ring
type Transport interface {
	// Gets a list of the vnodes on the box
	ListVnodes(string) ([]*Vnode, error)

	// Get the coordinates for a vnode
	GetCoordinate(vn *Vnode) (*coordinate.Coordinate, error)

	// Ping a Vnode, return liveness and/or coordinates
	Ping(self, target *Vnode) (bool, *coordinate.Coordinate, error)

	// Request a nodes predecessor
	GetPredecessor(*Vnode) (*Vnode, error)

	// Notify our successor of ourselves
	Notify(target, self *Vnode) ([]*Vnode, error)

	// Find a successor
	FindSuccessors(*Vnode, int, []byte) ([]*Vnode, error)

	// Clears a predecessor if it matches a given vnode. Used to leave.
	ClearPredecessor(target, self *Vnode) error

	// Instructs a node to skip a given successor. Used to leave.
	SkipSuccessor(target, self *Vnode) error

	// Register for an RPC callbacks
	Register(*Vnode, VnodeRPC)
}

// VnodeRPC contains methods to invoke on the registered vnodes
type VnodeRPC interface {
	GetPredecessor() (*Vnode, error)
	Notify(*Vnode) ([]*Vnode, error)
	FindSuccessors(int, []byte) ([]*Vnode, error)
	ClearPredecessor(*Vnode) error
	SkipSuccessor(*Vnode) error
	GetCoordinate() *coordinate.Coordinate
	UpdateCoordinate(remote *Vnode, rtt time.Duration) (*coordinate.Coordinate, error)
}

// Delegate to notify on ring events
type Delegate interface {
	NewPredecessor(local, remoteNew, remotePrev *Vnode)
	Leaving(local, pred, succ *Vnode)
	PredecessorLeaving(local, remote *Vnode)
	SuccessorLeaving(local, remote *Vnode)
	Shutdown()
}

// Config for Chord nodes
type Config struct {
	Hostname string // Local host name

	Region string // General region
	Zone   string // Zone within a region
	Sector string // Sector within a zone
	Meta   Meta   // User defined metadata

	NumVnodes     int // Number of vnodes per physical node
	NumSuccessors int // Number of successors to maintain

	HashFunc func() hash.Hash `json:"-"` // Hash function to use

	StabilizeMin time.Duration // Minimum stabilization time
	StabilizeMax time.Duration // Maximum stabilization time
	// Setting this to anything greater than 0 enables adaptive stabilization.
	// If eneabled the above min and max are used as starting values.
	StabilizeThresh time.Duration
	// Number of interations to stay at start before stepping
	StabilizeStayCount int

	Coordinate        *coordinate.Config // vivaldi coordinate configuration
	Delegate          Delegate           `json:"-"` // Invoked to handle ring events
	DelegateQueueSize int                // Number of delegate calls to hold in the queue

	hashBits int // Bit size of the hash function used by finger table
}

// Represents a local Vnode
type localVnode struct {
	Vnode
	ring *Ring

	// Successor list and its lock
	succLock   sync.RWMutex
	successors []*Vnode

	// Finger table and its lock
	fingLock   sync.RWMutex
	finger     []*Vnode
	lastFinger int

	// Predecessor and its lock
	predLock    sync.RWMutex
	predecessor *Vnode

	timeLock   sync.RWMutex
	stabilized time.Time   // Last stabilized time
	timer      *time.Timer // stabilization timer
}

// Ring stores the state required for a Chord ring
type Ring struct {
	config      *Config
	transport   Transport // Transport to handle local and remote efficiently
	vnodes      []*localVnode
	delegateCh  chan func()        // channel for delegate callbacks
	coordClient *coordinate.Client // vivaldi coordinate client
	stab        *adaptiveStabilize // adaptive stabilizer
	shutdown    chan bool          // channel to wait for vnodes to shutdown
	sigshut     int32              // signal shutdown
}

// HashBits returns the number of hash bits
func (config *Config) HashBits() int {
	return config.hashBits
}

// DefaultConfig returns the default Ring configuration.  It uses SHA1 as the
// default hash function
func DefaultConfig(hostname string) *Config {
	return &Config{
		Hostname:           hostname,
		Region:             "region",
		Zone:               "zone",
		Sector:             "sector",
		Meta:               make(Meta),
		NumVnodes:          8,
		HashFunc:           sha1.New,
		StabilizeMin:       time.Duration(15 * time.Second),
		StabilizeMax:       time.Duration(45 * time.Second),
		StabilizeThresh:    0,
		StabilizeStayCount: 20,
		NumSuccessors:      8,
		Coordinate:         coordinate.DefaultConfig(),
		Delegate:           nil,
		DelegateQueueSize:  32,
		hashBits:           160, // 160bit hash function for sha1
	}
}

// Create a new Chord ring given the config and transport
func Create(conf *Config, trans Transport) (*Ring, error) {
	ring, err := initializeRing(conf, trans)
	if err != nil {
		return nil, err
	}

	ring.setLocalSuccessors()
	ring.schedule()

	return ring, nil
}

// Join an existing Chord ring
func Join(conf *Config, trans Transport, existing string) (*Ring, error) {
	// Request a list of Vnodes from the remote host
	hosts, err := trans.ListVnodes(existing)
	if err != nil {
		return nil, err
	}
	if hosts == nil || len(hosts) == 0 {
		return nil, fmt.Errorf("remote host has no vnodes")
	}
	// Initialize the ring
	ring, err := initializeRing(conf, trans)
	if err != nil {
		return nil, err
	}

	// Acquire a live successor for each Vnode
	for _, vn := range ring.vnodes {
		// Get the nearest remote vnode
		nearest := nearestVnodeToKey(hosts, vn.Id)

		// Query for a list of successors to this Vnode
		succs, err := trans.FindSuccessors(nearest, conf.NumSuccessors, vn.Id)
		if err != nil {
			//return nil, fmt.Errorf("Failed to find successor for vnodes! Got %s", err)
			return nil, err
		}
		if succs == nil || len(succs) == 0 {
			return nil, fmt.Errorf("successor vnodes not found")
		}

		// Assign the successors
		for idx, s := range succs {
			vn.successors[idx] = s
		}
	}

	// Start delegate handler
	if ring.config.Delegate != nil {
		go ring.delegateHandler()
	}

	// Do a fast stabilization, and schedule regular execution
	for _, vn := range ring.vnodes {
		vn.stabilize()
	}

	//ring.schedule()

	return ring, nil
}

// Leave a given Chord ring and shuts down the local vnodes
func (r *Ring) Leave() error {
	// Shutdown the vnodes first to avoid further stabilization runs
	r.stopVnodes()

	// Instruct each vnode to leave
	var err error
	for _, vn := range r.vnodes {
		err = mergeErrors(err, vn.leave())
	}

	// Wait for the delegate callbacks to complete
	r.stopDelegate()
	return err
}

// Shutdown shuts down the local processes in a given Chord ring
// Blocks until all the vnodes terminate.
func (r *Ring) Shutdown() {
	// Stop vnodes first
	r.stopVnodes()
	r.stopDelegate()
}

// LookupCoordinate returns the coordinate for the given vnode
func (r *Ring) LookupCoordinate(vn *Vnode) (*coordinate.Coordinate, error) {
	return r.transport.GetCoordinate(vn)
}

// LookupHash does a lookup for up to N successors of a hash.  It returns the
// predecessor and up to N successors. The hash size must match the hash function
// used when init'ing the ring.
func (r *Ring) LookupHash(n int, hash []byte) ([]*Vnode, error) {
	// Ensure that n is sane
	if n > r.config.NumSuccessors {
		return nil, fmt.Errorf("cannot ask for more successors than NumSuccessors")
	}

	// Ensure hash size matches what is configured
	l, cl := len(hash), r.config.HashFunc().Size()
	if l != cl {
		return nil, fmt.Errorf("invalid hash size")
	}

	// Find the nearest local vnode
	nearest := r.nearestVnode(hash)
	// Use the nearest node for the lookup
	successors, err := nearest.FindSuccessors(n, hash)
	if err != nil {
		return nil, err
	}

	// Trim the nil successors
	for successors[len(successors)-1] == nil {
		successors = successors[:len(successors)-1]
	}
	return successors, nil
}

// Lookup does a lookup for up to N successors on the hash of a key.  It returns the hash of the key used to
// perform the lookup, the closest vnode and up to N successors.
func (r *Ring) Lookup(n int, key []byte) ([]byte, []*Vnode, error) {
	// Hash the key
	h := r.config.HashFunc()
	h.Write(key)
	kh := h.Sum(nil)

	succs, err := r.LookupHash(n, kh)
	return kh, succs, err
}
