package chord

import (
	"fmt"
	"sync"

	"github.com/hexablock/go-chord/coordinate"
)

// Wraps vnode and object
type localRPC struct {
	vnode *Vnode
	obj   VnodeRPC
}

// LocalTransport is used to provides fast routing to Vnodes running
// locally using direct method calls. For any non-local vnodes, the
// request is passed on to another transport.
type LocalTransport struct {
	host   string
	remote Transport
	lock   sync.RWMutex
	local  map[string]*localRPC
}

// InitLocalTransport creates a local transport to wrap a remote transport
func InitLocalTransport(remote Transport) Transport {
	// Replace a nil transport with black hole
	if remote == nil {
		remote = &BlackholeTransport{}
	}

	local := make(map[string]*localRPC)
	return &LocalTransport{remote: remote, local: local}
}

// Checks for a local vnode
func (lt *LocalTransport) get(vn *Vnode) (VnodeRPC, bool) {
	key := vn.StringID()
	lt.lock.RLock()
	defer lt.lock.RUnlock()
	w, ok := lt.local[key]
	if ok {
		return w.obj, ok
	}
	return nil, ok
}

// ListVnodes requests a list of vnodes from host
func (lt *LocalTransport) ListVnodes(host string) ([]*Vnode, error) {
	// Check if this is a local host
	if host == lt.host {
		// Generate all the local clients
		res := make([]*Vnode, 0, len(lt.local))

		// Build list
		lt.lock.RLock()
		for _, v := range lt.local {
			res = append(res, v.vnode)
		}
		lt.lock.RUnlock()

		return res, nil
	}

	// Pass onto remote
	return lt.remote.ListVnodes(host)
}

// GetCoordinate gets the coordinates for a vnode
func (lt *LocalTransport) GetCoordinate(vn *Vnode) (*coordinate.Coordinate, error) {
	// Look for it locally
	obj, ok := lt.get(vn)
	// If it exists locally, handle it
	if ok {
		return obj.GetCoordinate(), nil
	}
	// Pass onto remote
	return lt.remote.GetCoordinate(vn)
}

// Ping pings a local or remote Vnode
func (lt *LocalTransport) Ping(self, target *Vnode) (bool, error) {
	// Look for it locally
	_, ok := lt.get(target)
	// If it exists locally, handle it
	if ok {
		return true, nil
	}
	// Pass onto remote
	return lt.remote.Ping(self, target)
}

// GetPredecessor calls GetPredecessor on a local or remote Vnode
func (lt *LocalTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)
	// If it exists locally, handle it
	if ok {
		return obj.GetPredecessor()
	}
	// Pass onto remote
	return lt.remote.GetPredecessor(vn)
}

// Notify calls Notify on a local or remote Vnode
func (lt *LocalTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)
	// If it exists locally, handle it
	if ok {
		return obj.Notify(self)
	}
	// Pass onto remote
	return lt.remote.Notify(vn, self)
}

// FindSuccessors calls FindSuccessors on a local or remote Vnode
func (lt *LocalTransport) FindSuccessors(vn *Vnode, n int, key []byte) ([]*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)
	// If it exists locally, handle it
	if ok {
		return obj.FindSuccessors(n, key)
	}
	// Pass onto remote
	return lt.remote.FindSuccessors(vn, n, key)
}

// ClearPredecessor calls ClearPredecessor on a local or remote Vnode
func (lt *LocalTransport) ClearPredecessor(target, self *Vnode) error {
	// Look for it locally
	obj, ok := lt.get(target)
	// If it exists locally, handle it
	if ok {
		return obj.ClearPredecessor(self)
	}
	// Pass onto remote
	return lt.remote.ClearPredecessor(target, self)
}

// SkipSuccessor calls SkipSuccessor on a local or remote Vnode
func (lt *LocalTransport) SkipSuccessor(target, self *Vnode) error {
	// Look for it locally
	obj, ok := lt.get(target)
	// If it exists locally, handle it
	if ok {
		return obj.SkipSuccessor(self)
	}
	// Pass onto remote
	return lt.remote.SkipSuccessor(target, self)
}

// Register registers a Vnode with its RPC calls
func (lt *LocalTransport) Register(v *Vnode, o VnodeRPC) {
	// Register local instance
	key := v.StringID()
	lt.lock.Lock()
	lt.host = v.Host
	lt.local[key] = &localRPC{v, o}
	lt.lock.Unlock()

	// Register with remote transport
	lt.remote.Register(v, o)
}

// Deregister de-registers a Vnode from the transport
func (lt *LocalTransport) Deregister(v *Vnode) {
	key := v.StringID()
	lt.lock.Lock()
	delete(lt.local, key)
	lt.lock.Unlock()
}

// BlackholeTransport is used to provide an implemenation of the Transport that
// does nothing. Any operation will result in an error.
type BlackholeTransport struct{}

// ListVnodes is a no-op call
func (*BlackholeTransport) ListVnodes(host string) ([]*Vnode, error) {
	return nil, fmt.Errorf("failed to connect blackhole: %s", host)
}

// GetCoordinate is a no-op call
func (*BlackholeTransport) GetCoordinate(vn *Vnode) (*coordinate.Coordinate, error) {
	return nil, fmt.Errorf("failed to connect blackhole: %s", vn.StringID())
}

// Ping is a no-op call
func (*BlackholeTransport) Ping(self, target *Vnode) (bool, error) {
	return false, nil
}

// GetPredecessor is a no-op call
func (*BlackholeTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	return nil, fmt.Errorf("failed to connect blackhole: %s", vn.StringID())
}

// Notify is a no-op call
func (*BlackholeTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	return nil, fmt.Errorf("failed to connect blackhole: %s", vn.StringID())
}

// FindSuccessors is a no-op call
func (*BlackholeTransport) FindSuccessors(vn *Vnode, n int, key []byte) ([]*Vnode, error) {
	return nil, fmt.Errorf("failed to connect blackhole: %s", vn.StringID())
}

// ClearPredecessor is a no-op call
func (*BlackholeTransport) ClearPredecessor(target, self *Vnode) error {
	return fmt.Errorf("failed to connect blackhole: %s", target.StringID())
}

// SkipSuccessor is a no-op call
func (*BlackholeTransport) SkipSuccessor(target, self *Vnode) error {
	return fmt.Errorf("failed to connect blackhole: %s", target.StringID())
}

// Register is a no-op call
func (*BlackholeTransport) Register(v *Vnode, o VnodeRPC) {
}
