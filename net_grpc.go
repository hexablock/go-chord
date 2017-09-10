package chord

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hexablock/go-chord/coordinate"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	errTimedOut = errors.New("timed out")
)

type rpcOutConn struct {
	host   string
	conn   *grpc.ClientConn
	client ChordClient
	used   time.Time
}

// GRPCTransport used by chord
type GRPCTransport struct {
	//server *grpc.Server

	lock  sync.RWMutex
	local map[string]*localRPC

	poolLock sync.RWMutex
	pool     map[string]*rpcOutConn

	shutdown int32
	timeout  time.Duration
	maxIdle  time.Duration
}

// NewGRPCTransport creates a new grpc transport using the provided listener
// and grpc server.
func NewGRPCTransport(rpcTimeout, connMaxIdle time.Duration) *GRPCTransport {
	gt := &GRPCTransport{
		//server:  gserver,
		local:   make(map[string]*localRPC),
		pool:    make(map[string]*rpcOutConn),
		timeout: rpcTimeout,
		maxIdle: connMaxIdle,
	}

	//RegisterChordServer(gt.server, gt)

	//go gt.reapOld()

	return gt
}

// RegisterServer registers the transport with the grpc server and starts the connection
// reaper
func (cs *GRPCTransport) RegisterServer(server *grpc.Server) {
	RegisterChordServer(server, cs)
	go cs.reapOld()
}

// Register vnode rpc's for a vnode.
func (cs *GRPCTransport) Register(v *Vnode, o VnodeRPC) {
	key := v.StringID()
	cs.lock.Lock()
	cs.local[key] = &localRPC{v, o}
	cs.lock.Unlock()
}

// ListVnodes gets a list of the vnodes on the box
func (cs *GRPCTransport) ListVnodes(host string) ([]*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(host)
	if err != nil {
		return nil, err
	}

	// Response channels
	respChan := make(chan []*Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		le, err := out.client.ListVnodesServe(context.Background(), &StringParam{Value: host})
		// Return the connection
		cs.returnConn(out)

		if err == nil {
			respChan <- le.Vnodes
		} else {
			errChan <- err
		}

	}()

	select {
	case <-time.After(cs.timeout):
		return nil, errTimedOut
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// GetCoordinate gets the coordinates for the given remote vnode
func (cs *GRPCTransport) GetCoordinate(vn *Vnode) (*coordinate.Coordinate, error) {
	// Get a conn
	out, err := cs.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan *Response, 1)
	errChan := make(chan error, 1)

	go func(vnode *Vnode) {
		resp, err := out.client.GetCoordinateServe(context.Background(), vnode)
		// Return the connection
		cs.returnConn(out)

		if err == nil {
			respChan <- resp
		} else {
			errChan <- err
		}

	}(vn)

	select {
	case <-time.After(cs.timeout):
		return nil, errTimedOut
	case err := <-errChan:
		return nil, err
	case resp := <-respChan:
		return resp.Coordinate, nil
	}

}

// Ping pings a Vnode to check for liveness and updates the vnode coordinates.
// Self is the caller Vnode and is used to determine rtt's from its vnodes
// perspective.
func (cs *GRPCTransport) Ping(self, target *Vnode) (bool, error) {
	out, err := cs.getConn(target.Host)
	if err != nil {
		return false, err
	}

	// Response channels
	respChan := make(chan *Response, 1)
	errChan := make(chan error, 1)
	// Start time to calculate RTT
	start := time.Now()

	go func() {
		resp, err := out.client.PingServe(context.Background(), &VnodePair{Self: self, Target: target})

		// Return the connection
		cs.returnConn(out)

		if err == nil {
			respChan <- resp
		} else {
			errChan <- err
		}

	}()

	select {
	case <-time.After(cs.timeout):
		return false, errTimedOut
	case err := <-errChan:
		return false, err
	case resp := <-respChan:
		// Get RTT
		rtt := time.Since(start)
		// Get local vnode to be used to update the coordinate
		obj, _ := cs.get(self)
		// Update coordinate using the local vnode
		if _, err := obj.UpdateCoordinate(target, resp.Coordinate, rtt); err != nil {
			log.Printf("[ERROR] Failed to update coordinates for %s: %v", target.Host, err)
		}

		return resp.Ok, nil
	}
}

// GetPredecessor requests a vnode's predecessor
func (cs *GRPCTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan *Response, 1)
	errChan := make(chan error, 1)

	go func(vnode *Vnode) {
		resp, err := out.client.GetPredecessorServe(context.Background(), vnode)
		// Return the connection
		cs.returnConn(out)

		if err == nil {
			respChan <- resp
		} else {
			errChan <- err
		}

	}(vn)

	select {
	case <-time.After(cs.timeout):
		return nil, errTimedOut
	case err := <-errChan:
		return nil, err
	case resp := <-respChan:
		return resp.Vnode, nil
	}
}

// Notify our successor of ourselves
func (cs *GRPCTransport) Notify(target, self *Vnode) ([]*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(target.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan []*Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		le, err := out.client.NotifyServe(context.Background(), &VnodePair{Target: target, Self: self})
		cs.returnConn(out)

		if err == nil {
			respChan <- le.Vnodes
		} else {
			errChan <- err
		}

	}()

	select {
	case <-time.After(cs.timeout):
		return nil, errTimedOut
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// FindSuccessors given the vnode upto n successors
func (cs *GRPCTransport) FindSuccessors(vn *Vnode, n int, k []byte) ([]*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan []*Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		req := &FindSuccReq{VN: vn, Count: int32(n), Key: k}
		le, err := out.client.FindSuccessorsServe(context.Background(), req)
		// Return the connection
		cs.returnConn(out)

		if err == nil {
			respChan <- le.Vnodes
		} else {
			errChan <- err
		}

	}()

	select {
	case <-time.After(cs.timeout):
		return nil, errTimedOut
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// ClearPredecessor clears a predecessor if it matches a given vnode. Used to leave.
func (cs *GRPCTransport) ClearPredecessor(target, self *Vnode) error {
	// Get a conn
	out, err := cs.getConn(target.Host)
	if err != nil {
		return err
	}

	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		_, err := out.client.ClearPredecessorServe(context.Background(), &VnodePair{Target: target, Self: self})
		// Return the connection
		cs.returnConn(out)
		if err == nil {
			respChan <- true
		} else {
			errChan <- err
		}

	}()

	select {
	case <-time.After(cs.timeout):
		return errTimedOut
	case err := <-errChan:
		return err
	case <-respChan:
		return nil
	}
}

// SkipSuccessor instructs a node to skip a given successor. Used to leave.
func (cs *GRPCTransport) SkipSuccessor(target, self *Vnode) error {

	// Get a conn
	out, err := cs.getConn(target.Host)
	if err != nil {
		return err
	}

	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		_, err := out.client.SkipSuccessorServe(context.Background(), &VnodePair{Target: target, Self: self})
		// Return the connection
		cs.returnConn(out)

		if err == nil {
			respChan <- true
		} else {
			errChan <- err
		}

	}()

	select {
	case <-time.After(cs.timeout):
		return errTimedOut
	case err := <-errChan:
		return err
	case <-respChan:
		return nil
	}
}

// Gets an outbound connection to a host
func (cs *GRPCTransport) getConn(host string) (*rpcOutConn, error) {
	if atomic.LoadInt32(&cs.shutdown) == 1 {
		return nil, fmt.Errorf("transport is shutdown")
	}

	// Check if we have a conn cached
	cs.poolLock.RLock()
	if out, ok := cs.pool[host]; ok && out != nil {
		defer cs.poolLock.RUnlock()
		return out, nil
	}
	cs.poolLock.RUnlock()

	// Make a new connection
	conn, err := grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	cs.poolLock.Lock()
	out := &rpcOutConn{
		host:   host,
		client: NewChordClient(conn),
		conn:   conn,
		used:   time.Now(),
	}
	cs.poolLock.Unlock()

	return out, nil
}

func (cs *GRPCTransport) returnConn(o *rpcOutConn) {
	if atomic.LoadInt32(&cs.shutdown) == 1 {
		o.conn.Close()
		return
	}

	// Update the last used time
	o.used = time.Now()

	// Push back into the pool
	cs.poolLock.Lock()
	cs.pool[o.host] = o
	cs.poolLock.Unlock()
}

// get gets a local vnode
func (cs *GRPCTransport) get(vn *Vnode) (VnodeRPC, bool) {
	key := vn.StringID()

	cs.lock.RLock()
	defer cs.lock.RUnlock()

	w, ok := cs.local[key]
	if ok {
		return w.obj, ok
	}
	return nil, ok
}

// ListVnodesServe is the server side call
func (cs *GRPCTransport) ListVnodesServe(ctx context.Context, in *StringParam) (*VnodeList, error) {
	// Generate all the local clients
	vnodes := make([]*Vnode, 0, len(cs.local))
	// Build list
	cs.lock.RLock()
	for _, v := range cs.local {
		vnodes = append(vnodes, v.vnode)
	}
	cs.lock.RUnlock()

	return &VnodeList{Vnodes: vnodes}, nil
}

// PingServe serves a ping request
func (cs *GRPCTransport) PingServe(ctx context.Context, in *VnodePair) (*Response, error) {
	target := in.Target
	obj, ok := cs.get(target)
	if ok {
		return &Response{Coordinate: obj.GetCoordinate(), Ok: ok}, nil
	}
	return &Response{}, fmt.Errorf("target vnode not found: %s/%x", target.Host, target.Id)
}

// NotifyServe serves a notify request
func (cs *GRPCTransport) NotifyServe(ctx context.Context, in *VnodePair) (*VnodeList, error) {
	var (
		obj, ok = cs.get(in.Target)
		resp    = &VnodeList{}
		err     error
	)

	if ok {
		var nodes []*Vnode
		if nodes, err = obj.Notify(in.Self); err == nil {
			resp.Vnodes = trimSlice(nodes)
		}
	} else {
		err = fmt.Errorf("target vnode not found: %s/%x", in.Target.Host, in.Target.Id)
	}

	return resp, err
}

// GetPredecessorServe serves a GetPredecessor request
func (cs *GRPCTransport) GetPredecessorServe(ctx context.Context, in *Vnode) (*Response, error) {
	obj, ok := cs.get(in)

	var (
		// gRPC requires a non-nil response but the vnode may not have a predecessor
		// returning a nil.  We wrap the response in a Response struct to satisfy
		// gRPC.
		resp = &Response{}
		err  error
	)

	if ok {
		resp.Vnode, err = obj.GetPredecessor()
	} else {
		err = fmt.Errorf("target vnode not found: %s/%x", in.Host, in.Id)
	}

	return resp, err
}

// FindSuccessorsServe serves a FindSuccessors request
func (cs *GRPCTransport) FindSuccessorsServe(ctx context.Context, in *FindSuccReq) (*VnodeList, error) {
	var (
		obj, ok = cs.get(in.VN)
		resp    = &VnodeList{}
		err     error
	)

	if ok {
		var nodes []*Vnode
		if nodes, err = obj.FindSuccessors(int(in.Count), in.Key); err == nil {
			resp.Vnodes = trimSlice(nodes)
		}
	} else {
		err = fmt.Errorf("target vnode not found: %s/%x", in.VN.Host, in.VN.Id)
	}

	return resp, err
}

// ClearPredecessorServe serves a ClearPredecessor request
func (cs *GRPCTransport) ClearPredecessorServe(ctx context.Context, in *VnodePair) (*Response, error) {
	var (
		obj, ok = cs.get(in.Target)
		resp    = &Response{}
		err     error
	)

	if ok {
		err = obj.ClearPredecessor(in.Self)
	} else {
		err = fmt.Errorf("target vnode not found: %s/%x", in.Target.Host, in.Target.Id)
	}

	return resp, err
}

// SkipSuccessorServe serves a SkipSuccessor request
func (cs *GRPCTransport) SkipSuccessorServe(ctx context.Context, in *VnodePair) (*Response, error) {
	var (
		obj, ok = cs.get(in.Target)
		resp    = &Response{}
		err     error
	)

	if ok {
		err = obj.SkipSuccessor(in.Self)
	} else {
		err = fmt.Errorf("target vnode not found: %s/%x", in.Target.Host, in.Target.Id)
	}

	return resp, err
}

// GetCoordinateServe serves a GetCoordinate request returning the Coordinate for this node
func (cs *GRPCTransport) GetCoordinateServe(ctx context.Context, vn *Vnode) (*Response, error) {
	var (
		obj, ok = cs.get(vn)
		resp    = &Response{}
		err     error
	)
	if ok {
		resp.Coordinate = obj.GetCoordinate()
	} else {
		err = fmt.Errorf("target vnode not found: %s/%x", vn.Host, vn.Id)
	}

	return resp, err
}

// Shutdown signals a shutdown on the transport, closing all outbound connections.
func (cs *GRPCTransport) Shutdown() {
	atomic.StoreInt32(&cs.shutdown, 1)

	// Close all the outbound
	cs.poolLock.Lock()
	for _, conn := range cs.pool {
		conn.conn.Close()
	}
	cs.pool = nil
	cs.poolLock.Unlock()
}

// Closes old outbound connections
func (cs *GRPCTransport) reapOld() {
	for {
		if atomic.LoadInt32(&cs.shutdown) == 1 {
			return
		}
		time.Sleep(30 * time.Second)
		cs.reapOnce()
	}
}

func (cs *GRPCTransport) reapOnce() {
	cs.poolLock.Lock()
	for host, conn := range cs.pool {
		if time.Since(conn.used) > cs.maxIdle {
			conn.conn.Close()
			delete(cs.pool, host)
		}
	}
	cs.poolLock.Unlock()
}
