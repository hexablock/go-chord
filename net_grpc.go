package chord

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"
)

type rpcOutConn struct {
	host   string
	conn   *grpc.ClientConn
	client ChordClient
	used   time.Time
}

// GRPCTransport used by chord
type GRPCTransport struct {
	lock  sync.RWMutex
	local map[string]*localRPC

	poolLock sync.Mutex
	pool     map[string][]*rpcOutConn

	shutdown int32
	timeout  time.Duration
	maxIdle  time.Duration
}

// NewGRPCTransport creates a new grpc transport using the provided listener and grpc server.
func NewGRPCTransport(rpcTimeout, connMaxIdle time.Duration) *GRPCTransport {
	gt := &GRPCTransport{
		local:   map[string]*localRPC{},
		pool:    map[string][]*rpcOutConn{},
		timeout: rpcTimeout,
		maxIdle: connMaxIdle,
	}

	go gt.reapOld()

	return gt
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
	defer cs.poolLock.Unlock()
	for host, conns := range cs.pool {
		max := len(conns)
		for i := 0; i < max; i++ {
			if time.Since(conns[i].used) > cs.maxIdle {
				conns[i].conn.Close()
				conns[i], conns[max-1] = conns[max-1], nil
				max--
				i--
			}
		}
		// Trim any idle conns
		cs.pool[host] = conns[:max]
	}
}

// Register registers rpc's for a vnode
func (cs *GRPCTransport) Register(v *Vnode, o VnodeRPC) {
	key := v.String()
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
	// Build payload
	payload := serializeStringToPayload(host)
	// Call
	rsp, err := out.client.ListVnodesServe(context.Background(), payload)
	if err != nil {
		out.conn.Close()
		return nil, err
	}

	cs.returnConn(out)

	return DeserializeVnodeListErr(rsp.Data)
}

// Ping a Vnode, check for liveness
func (cs *GRPCTransport) Ping(target *Vnode) (bool, error) {
	out, err := cs.getConn(target.Host)
	if err != nil {
		return false, err
	}

	payload := serializeVnodeToPayload(target)
	rsp, err := out.client.PingServe(context.Background(), payload)
	if err != nil {
		out.conn.Close()
		return false, err
	}

	// Return the connection
	cs.returnConn(out)

	return deserializeBoolErr(rsp.Data)
}

// GetPredecessor requests a vnode's predecessor
func (cs *GRPCTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	payload := serializeVnodeToPayload(vn)
	rsp, err := out.client.GetPredecessorServe(context.Background(), payload)
	if err != nil {
		out.conn.Close()
		return nil, err
	}
	// Return the connection
	cs.returnConn(out)

	return DeserializeVnodeErr(rsp.Data)
}

// Notify our successor of ourselves
func (cs *GRPCTransport) Notify(target, self *Vnode) ([]*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(target.Host)
	if err != nil {
		return nil, err
	}

	payload := serializeVnodePairToPayload(target, self)
	rsp, err := out.client.NotifyServe(context.Background(), payload)
	if err != nil {
		out.conn.Close()
		return nil, err
	}

	cs.returnConn(out)

	return DeserializeVnodeListErr(rsp.Data)
}

// FindSuccessors given the vnode upto n successors
func (cs *GRPCTransport) FindSuccessors(vn *Vnode, n int, k []byte) ([]*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	payload := serializeVnodeIntBytesToPayload(vn, n, k)
	rsp, err := out.client.FindSuccessorsServe(context.Background(), payload)
	if err != nil {
		out.conn.Close()
		return nil, err
	}
	// Return the connection
	cs.returnConn(out)

	return DeserializeVnodeListErr(rsp.Data)
}

// ClearPredecessor clears a predecessor if it matches a given vnode. Used to leave.
func (cs *GRPCTransport) ClearPredecessor(target, self *Vnode) error {
	// Get a conn
	out, err := cs.getConn(target.Host)
	if err != nil {
		return err
	}

	payload := serializeVnodePairToPayload(target, self)
	rsp, err := out.client.ClearPredecessorServe(context.Background(), payload)
	if err != nil {
		out.conn.Close()
		return err
	}

	// Return the connection
	cs.returnConn(out)

	return deserializeErr(rsp.Data)
}

// SkipSuccessor instructs a node to skip a given successor. Used to leave.
func (cs *GRPCTransport) SkipSuccessor(target, self *Vnode) error {

	// Get a conn
	out, err := cs.getConn(target.Host)
	if err != nil {
		return err
	}

	payload := serializeVnodePairToPayload(target, self)
	rsp, err := out.client.SkipSuccessorServe(context.Background(), payload)
	if err != nil {
		out.conn.Close()
		return err
	}

	// Return the connection
	cs.returnConn(out)

	return deserializeErr(rsp.Data)
}

// Gets an outbound connection to a host
func (cs *GRPCTransport) getConn(host string) (*rpcOutConn, error) {
	// Check if we have a conn cached
	var out *rpcOutConn
	cs.poolLock.Lock()
	if atomic.LoadInt32(&cs.shutdown) == 1 {
		cs.poolLock.Unlock()
		return nil, fmt.Errorf("gRPC transport is shutdown")
	}

	list, ok := cs.pool[host]
	if ok && len(list) > 0 {
		out = list[len(list)-1]
		list = list[:len(list)-1]
		cs.pool[host] = list
	}
	cs.poolLock.Unlock()

	// Make a new connection
	if out == nil {
		conn, err := grpc.Dial(host, grpc.WithInsecure(), grpc.WithCodec(&PayloadCodec{}))
		if err != nil {
			return nil, err
		}

		out = &rpcOutConn{
			host:   host,
			client: NewChordClient(conn),
			conn:   conn,
			used:   time.Now(),
		}
	}

	// return connection
	return out, nil
}

func (cs *GRPCTransport) returnConn(o *rpcOutConn) {
	// Update the last used time
	o.used = time.Now()
	// Push back into the pool
	cs.poolLock.Lock()
	defer cs.poolLock.Unlock()
	if atomic.LoadInt32(&cs.shutdown) == 1 {
		o.conn.Close()
		return
	}

	list, _ := cs.pool[o.host]
	cs.pool[o.host] = append(list, o)
}

// Checks for a local vnode
func (cs *GRPCTransport) get(vn *Vnode) (VnodeRPC, bool) {
	key := vn.String()

	cs.lock.RLock()
	defer cs.lock.RUnlock()

	w, ok := cs.local[key]
	if ok {
		return w.obj, ok
	}
	return nil, ok
}

// ListVnodesServe is the server side call
func (cs *GRPCTransport) ListVnodesServe(ctx context.Context, in *Payload) (*Payload, error) {
	// Generate all the local clients
	list := make([]*Vnode, 0, len(cs.local))
	// Build list
	cs.lock.RLock()
	for _, v := range cs.local {
		list = append(list, v.vnode)
	}
	cs.lock.RUnlock()

	return &Payload{Data: SerializeVnodeListErr(list, nil)}, nil
}

// PingServe serves a ping request to a vnode.
func (cs *GRPCTransport) PingServe(ctx context.Context, in *Payload) (*Payload, error) {
	vn := deserializeVnode(in.Data)

	var err error
	_, ok := cs.get(vn)
	if !ok {
		err = fmt.Errorf("target vnode not found: %s/%s", vn.Host, vn.Id)
	}

	return serializeBoolErrToPayload(ok, err), nil
}

// NotifyServe serves a Notify request
func (cs *GRPCTransport) NotifyServe(ctx context.Context, in *Payload) (*Payload, error) {
	target, self := deserializeVnodePair(in.Data)
	obj, ok := cs.get(target)

	var (
		vnodes []*Vnode
		err    error
	)

	if ok {
		if vnodes, err = obj.Notify(self); err == nil {
			vnodes = trimSlice(vnodes)
		}
	} else {
		err = fmt.Errorf("target vnode not found: %s/%s", target.Host, target.Id)
	}

	return &Payload{Data: SerializeVnodeListErr(vnodes, err)}, nil
}

// GetPredecessorServe serves a GetPredecessor request
func (cs *GRPCTransport) GetPredecessorServe(ctx context.Context, in *Payload) (*Payload, error) {
	vn := deserializeVnode(in.Data)
	obj, ok := cs.get(vn)

	var (
		pred *Vnode
		err  error
	)

	if ok {
		pred, err = obj.GetPredecessor()
	} else {
		err = fmt.Errorf("target vnode not found: %s/%x", vn.Host, vn.Id)
	}

	return &Payload{Data: SerializeVnodeErr(pred, err)}, nil
}

// FindSuccessorsServe serves a FindSuccessors request
func (cs *GRPCTransport) FindSuccessorsServe(ctx context.Context, in *Payload) (*Payload, error) {
	vn, n, k := deserializeVnodeIntBytes(in.Data)
	obj, ok := cs.get(vn)

	var (
		vnodes []*Vnode
		err    error
	)

	if ok {
		if vnodes, err = obj.FindSuccessors(n, k); err == nil {
			vnodes = trimSlice(vnodes)
		}
	} else {
		err = fmt.Errorf("target vnode not found: %s/%s", vn.Host, vn.Id)
	}

	return &Payload{Data: SerializeVnodeListErr(vnodes, err)}, nil
}

// ClearPredecessorServe handles the server-side call to a clear a predecessor
func (cs *GRPCTransport) ClearPredecessorServe(ctx context.Context, in *Payload) (*Payload, error) {
	var (
		target, self = deserializeVnodePair(in.Data)
		obj, ok      = cs.get(target)
		e            string
	)

	if ok {
		if err := obj.ClearPredecessor(self); err != nil {
			e = err.Error()
		}
	} else {
		e = fmt.Sprintf("target vnode not found: %s/%s", target.Host, target.Id)
	}

	return serializeStringToPayload(e), nil
}

// SkipSuccessorServe handles the server-side call to skip a successor
func (cs *GRPCTransport) SkipSuccessorServe(ctx context.Context, in *Payload) (*Payload, error) {
	var (
		target, self = deserializeVnodePair(in.Data)
		obj, ok      = cs.get(target)
		e            string
	)

	if ok {
		if err := obj.SkipSuccessor(self); err != nil {
			e = err.Error()
		}
	} else {
		e = fmt.Sprintf("target vnode not found: %s/%s", target.Host, target.Id)
	}

	return serializeStringToPayload(e), nil
}

// Shutdown the TCP transport
func (cs *GRPCTransport) Shutdown() {
	atomic.StoreInt32(&cs.shutdown, 1)
	// Stop grcp server
	//cs.server.GracefulStop()

	// Close all the outbound
	cs.poolLock.Lock()
	for _, conns := range cs.pool {
		for _, out := range conns {
			out.conn.Close()
		}
	}
	cs.pool = nil
	cs.poolLock.Unlock()
}
