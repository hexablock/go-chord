package chord

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipkg/go-mux"
	"github.com/zeebo/bencode"
)

type UTPTransport struct {
	sock *mux.Layer

	dialTimeout time.Duration // Dial timeout
	rpcTimeout  time.Duration
	maxIdle     time.Duration // Max age of a connection

	local map[string]*localRPC

	lock    sync.RWMutex
	inbound map[net.Conn]struct{} // inbound connections

	poolLock sync.Mutex
	pool     map[string][]*utpOutConn // outbound connection pool

	shutdown int32

	RPCServe func(IEncoder, IDecoder) (interface{}, error)
}

type IEncoder interface {
	Encode(interface{}) error
}
type IDecoder interface {
	Decode(interface{}) error
}

type utpOutConn struct {
	host   string
	sock   net.Conn
	header utpHeader
	enc    IEncoder
	dec    IDecoder
	used   time.Time
}

func (u *utpOutConn) Encode(v interface{}) error {
	return u.enc.Encode(v)
}
func (u *utpOutConn) Decode(v interface{}) error {
	return u.dec.Decode(v)
}

const (
	utpPing = iota
	utpListReq
	utpGetPredReq
	utpNotifyReq
	utpFindSucReq
	utpClearPredReq
	utpSkipSucReq
)

type utpHeader struct {
	T int
}

// Potential body types
type utpBodyError struct {
	//Err error
	Err string
}
type utpBodyString struct {
	S string
}
type utpBodyVnode struct {
	Vn *Vnode
}
type utpBodyTwoVnode struct {
	Target *Vnode
	Vn     *Vnode
}
type utpBodyFindSuc struct {
	Target *Vnode
	Num    int
	Key    []byte
}
type utpBodyVnodeError struct {
	Vnode *Vnode
	Err   string
	//Err   error
}
type utpBodyVnodeListError struct {
	Vnodes []*Vnode
	Err    string
	//Err    error
}
type utpBodyBoolError struct {
	B   bool
	Err string
	//Err error
}

// InitUTPTransport creates a new UTP transport on the given listen address with the
// configured timeout duration. A RPC function can be given to implement additional
// rpc's. maxConnIdle is the maximum age of a connection
func InitUTPTransport(layer *mux.Layer, dialTimeout, rpcTimeout, maxConnIdle time.Duration) (*UTPTransport, error) {
	// Setup the transport
	u := &UTPTransport{
		sock:        layer,
		dialTimeout: dialTimeout,
		rpcTimeout:  rpcTimeout,
		maxIdle:     maxConnIdle,
		local:       make(map[string]*localRPC),
		inbound:     make(map[net.Conn]struct{}),
		pool:        make(map[string][]*utpOutConn),
	}

	// Listen for connections
	go u.listen()

	// Reap old connections
	go u.reapOld()

	// Done
	return u, nil
}

// Checks for a local vnode
func (t *UTPTransport) get(vn *Vnode) (VnodeRPC, bool) {

	key := vn.String()
	t.lock.RLock()
	defer t.lock.RUnlock()
	w, ok := t.local[key]
	if ok {
		return w.obj, ok
	}
	return nil, ok
}

// Gets an outbound connection to a host
func (t *UTPTransport) getConn(host string) (*utpOutConn, error) {
	// Check if we have a conn cached
	var out *utpOutConn
	t.poolLock.Lock()
	if atomic.LoadInt32(&t.shutdown) == 1 {
		t.poolLock.Unlock()
		return nil, fmt.Errorf("UTP transport is shutdown")
	}
	list, ok := t.pool[host]
	if ok && len(list) > 0 {
		out = list[len(list)-1]
		list = list[:len(list)-1]
		t.pool[host] = list
	}
	t.poolLock.Unlock()

	if out != nil {
		return out, nil
		// TODO:=Verify that the socket is valid. Might be closed.  THis blocks
		// and needs to be revisited.
		//if _, err := out.sock.Read(nil); err == nil {
		//return out, nil
		//}
		//out.sock.Close()
	}

	// Try to establish a connection
	conn, err := t.sock.Dial(host, t.dialTimeout)
	if err != nil {
		return nil, err
	}

	// Wrap the sock
	out = &utpOutConn{
		host: host,
		sock: conn,
		enc:  newEncoder(conn),
		dec:  newDecoder(conn),
		used: time.Now(),
	}
	return out, nil
}

// Returns an outbound TCP connection to the pool
func (t *UTPTransport) returnConn(o *utpOutConn) {
	// Update the last used time
	o.used = time.Now()

	// Push back into the pool
	t.poolLock.Lock()
	defer t.poolLock.Unlock()
	if atomic.LoadInt32(&t.shutdown) == 1 {
		o.sock.Close()
		return
	}
	list, _ := t.pool[o.host]
	t.pool[o.host] = append(list, o)
}

// ListVnodes gets a list of the vnodes on the box
func (t *UTPTransport) ListVnodes(host string) ([]*Vnode, error) {
	// Get a conn
	out, err := t.getConn(host)
	if err != nil {
		return nil, err
	}

	// Response channels
	respChan := make(chan []*Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.T = utpListReq
		body := utpBodyString{S: host}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := utpBodyVnodeListError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == "" {
			respChan <- resp.Vnodes
		} else {
			errChan <- errors.New(resp.Err)
		}
	}()

	select {
	case <-time.After(t.rpcTimeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// Ping a Vnode, check for liveness
func (t *UTPTransport) Ping(vn *Vnode) (bool, error) {
	// Get a conn
	out, err := t.getConn(vn.Host)
	if err != nil {
		return false, err
	}

	// Response channels
	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.T = utpPing
		body := utpBodyVnode{Vn: vn}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := utpBodyBoolError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == "" {
			respChan <- resp.B
		} else {
			errChan <- errors.New(resp.Err)
		}
	}()

	select {
	case <-time.After(t.rpcTimeout):
		return false, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return false, err
	case res := <-respChan:
		return res, nil
	}
}

// GetPredecessor requests a nodes predecessor
func (t *UTPTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	// Get a conn
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan *Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.T = utpGetPredReq
		body := utpBodyVnode{Vn: vn}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := utpBodyVnodeError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == "" {
			respChan <- resp.Vnode
		} else {
			errChan <- errors.New(resp.Err)
		}
	}()

	select {
	case <-time.After(t.rpcTimeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// Notify our successor of ourselves
func (t *UTPTransport) Notify(target, self *Vnode) ([]*Vnode, error) {
	// Get a conn
	out, err := t.getConn(target.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan []*Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.T = utpNotifyReq
		body := utpBodyTwoVnode{Target: target, Vn: self}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := utpBodyVnodeListError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == "" {
			respChan <- resp.Vnodes
		} else {
			errChan <- errors.New(resp.Err)
		}
	}()

	select {
	case <-time.After(t.rpcTimeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// FindSuccessors for the given vnode and key upto n successors
func (t *UTPTransport) FindSuccessors(vn *Vnode, n int, k []byte) ([]*Vnode, error) {
	// Get a conn
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan []*Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.T = utpFindSucReq
		body := utpBodyFindSuc{Target: vn, Num: n, Key: k}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := utpBodyVnodeListError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == "" {
			respChan <- resp.Vnodes
		} else {
			errChan <- errors.New(resp.Err)
		}
	}()

	select {
	case <-time.After(t.rpcTimeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// ClearPredecessor clears a predecessor if it matches a given vnode. Used to leave.
func (t *UTPTransport) ClearPredecessor(target, self *Vnode) error {
	// Get a conn
	out, err := t.getConn(target.Host)
	if err != nil {
		return err
	}

	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.T = utpClearPredReq
		body := utpBodyTwoVnode{Target: target, Vn: self}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := utpBodyError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == "" {
			respChan <- true
		} else {
			errChan <- errors.New(resp.Err)
		}
	}()

	select {
	case <-time.After(t.rpcTimeout):
		return fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return err
	case <-respChan:
		return nil
	}
}

// SkipSuccessor instructs a node to skip a given successor. Used to leave.
func (t *UTPTransport) SkipSuccessor(target, self *Vnode) error {
	// Get a conn
	out, err := t.getConn(target.Host)
	if err != nil {
		return err
	}

	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.T = utpSkipSucReq
		body := utpBodyTwoVnode{Target: target, Vn: self}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := utpBodyError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == "" {
			respChan <- true
		} else {
			errChan <- errors.New(resp.Err)
		}
	}()

	select {
	case <-time.After(t.rpcTimeout):
		return fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return err
	case <-respChan:
		return nil
	}
}

// Register for an RPC callbacks
func (t *UTPTransport) Register(v *Vnode, o VnodeRPC) {
	key := v.String()
	t.lock.Lock()
	t.local[key] = &localRPC{v, o}
	t.lock.Unlock()
}

// Shutdown the TCP transport
func (t *UTPTransport) Shutdown() {
	atomic.StoreInt32(&t.shutdown, 1)
	t.sock.Close()

	// Close all the inbound connections
	t.lock.RLock()
	for conn := range t.inbound {
		conn.Close()
	}
	t.lock.RUnlock()

	// Close all the outbound
	t.poolLock.Lock()
	for _, conns := range t.pool {
		for _, out := range conns {
			out.sock.Close()
		}
	}
	t.pool = nil
	t.poolLock.Unlock()
}

// Closes old outbound connections
func (t *UTPTransport) reapOld() {
	for {
		if atomic.LoadInt32(&t.shutdown) == 1 {
			return
		}
		time.Sleep(30 * time.Second)
		t.reapOnce()
	}
}

func (t *UTPTransport) reapOnce() {
	t.poolLock.Lock()
	defer t.poolLock.Unlock()
	for host, conns := range t.pool {
		max := len(conns)
		for i := 0; i < max; i++ {
			if time.Since(conns[i].used) > t.maxIdle {
				conns[i].sock.Close()
				conns[i], conns[max-1] = conns[max-1], nil
				max--
				i--
			}
		}
		// Trim any idle conns
		t.pool[host] = conns[:max]
	}
}

// Listens for inbound connections
func (t *UTPTransport) listen() {
	for {
		conn, err := t.sock.Accept()
		if err != nil {
			if atomic.LoadInt32(&t.shutdown) == 0 {
				fmt.Printf("[ERR] Error accepting TCP connection! %s", err)
				continue
			} else {
				return
			}
		}

		// Register the inbound conn
		t.lock.Lock()
		t.inbound[conn] = struct{}{}
		t.lock.Unlock()

		// Start handler
		go t.handleConn(conn)
	}
}

// Handles inbound TCP connections
func (t *UTPTransport) handleConn(conn net.Conn) {
	// Defer the cleanup
	defer func() {
		t.lock.Lock()
		delete(t.inbound, conn)
		t.lock.Unlock()
		conn.Close()
	}()

	dec := newDecoder(conn)
	enc := newEncoder(conn)

	header := utpHeader{}
	var sendResp interface{}
	for {
		// Get the header
		if err := dec.Decode(&header); err != nil {
			if atomic.LoadInt32(&t.shutdown) == 0 && err.Error() != "EOF" {
				log.Printf("[ERR] Failed to decode UTP header! Got %s", err)
			}
			return
		}
		// Read in the body and process request
		switch header.T {
		case utpPing:
			body := utpBodyVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode UTP body! Got %s", err)
				return
			}

			// Generate a response
			_, ok := t.get(body.Vn)
			if ok {
				sendResp = utpBodyBoolError{B: ok}
			} else {
				sendResp = utpBodyBoolError{Err: fmt.Sprintf("target VN not found: %s:%s",
					body.Vn.Host, body.Vn.String())}
			}

		case utpListReq:
			body := utpBodyString{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode UTP body! Got %s", err)
				return
			}

			// Generate all the local clients
			res := make([]*Vnode, 0, len(t.local))

			// Build list
			t.lock.RLock()
			for _, v := range t.local {
				res = append(res, v.vnode)
			}
			t.lock.RUnlock()

			sendResp = utpBodyVnodeListError{Vnodes: trimSlice(res)}

		case utpGetPredReq:
			body := utpBodyVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode UTP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Vn)
			resp := utpBodyVnodeError{}
			sendResp = &resp
			if !ok {
				resp.Err = fmt.Sprintf("target VN not found: %s:%s",
					body.Vn.Host, body.Vn.String())
				break
			}
			node, err := obj.GetPredecessor()
			if err == nil {
				resp.Vnode = node
			} else {
				resp.Err = err.Error()
			}

		case utpNotifyReq:
			body := utpBodyTwoVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode UTP body! Got %s", err)
				return
			}

			// invalid request
			if body.Target == nil {
				return
			}

			// Generate a response
			obj, ok := t.get(body.Target)
			resp := utpBodyVnodeListError{}
			sendResp = &resp
			if !ok {
				resp.Err = fmt.Sprintf("target VN not found: %s:%s",
					body.Target.Host, body.Target.String())
				break
			}

			nodes, err := obj.Notify(body.Vn)
			if err == nil {
				resp.Vnodes = trimSlice(nodes)
			} else {
				resp.Err = err.Error()
			}

		case utpFindSucReq:
			body := utpBodyFindSuc{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode UTP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Target)
			resp := utpBodyVnodeListError{}
			sendResp = &resp
			if ok {
				nodes, err := obj.FindSuccessors(body.Num, body.Key)
				if err == nil {
					resp.Vnodes = trimSlice(nodes)
				} else {
					resp.Err = err.Error()
				}
			} else {
				resp.Err = fmt.Sprintf("target VN not found: %s:%s",
					body.Target.Host, body.Target.String())
			}

		case utpClearPredReq:
			body := utpBodyTwoVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode UTP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Target)
			resp := utpBodyError{}
			sendResp = &resp
			if ok {
				if err := obj.ClearPredecessor(body.Vn); err != nil {
					resp.Err = err.Error()
				}

			} else {
				resp.Err = fmt.Sprintf("target VN not found: %s:%s",
					body.Target.Host, body.Target.String())
			}

		case utpSkipSucReq:
			body := utpBodyTwoVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode UTP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Target)
			resp := utpBodyError{}
			sendResp = &resp
			if ok {
				if err := obj.SkipSuccessor(body.Vn); err != nil {
					resp.Err = err.Error()
				}
			} else {
				resp.Err = fmt.Sprintf("target VN not found: %s:%s",
					body.Target.Host, body.Target.String())
			}

		default:
			log.Printf("[ERR] Unknown request type! Got %d", header.T)
			return
		}

		// Send the response
		if err := enc.Encode(sendResp); err != nil {
			log.Printf("[ERR] Failed to send UTP body! Got %s", err)
			return
		}
	}
}

//
// abstratct encoder and decoder
//

func newDecoder(r io.Reader) IDecoder {
	return bencode.NewDecoder(r)
}

func newEncoder(w io.Writer) IEncoder {
	return bencode.NewEncoder(w)
}
