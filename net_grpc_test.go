package chord

import (
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func prepRingGrpc(port int) (*Config, *grpc.Server, *GRPCTransport, error) {
	listen := fmt.Sprintf("127.0.0.1:%d", port)
	conf := DefaultConfig(listen)
	conf.Delegate = &MockDelegate{}
	conf.StabilizeMin = time.Duration(15 * time.Millisecond)
	conf.StabilizeMax = time.Duration(45 * time.Millisecond)
	timeout := time.Duration(3 * time.Second)
	connMaxIdle := time.Duration(300 * time.Second)

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, nil, nil, err
	}

	opt := grpc.CustomCodec(&PayloadCodec{})
	gserver := grpc.NewServer(opt)

	trans := NewGRPCTransport(timeout, connMaxIdle)
	RegisterChordServer(gserver, trans)
	go gserver.Serve(ln)

	return conf, gserver, trans, nil
}

func TestGRPCJoin(t *testing.T) {
	// Prepare to create 2 nodes
	c1, s1, t1, err := prepRingGrpc(20025)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create initial ring
	r1, err := Create(c1, t1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	c2, s2, t2, err := prepRingGrpc(20026)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	<-time.After(1 * time.Second)
	// Join ring
	r2, err := Join(c2, t2, c1.Hostname)
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Shutdown
	r1.Shutdown()
	r2.Shutdown()

	s1.Stop()
	t1.Shutdown()

	s2.Stop()
	t2.Shutdown()
}

func TestGRPCLeave(t *testing.T) {
	// Prepare to create 2 nodes
	c1, s1, t1, err := prepRingGrpc(20027)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	c2, s2, t2, err := prepRingGrpc(20028)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create initial ring
	r1, err := Create(c1, t1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Join ring
	r2, err := Join(c2, t2, c1.Hostname)
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Wait for some stabilization
	<-time.After(1 * time.Second)

	// Node 1 should leave
	r1.Leave()

	s1.GracefulStop()
	t1.Shutdown()

	// Wait for stabilization
	<-time.After(200 * time.Millisecond)

	// Verify r2 ring is still in tact
	for _, vn := range r2.vnodes {
		if vn.successors[0].Host != r2.config.Hostname {
			t.Fatalf("bad successor! Got:%s:%s want: %s", vn.successors[0].Host,
				vn.successors[0].String(), r2.config.Hostname)
		}
	}

	r2.Shutdown()
	s2.Stop()
	t2.Shutdown()
}

func TestGRPCLookup(t *testing.T) {
	c1, _, t1, err := prepRingGrpc(21025)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create initial ring
	r1, err := Create(c1, t1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	c2, _, t2, err := prepRingGrpc(21026)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	<-time.After(1 * time.Second)
	// Join ring
	r2, err := Join(c2, t2, c1.Hostname)
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	testkey := []byte("foobarboze")
	_, v1, _ := r1.Lookup(5, testkey)

	r2.Lookup(5, testkey)
	_, v2, _ := r2.Lookup(5, testkey)
	for i, v := range v1 {
		if v.String() != v2[i].String() {
			t.Error("mismatch")
		}
	}
}
