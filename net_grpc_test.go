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
	timeout := time.Duration(2 * time.Second)
	connMaxIdle := time.Duration(300 * time.Second)

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, nil, nil, err
	}
	gserver := grpc.NewServer()
	trans := NewGRPCTransport(timeout, connMaxIdle)
	trans.RegisterServer(gserver)
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

	// Stop accepting new connections letting current ones complete
	s1.GracefulStop()
	t1.Shutdown()

	// Stop accepting new connections letting current ones complete
	s2.GracefulStop()
	t2.Shutdown()
}

func TestGRPCLeave(t *testing.T) {
	// Prepare to create 2 nodes
	c1, s1, t1, err := prepRingGrpc(20027)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	c2, _, t2, err := prepRingGrpc(20028)
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
	<-time.After(100 * time.Millisecond)

	// Node 1 should leave
	r1.Leave()

	s1.GracefulStop()
	t1.Shutdown()

	// Wait for stabilization
	<-time.After(100 * time.Millisecond)

	// Verify r2 ring is still in tact
	for _, vn := range r2.vnodes {
		if vn.successors[0].Host != r2.config.Hostname {
			t.Fatalf("bad successor! Got:%s:%s want: %s", vn.successors[0].Host,
				vn.successors[0].StringID(), r2.config.Hostname)
		}
	}
}

func TestGRPCCoordinate(t *testing.T) {
	// Prepare to create 2 nodes
	c1, s1, t1, err := prepRingGrpc(20037)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	c2, s2, t2, err := prepRingGrpc(20038)
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
	<-time.After(100 * time.Millisecond)

	vns, _ := r2.transport.ListVnodes("127.0.0.1:20038")
	co, err := r1.LookupCoordinate(vns[0])
	if err != nil {
		t.Fatal(err)
	}
	if co == nil {
		t.Fatal("coords should not be nil")
	}

	r1.Shutdown()
	r2.Shutdown()

	// Stop accepting new connections letting current ones complete
	s1.GracefulStop()
	t1.Shutdown()

	// Stop accepting new connections letting current ones complete
	s2.GracefulStop()
	t2.Shutdown()
}
