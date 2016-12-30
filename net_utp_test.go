package chord

import (
	"fmt"
	"testing"
	"time"

	"github.com/anacrolix/utp"

	"github.com/ipkg/go-mux"
)

func prepRingUTP(port int) (*Config, *UTPTransport, error) {
	listen := fmt.Sprintf("127.0.0.1:%d", port)
	conf := DefaultConfig(listen)
	conf.StabilizeMin = time.Duration(15 * time.Millisecond)
	conf.StabilizeMax = time.Duration(45 * time.Millisecond)

	dialTimeout := time.Duration(1 * time.Second)
	rpcTimeout := time.Duration(20 * time.Millisecond)
	maxConnIdle := time.Duration(300 * time.Second)

	ln, err := utp.NewSocket("udp", listen)
	if err != nil {
		return nil, nil, err
	}

	mx := mux.NewMux(ln, ln.Addr())
	go mx.Serve()
	sock1 := mx.Listen(72)

	trans, err := InitUTPTransport(sock1, dialTimeout, rpcTimeout, maxConnIdle)
	if err != nil {
		return nil, nil, err
	}
	return conf, trans, nil
}

func TestUTPJoin(t *testing.T) {
	// Prepare to create 2 nodes
	c1, t1, err := prepRingUTP(30025)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	c2, t2, err := prepRingUTP(30026)
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

	// Shutdown
	r1.Shutdown()
	r2.Shutdown()
	t1.Shutdown()
	t2.Shutdown()
}

func TestUTPLeave(t *testing.T) {
	c1, t1, err := prepRingUTP(30027)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	// Create initial ring
	r1, err := Create(c1, t1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	<-time.After(200 * time.Millisecond)

	c2, t2, err := prepRingUTP(30028)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	// Join ring
	r2, err := Join(c2, t2, c1.Hostname)
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Wait for some stabilization
	<-time.After(300 * time.Millisecond)

	// Node 1 should leave
	r1.Leave()
	t1.Shutdown()

	// Wait for stabilization
	<-time.After(1000 * time.Millisecond)

	// Verify r2 ring is still in tact
	for _, vn := range r2.vnodes {
		if vn.successors[0].Host != r2.config.Hostname {
			t.Fatalf("bad successor! Got:%s:%s", vn.successors[0].Host,
				vn.successors[0])
		}
	}
}
