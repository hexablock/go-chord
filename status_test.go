package chord

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStatus(t *testing.T) {
	// Create a multi transport
	ml := InitMLTransport()

	// Create the initial ring
	conf1 := fastConf()
	r1, err := Create(conf1, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "test2"
	r2, err := Join(conf2, ml, "test")
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Wait for some stabilization
	<-time.After(100 * time.Millisecond)

	s1 := r1.Status()
	s2 := r2.Status()

	if s1.Coordinate == nil {
		t.Error("status coordinate should not nil")
	}
	if s1.HashBits != conf1.hashBits {
		t.Error("status hashbits mismatch")
	}
	if len(s1.Vnodes) != conf1.NumVnodes {
		t.Error("status vnode count mismatch")
	}

	if s2.Coordinate == nil {
		t.Error("status coordinate should not nil")
	}
	if s2.HashBits != conf2.hashBits {
		t.Error("status hashbits mismatch")
	}
	if len(s2.Vnodes) != conf2.NumVnodes {
		t.Error("status vnode count mismatch")
	}

	b, _ := json.MarshalIndent(s1, "", " ")
	t.Logf("%s", b)
}
