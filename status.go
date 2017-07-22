package chord

import (
	"fmt"
	"time"

	"github.com/hexablock/go-chord/coordinate"
)

// VnodeStatus holds the status for a single Vnode
type VnodeStatus struct {
	Vnode          Vnode
	LastStabilized time.Time
}

// Status represents the status of a node
type Status struct {
	Coordinate    *coordinate.Coordinate
	Vnodes        []*VnodeStatus
	HashBits      int
	DelegateQueue string // The current to size ratio ie. curr/size
	Meta          Meta
}

// Status returns ring information of this node
func (r *Ring) Status() *Status {
	status := &Status{
		HashBits:      r.config.hashBits,
		Coordinate:    r.coordClient.GetCoordinate(),
		Vnodes:        make([]*VnodeStatus, len(r.vnodes)),
		Meta:          r.config.Meta,
		DelegateQueue: fmt.Sprintf("%d/%d", len(r.delegateCh), r.config.DelegateQueueSize),
	}

	for i, vn := range r.vnodes {
		status.Vnodes[i] = vn.Status()
	}

	return status
}
