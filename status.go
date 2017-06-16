package chord

import (
	"time"

	"github.com/hexablock/go-chord/coordinate"
)

// VnodeStatus holds the status for a single Vnode
type VnodeStatus struct {
	LastStabilized time.Time
	Predecessor    *Vnode
	Successors     []*Vnode
}

// Status represents the status of a node
type Status struct {
	Coordinate *coordinate.Coordinate
	Vnodes     []*VnodeStatus
	HashBits   int
	Meta       Meta
}

// Status returns ring information of this node
func (r *Ring) Status() *Status {
	status := &Status{
		HashBits:   r.config.hashBits,
		Coordinate: r.coordClient.GetCoordinate(),
		Vnodes:     make([]*VnodeStatus, len(r.vnodes)),
		Meta:       r.config.Meta,
	}

	for i, vn := range r.vnodes {
		status.Vnodes[i] = &VnodeStatus{
			LastStabilized: vn.stabilized,
			Predecessor:    vn.predecessor,
			Successors:     vn.successors,
		}
	}

	return status
}
