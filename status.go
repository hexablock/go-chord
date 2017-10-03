package chord

import (
	"time"

	"github.com/hexablock/go-chord/coordinate"
)

// VnodeStatus holds the status for a single Vnode
type VnodeStatus struct {
	ID         string
	Stabilized time.Time
}

// Status represents the status of a node
type Status struct {
	Hostname   string
	HashBits   int
	Meta       Meta
	Coordinate *coordinate.Coordinate
	Vnodes     []*VnodeStatus
}

// Status returns ring information of this node
func (r *Ring) Status() *Status {
	status := &Status{
		Hostname:   r.config.Hostname,
		HashBits:   r.config.hashBits,
		Coordinate: r.coordClient.GetCoordinate(),
		Vnodes:     make([]*VnodeStatus, len(r.vnodes)),
		Meta:       r.config.Meta,
	}

	for i, vn := range r.vnodes {
		status.Vnodes[i] = vn.Status()
	}

	return status
}
