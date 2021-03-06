package chord

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/hexablock/go-chord/coordinate"
)

var (
	errAllKnownSuccDead      = errors.New("all known successors dead")
	errExhaustedAllPredNodes = errors.New("exhausted all preceeding nodes")
)

// SetMetadata sets the metadata map to a slice of byte slices per the protobuf
func (vn *Vnode) SetMetadata(meta Meta) {
	vn.Meta = make([][]byte, len(meta))
	var i int
	for k, v := range meta {
		vn.Meta[i] = append(append([]byte(k), byte('=')), v...)
		i++
	}
}

// Metadata returns the metadata from a slice of byte slices to a map.
func (vn *Vnode) Metadata() Meta {
	meta := make(Meta)

	for _, m := range vn.Meta {

		arr := bytes.Split(m, []byte("="))
		l := len(arr)

		switch l {
		case 0:
		case 1:
			meta[string(arr[0])] = []byte{}
		case 2:
			meta[string(arr[0])] = arr[1]
		default:
			meta[string(arr[0])] = bytes.Join(arr[1:], []byte("="))
		}

	}

	return meta
}

// MarshalJSON is a custom JSON marshaller
func (vn *Vnode) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID         string
		Host       string
		Region     string
		Zone       string
		Sector     string
		Coordinate *coordinate.Coordinate
		Meta       Meta
	}{
		ID:         hex.EncodeToString(vn.Id),
		Host:       vn.Host,
		Region:     vn.Region,
		Zone:       vn.Zone,
		Sector:     vn.Sector,
		Coordinate: vn.Coordinate,
		Meta:       vn.Metadata(),
	})
}

// StringID converts the ID to a hex encoded string.  As grpc uses String() we use
// StringID() instead.
func (vn *Vnode) StringID() string {
	return fmt.Sprintf("%x", vn.Id)
}

// Initializes a local vnode
func (vn *localVnode) init(idx int) {
	// Generate an ID
	vn.genID(uint16(idx))

	conf := vn.ring.config

	// Set our node contstants
	vn.Host = conf.Hostname
	vn.Region = conf.Region
	vn.Zone = conf.Zone
	vn.Sector = conf.Sector

	// Set binary metadata
	vn.SetMetadata(conf.Meta)

	// Initialize all state
	vn.successors = make([]*Vnode, conf.NumSuccessors)
	vn.finger = make([]*Vnode, conf.hashBits)

	// Register with the RPC mechanism
	vn.ring.transport.Register(&vn.Vnode, vn)
}

// Generates an ID for the node
func (vn *localVnode) genID(idx uint16) {
	// Use the hash funciton
	conf := vn.ring.config
	hash := conf.HashFunc()
	hash.Write([]byte(conf.Hostname))
	binary.Write(hash, binary.BigEndian, idx)

	// Use the hash as the ID
	vn.Id = hash.Sum(nil)
}

// Schedules the Vnode to do regular maintenence
func (vn *localVnode) schedule() {
	// Setup our stabilize timer
	vn.timeLock.Lock()
	//vn.timer = time.AfterFunc(randStabilize(vn.ring.config), vn.stabilize)
	vn.timer = time.AfterFunc(vn.ring.stab.rand(), vn.stabilize)
	vn.timeLock.Unlock()
}

// Called to periodically stabilize the vnode
func (vn *localVnode) stabilize() {
	// QUESTION:
	// The clearing of the timer may need to be re-enabled.  It is currently disabled as it
	// introduces a race condition.  Currently no effects are seen by disabling it.  The
	// corresponding unit test must also be enabled.

	// Clear the timer
	//vn.timer = nil

	// Check for shutdown
	if atomic.LoadInt32(&vn.ring.sigshut) == 1 {
		vn.ring.shutdown <- true
		return
	}

	// Check for new successor
	if err := vn.checkNewSuccessor(); err != nil {
		log.Printf("[ERR] Error checking for new successor: %s", err)
	}

	// Notify the successor
	if err := vn.notifySuccessor(); err != nil {
		log.Printf("[ERR] Error notifying successor: %s", err)
	}

	// Check the predecessor
	if err := vn.checkPredecessor(); err != nil {
		log.Printf("[ERR] Error checking predecessor: %s", err)
	}

	// Fix finger table after fixing node's immediate successor and predecessor
	if err := vn.fixFingerTable(); err != nil {
		log.Printf("[ERR] Error fixing finger table: %s", err)
	}

	// Set the last stabilized time performing any other required ops
	vn.setStabilized()

	// Setup the next stabilize timer
	vn.schedule()
}

// set the last stabilized time safely
func (vn *localVnode) setStabilized() {
	// Set the vnode coordinate cache
	vn.timeLock.Lock()
	vn.stabilized = time.Now()
	vn.timeLock.Unlock()
}

// Checks for a new successor
func (vn *localVnode) checkNewSuccessor() error {
	// Ask our successor for it's predecessor
	trans := vn.ring.transport

	self := vn.Vnode
	self.Coordinate = vn.GetCoordinate()

CHECK_NEW_SUC:
	vn.succLock.RLock()
	succ := vn.successors[0]
	if succ == nil {
		vn.succLock.RUnlock()
		panic("Node has no successor!")
	}

	maybeSuc, err := trans.GetPredecessor(succ)
	if err != nil {
		// Check if we have succ list, try to contact next live succ
		known := vn.knownSuccessors()
		vn.succLock.RUnlock()

		if known > 1 {
			for i := 0; i < known; i++ {
				//
				// TODO: May need a lock on vn.successors
				//
				alive, _, _ := trans.Ping(&self, vn.successors[0])
				if !alive {
					// Don't eliminate the last successor we know of
					if i+1 == known {
						return errAllKnownSuccDead
					}
					//
					// TODO: The copy operation is being picked up by the race detector
					//
					vn.succLock.Lock()
					// Advance the successors list past the dead one
					copy(vn.successors[0:], vn.successors[1:])
					vn.successors[known-1-i] = nil
					vn.succLock.Unlock()

				} else {
					// Found live successor, check for new one
					goto CHECK_NEW_SUC
				}
			}
		}
		return err
	}
	vn.succLock.RUnlock()

	// Check if we should replace our successor
	if maybeSuc != nil && between(vn.Id, succ.Id, maybeSuc.Id) {
		// Check if new successor is alive before switching
		alive, _, err := trans.Ping(&self, maybeSuc)
		if alive && err == nil {
			//
			// TODO: The copy operation is being picked up by the race detector
			//
			vn.succLock.Lock()
			copy(vn.successors[1:], vn.successors[0:len(vn.successors)-1])
			vn.successors[0] = maybeSuc
			vn.succLock.Unlock()

		} else {
			return err
		}
	}
	return nil
}

// RPC: Invoked to return out predecessor
func (vn *localVnode) GetPredecessor() (*Vnode, error) {
	vn.predLock.RLock()
	defer vn.predLock.RUnlock()
	return vn.predecessor, nil
}

// Notifies our successor of us and updates successor list
func (vn *localVnode) notifySuccessor() error {
	maxSucc := vn.ring.config.NumSuccessors

	// Get successor
	vn.succLock.RLock()
	succ := vn.successors[0]
	vn.succLock.RUnlock()

	// Notify successor
	succList, err := vn.ring.transport.Notify(succ, &vn.Vnode)
	if err != nil {
		return err
	}

	// The returned succList from the transport may be our local one so we establish a lock
	// here.
	vn.succLock.Lock()

	// Trim the successors list if too long
	if len(succList) > maxSucc-1 {
		succList = succList[:maxSucc-1]
	}

	// Update local successors list
	for idx, s := range succList {
		// Ensure we don't set ourselves as a successor!
		if s == nil || s.StringID() == vn.StringID() {
			break
		}

		vn.successors[idx+1] = s
	}

	vn.succLock.Unlock()

	return nil
}

// Notify is invoked when a Vnode gets notified
func (vn *localVnode) Notify(maybePred *Vnode) ([]*Vnode, error) {
	vn.predLock.RLock()

	// Check if we should update our predecessor
	if vn.predecessor == nil || between(vn.predecessor.Id, vn.Id, maybePred.Id) {
		// Inform the delegate
		conf := vn.ring.config
		old := vn.predecessor

		vn.predLock.RUnlock()

		vn.ring.invokeDelegate(func(vns ...*Vnode) {
			conf.Delegate.NewPredecessor(vns[0], vns[1], vns[2])
		}, &vn.Vnode, maybePred, old)

		vn.predLock.Lock()
		vn.predecessor = maybePred
		vn.predLock.Unlock()
		// Reset stablize to init to gently backoff
		vn.ring.stab.reset()

	} else {
		vn.predLock.RUnlock()
	}

	// Return our successors list
	return vn.Successors(), nil
}

// Successors returns the current successors in a thread-safe manner.
func (vn *localVnode) Successors() []*Vnode {
	vn.succLock.RLock()
	defer vn.succLock.RUnlock()

	return vn.successors
}

// Fixes up the finger table
func (vn *localVnode) fixFingerTable() error {
	// Determine the offset
	hb := vn.ring.config.hashBits
	offset := powerOffset(vn.Id, vn.lastFinger, hb)

	// Find the successor
	nodes, err := vn.FindSuccessors(1, offset)

	if nodes == nil || len(nodes) == 0 || err != nil {
		return err
	}

	//vn.succLock.RLock()
	//defer vn.succLock.RUnlock()

	vn.succLock.RLock()
	node := nodes[0]
	vn.succLock.RUnlock()

	// Update the finger table
	vn.fingLock.Lock()
	defer vn.fingLock.Unlock()

	vn.finger[vn.lastFinger] = node

	// Try to skip as many finger entries as possible
	for {
		next := vn.lastFinger + 1
		if next >= hb {
			break
		}
		offset := powerOffset(vn.Id, next, hb)

		// While the node is the successor, update the finger entries
		if betweenRightIncl(vn.Id, node.Id, offset) {
			vn.finger[next] = node
			vn.lastFinger = next
		} else {
			break
		}
	}

	// Increment to the index to repair
	if vn.lastFinger+1 == hb {
		vn.lastFinger = 0
	} else {
		vn.lastFinger++
	}

	return nil
}

// Checks the health of our predecessor.  If it fails the predecessor is set to nil
func (vn *localVnode) checkPredecessor() error {
	vn.predLock.RLock()
	if vn.predecessor != nil {
		self := vn.Vnode
		self.Coordinate = vn.GetCoordinate()

		res, coord, err := vn.ring.transport.Ping(&self, vn.predecessor)
		vn.predLock.RUnlock()
		if err != nil {
			return err
		}

		vn.predLock.Lock()
		if !res {
			// Predecessor is dead
			vn.predecessor = nil
		} else {
			// Update coordinates
			vn.predecessor.Coordinate = coord
		}
		vn.predLock.Unlock()

	} else {
		vn.predLock.RUnlock()
	}

	return nil
}

// Finds next N successors. N must be <= NumSuccessors
func (vn *localVnode) FindSuccessors(n int, key []byte) ([]*Vnode, error) {

	vn.succLock.RLock()
	// Check if we are the immediate predecessor
	if betweenRightIncl(vn.Id, vn.successors[0].Id, key) {
		defer vn.succLock.RUnlock()
		return vn.successors[:n], nil
	}
	vn.succLock.RUnlock()

	// Try the closest preceeding nodes
	cp := closestPreceedingVnodeIterator{}
	cp.init(vn, key)
	for {
		// Get the next closest node
		closest := cp.Next()
		if closest == nil {
			break
		}

		// Try that node, break on success
		res, err := vn.ring.transport.FindSuccessors(closest, n, key)
		if err == nil {
			return res, nil
		}
		log.Printf("[ERR] Failed to contact %s/%x. Got %s", closest.Host, closest.Id, err)
	}

	// Check if the ID is between us and any non-immediate successors
	vn.succLock.RLock()
	defer vn.succLock.RUnlock()

	// Determine how many successors we know of
	successors := vn.knownSuccessors()

	for i := 1; i <= successors-n; i++ {
		if betweenRightIncl(vn.Id, vn.successors[i].Id, key) {
			remain := vn.successors[i:]
			if len(remain) > n {
				remain = remain[:n]
			}

			return remain, nil
		}
	}

	// Checked all closer nodes and our successors!
	return nil, errExhaustedAllPredNodes
}

// Used to clear our predecessor when a node is leaving
func (vn *localVnode) ClearPredecessor(p *Vnode) error {
	// Inform the delegate
	conf := vn.ring.config

	vn.predLock.RLock()
	if vn.predecessor != nil && vn.predecessor.StringID() == p.StringID() {
		old := vn.predecessor

		vn.predLock.RUnlock()

		vn.ring.invokeDelegate(func(vns ...*Vnode) {
			conf.Delegate.PredecessorLeaving(vns[0], vns[1])
		}, &vn.Vnode, old)

		vn.predLock.Lock()
		vn.predecessor = nil
		vn.predLock.Unlock()
	} else {
		vn.predLock.RUnlock()
	}

	return nil
}

// Used to skip a successor when a node is leaving
func (vn *localVnode) SkipSuccessor(s *Vnode) error {

	conf := vn.ring.config

	vn.succLock.RLock()
	// Return if we have no match
	if vn.successors[0].StringID() != s.StringID() {
		vn.succLock.RUnlock()
		return nil
	}

	old := vn.successors[0]
	known := vn.knownSuccessors()
	vn.succLock.RUnlock()

	// Inform the delegate
	vn.ring.invokeDelegate(func(vns ...*Vnode) {
		conf.Delegate.SuccessorLeaving(vns[0], vns[1])
	}, &vn.Vnode, old)

	vn.succLock.Lock()
	copy(vn.successors[0:], vn.successors[1:])
	vn.successors[known-1] = nil
	vn.succLock.Unlock()
	return nil
}

// Determine how many successors we know of.  The caller is responsible for obtaining at
// least a read-lock before calling this function
func (vn *localVnode) knownSuccessors() (successors int) {
	for i := 0; i < len(vn.successors); i++ {
		if vn.successors[i] != nil {
			successors = i + 1
		}
	}

	return
}

func (vn *localVnode) Status() *VnodeStatus {
	//vn.timeLock.RLock()
	//defer vn.timeLock.RUnlock()

	return &VnodeStatus{
		ID:         hex.EncodeToString(vn.Id),
		Stabilized: vn.stabilized,
	}
}

// UpdateCoordinate updates the local coordinate state with the remote vnode and
// coordinate provided.  It currently tracks by hostname and returns the new local
// coordinates.
func (vn *localVnode) UpdateCoordinate(remote *Vnode, rtt time.Duration) (coord *coordinate.Coordinate, err error) {

	if remote.Coordinate == nil {
		return nil, fmt.Errorf("coordinate not provided")
	}

	// Dont update if rtt is zero.  This is to support server side updates of
	// remote coordiates
	if rtt > 0 {
		// Update the coordates based on the remote vnode for vivaldi
		coord, err = vn.ring.coordClient.Update(remote.Host, remote.Coordinate, rtt)
		if err != nil {
			return nil, err
		}

		vn.succLock.Lock()
		vn.updateSuccessorCoordinate(remote)
		vn.Vnode.Coordinate = coord
		vn.updateSuccessorCoordinate(&vn.Vnode)
		vn.succLock.Unlock()

	} else {

		vn.succLock.Lock()
		vn.updateSuccessorCoordinate(remote)
		vn.succLock.Unlock()

		coord = vn.GetCoordinate()
	}

	return coord, nil
}

// update all succesors matching the host with the given coords
func (vn *localVnode) updateSuccessorCoordinate(remote *Vnode) {
	for i, succ := range vn.successors {
		if succ == nil {
			continue
		} else if succ.Host == remote.Host {
			// Remote Vnode
			vn.successors[i].Coordinate = remote.Coordinate
		}

	}
}

// GetCoordinate returns the vivaldi coordinates for this Vnode.  All vnodes on given node will have
// the same coordinates.
func (vn *localVnode) GetCoordinate() *coordinate.Coordinate {
	return vn.ring.GetCoordinate()
}

// Instructs the vnode to leave
func (vn *localVnode) leave() error {
	var (
		conf  = vn.ring.config
		trans = vn.ring.transport
		err   error
	)

	vn.succLock.RLock()
	defer vn.succLock.RUnlock()

	// Inform the delegate we are leaving
	pred := vn.predecessor
	succ := vn.successors[0]
	vn.ring.invokeDelegate(func(vns ...*Vnode) {
		conf.Delegate.Leaving(vns[0], vns[1], vns[2])
	}, &vn.Vnode, pred, succ)

	// Notify predecessor to advance to their next successor
	if vn.predecessor != nil {
		err = trans.SkipSuccessor(vn.predecessor, &vn.Vnode)
	}

	// Notify successor to clear old predecessor
	err = mergeErrors(err, trans.ClearPredecessor(vn.successors[0], &vn.Vnode))
	return err
}
