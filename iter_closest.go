package chord

import (
	"math/big"
)

type closestPreceedingVnodeIterator struct {
	key          []byte
	vn           *localVnode
	fingerIdx    int
	successorIdx int
	yielded      map[string]struct{}
}

func (cp *closestPreceedingVnodeIterator) init(vn *localVnode, key []byte) {
	cp.key = key
	cp.vn = vn
	cp.successorIdx = len(vn.successors) - 1
	cp.fingerIdx = len(vn.finger) - 1
	cp.yielded = make(map[string]struct{})
}

func (cp *closestPreceedingVnodeIterator) Next() *Vnode {
	// Try to find each node
	var successorNode *Vnode
	var fingerNode *Vnode

	// Scan to find the next successor
	vn := cp.vn
	var i int

	vn.succLock.RLock()
	for i = cp.successorIdx; i >= 0; i-- {
		if vn.successors[i] == nil {
			continue
		}
		if _, ok := cp.yielded[vn.successors[i].StringID()]; ok {
			continue
		}
		if between(vn.Id, cp.key, vn.successors[i].Id) {
			successorNode = vn.successors[i]
			break
		}
	}
	vn.succLock.RUnlock()
	cp.successorIdx = i

	// Scan to find the next finger
	vn.fingLock.RLock()
	for i = cp.fingerIdx; i >= 0; i-- {
		if vn.finger[i] == nil {
			continue
		}
		if _, ok := cp.yielded[vn.finger[i].StringID()]; ok {
			continue
		}
		if between(vn.Id, cp.key, vn.finger[i].Id) {
			fingerNode = vn.finger[i]
			break
		}
	}
	vn.fingLock.RUnlock()
	cp.fingerIdx = i

	// Determine which node is better
	if successorNode != nil && fingerNode != nil {
		// Determine the closer node
		hb := cp.vn.ring.config.hashBits
		closest := closestPreceedingVnode(successorNode, fingerNode, cp.key, hb)
		if closest == successorNode {
			cp.successorIdx--
		} else {
			cp.fingerIdx--
		}
		cp.yielded[closest.StringID()] = struct{}{}
		return closest

	} else if successorNode != nil {
		cp.successorIdx--
		cp.yielded[successorNode.StringID()] = struct{}{}
		return successorNode

	} else if fingerNode != nil {
		cp.fingerIdx--
		cp.yielded[fingerNode.StringID()] = struct{}{}
		return fingerNode
	}

	return nil
}

// Returns the closest preceeding Vnode to the key
func closestPreceedingVnode(a, b *Vnode, key []byte, bits int) *Vnode {
	adist := distance(a.Id, key, bits)
	bdist := distance(b.Id, key, bits)
	if adist.Cmp(bdist) <= 0 {
		return a
	}
	return b
}

// Computes the forward distance from a to b modulus a ring size
func distance(a, b []byte, bits int) *big.Int {
	// Get the ring size
	var ring big.Int
	ring.Exp(big.NewInt(2), big.NewInt(int64(bits)), nil)

	// Convert to int
	var aint, bint big.Int
	(&aint).SetBytes(a)
	(&bint).SetBytes(b)

	// Compute the distances
	var dist big.Int
	(&dist).Sub(&bint, &aint)

	// Distance modulus ring size
	(&dist).Mod(&dist, &ring)
	return &dist
}
