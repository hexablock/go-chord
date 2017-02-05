package chord

import (
	"fmt"
	"log"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/ipkg/go-chord/fbtypes"
)

// PayloadCodec is custom grpc codec used to marshal and unmarshal flatbuffers
type PayloadCodec struct{}

func (cb *PayloadCodec) Marshal(v interface{}) ([]byte, error) {
	p, ok := v.(*Payload)
	if !ok {
		log.Fatalf("invalid type: %+v", v)
	}
	return p.Data, nil
}

func (cb *PayloadCodec) Unmarshal(data []byte, v interface{}) error {
	p, ok := v.(*Payload)
	if !ok {
		log.Fatalf("invalid type: %+v", v)
	}
	p.Data = data
	return nil
}

func (cb *PayloadCodec) String() string {
	return "chord.PayloadCodec"
}

func serializeVnodePairToPayload(target, self *Vnode) *Payload {
	fb := flatbuffers.NewBuilder(0)
	tp := serializeVnode(fb, target)
	sp := serializeVnode(fb, self)

	fbtypes.VnodePairStart(fb)
	fbtypes.VnodePairAddTarget(fb, tp)
	fbtypes.VnodePairAddSelf(fb, sp)
	ep := fbtypes.VnodePairEnd(fb)
	fb.Finish(ep)
	return &Payload{Data: fb.Bytes[fb.Head():]}
}

func deserializeVnodePair(data []byte) (*Vnode, *Vnode) {
	vp := fbtypes.GetRootAsVnodePair(data, 0)
	t := vp.Target(nil)
	s := vp.Self(nil)

	return &Vnode{Id: t.IdBytes(), Host: string(t.Host())}, &Vnode{Id: s.IdBytes(), Host: string(s.Host())}
}

func serializeVnode(fb *flatbuffers.Builder, vn *Vnode) flatbuffers.UOffsetT {
	ip := fb.CreateByteString(vn.Id)
	hp := fb.CreateString(vn.Host)

	fbtypes.VnodeStart(fb)
	fbtypes.VnodeAddId(fb, ip)
	fbtypes.VnodeAddHost(fb, hp)
	return fbtypes.VnodeEnd(fb)
}

func SerializeVnodeErr(vn *Vnode, err error) []byte {
	fb := flatbuffers.NewBuilder(0)
	if err != nil {
		ep := fb.CreateString(err.Error())
		fbtypes.VnodeErrStart(fb)
		fbtypes.VnodeErrAddErr(fb, ep)
	} else if vn != nil {
		of := serializeVnode(fb, vn)
		fbtypes.VnodeErrStart(fb)
		fbtypes.VnodeErrAddVnode(fb, of)
	} else {
		fbtypes.VnodeErrStart(fb)
	}

	p := fbtypes.VnodeErrEnd(fb)
	fb.Finish(p)
	return fb.Bytes[fb.Head():]
}

func DeserializeVnodeErr(data []byte) (*Vnode, error) {
	obj := fbtypes.GetRootAsVnodeErr(data, 0)
	if err := obj.Err(); err != nil && len(err) > 0 {
		return nil, fmt.Errorf("%s", err)
	}

	if vn := obj.Vnode(nil); vn != nil {
		return &Vnode{Id: vn.IdBytes(), Host: string(vn.Host())}, nil
	}
	return nil, nil
}

func serializeVnodeToPayload(vn *Vnode) *Payload {
	fb := flatbuffers.NewBuilder(0)
	ofs := serializeVnode(fb, vn)
	fb.Finish(ofs)
	return &Payload{Data: fb.Bytes[fb.Head():]}
}

func serializeString(fb *flatbuffers.Builder, str string) flatbuffers.UOffsetT {
	sp := fb.CreateString(str)
	fbtypes.StringStart(fb)
	fbtypes.StringAddString(fb, sp)
	return fbtypes.StringEnd(fb)
}

func serializeStringToPayload(str string) *Payload {
	fb := flatbuffers.NewBuilder(0)
	ofs := serializeString(fb, str)
	fb.Finish(ofs)
	return &Payload{Data: fb.Bytes[fb.Head():]}
}

// SerializeVnodeListErr serializes a vnode list and error
func SerializeVnodeListErr(list []*Vnode, err error) []byte {
	fb := flatbuffers.NewBuilder(0)
	if err != nil {
		ep := fb.CreateString(err.Error())
		fbtypes.VnodeListErrStart(fb)
		fbtypes.VnodeListErrAddErr(fb, ep)
	} else {
		ofs := make([]flatbuffers.UOffsetT, len(list))

		for i, vn := range list {
			ip := fb.CreateByteString(vn.Id)
			hp := fb.CreateString(vn.Host)
			fbtypes.VnodeStart(fb)
			fbtypes.VnodeAddId(fb, ip)
			fbtypes.VnodeAddHost(fb, hp)
			ofs[i] = fbtypes.VnodeEnd(fb)
		}

		fbtypes.VnodeListErrStartVnodesVector(fb, len(list))
		for _, o := range ofs {
			fb.PrependUOffsetT(o)
		}
		idsVec := fb.EndVector(len(list))

		fbtypes.VnodeListErrStart(fb)
		fbtypes.VnodeListErrAddVnodes(fb, idsVec)
	}
	p := fbtypes.VnodeListErrEnd(fb)
	fb.Finish(p)

	return fb.Bytes[fb.Head():]
}

// DeserializeVnodeListErr de-serializes data into a vnode slice and / or error
func DeserializeVnodeListErr(data []byte) ([]*Vnode, error) {
	obj := fbtypes.GetRootAsVnodeListErr(data, 0)
	if e := obj.Err(); e != nil && len(e) > 0 {
		return nil, fmt.Errorf("%s", e)
	}

	l := obj.VnodesLength()
	out := make([]*Vnode, l)

	for i := 0; i < l; i++ {
		var vo fbtypes.Vnode
		obj.Vnodes(&vo, i)
		// deserialize in the opposite order
		out[l-i-1] = &Vnode{Id: vo.IdBytes(), Host: string(vo.Host())}
	}

	return out, nil
}

// 1 = true
// 0 = false
func serializeBoolErrToPayload(b bool, e error) *Payload {
	fb := flatbuffers.NewBuilder(0)
	if e != nil {
		ep := fb.CreateString(e.Error())
		fbtypes.BoolErrStart(fb)
		fbtypes.BoolErrAddErr(fb, ep)
	} else {
		fbtypes.BoolErrStart(fb)
		if b {
			fbtypes.BoolErrAddBool(fb, byte(1))
		} else {
			fbtypes.BoolErrAddBool(fb, byte(0))
		}
	}

	p := fbtypes.BoolErrEnd(fb)
	fb.Finish(p)
	return &Payload{Data: fb.Bytes[fb.Head():]}
}

// 1 = true
// 0 = false
func deserializeBoolErr(data []byte) (bool, error) {
	be := fbtypes.GetRootAsBoolErr(data, 0)
	if e := be.Err(); e != nil && len(e) > 0 {
		return false, fmt.Errorf("%s", e)
	}
	return be.Bool() == byte(1), nil
}

func deserializeVnode(data []byte) *Vnode {
	obj := fbtypes.GetRootAsVnode(data, 0)
	return &Vnode{Id: obj.IdBytes(), Host: string(obj.Host())}
}

func deserializeErr(data []byte) error {
	e := fbtypes.GetRootAsString(data, 0)
	if s := e.String(); s != nil && len(s) > 0 {
		return fmt.Errorf("%s", s)
	}
	return nil
}

func serializeVnodeIntBytesToPayload(vn *Vnode, n int, b []byte) *Payload {
	fb := flatbuffers.NewBuilder(0)

	bp := fb.CreateByteString(b)
	ofs := serializeVnode(fb, vn)

	fbtypes.VnodeIntBytesStart(fb)
	fbtypes.VnodeIntBytesAddVnode(fb, ofs)
	fbtypes.VnodeIntBytesAddInt(fb, int32(n))
	fbtypes.VnodeIntBytesAddBytes(fb, bp)
	ep := fbtypes.VnodeIntBytesEnd(fb)
	fb.Finish(ep)
	return &Payload{Data: fb.Bytes[fb.Head():]}
}

func deserializeVnodeIntBytes(buf []byte) (*Vnode, int, []byte) {
	vib := fbtypes.GetRootAsVnodeIntBytes(buf, 0)
	obj := vib.Vnode(nil)
	return &Vnode{Id: obj.IdBytes(), Host: string(obj.Host())}, int(vib.Int()), vib.BytesBytes()
}
