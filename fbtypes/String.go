// automatically generated by the FlatBuffers compiler, do not modify

package fbtypes

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type String struct {
	_tab flatbuffers.Table
}

func GetRootAsString(buf []byte, offset flatbuffers.UOffsetT) *String {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &String{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *String) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *String) String() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func StringStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func StringAddString(builder *flatbuffers.Builder, String flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(String), 0)
}
func StringEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}