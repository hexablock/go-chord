// Code generated by protoc-gen-go. DO NOT EDIT.
// source: coordinate/structs.proto

/*
Package coordinate is a generated protocol buffer package.

It is generated from these files:
	coordinate/structs.proto

It has these top-level messages:
	Coordinate
*/
package coordinate

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// Coordinate is a specialized structure for holding network coordinates for the
// Vivaldi-based coordinate mapping algorithm. All of the fields should be public
// to enable this to be serialized. All values in here are in units of seconds.
type Coordinate struct {
	// Vec is the Euclidean portion of the coordinate. This is used along
	// with the other fields to provide an overall distance estimate. The
	// units here are seconds.
	Vec []float64 `protobuf:"fixed64,1,rep,packed,name=Vec" json:"Vec,omitempty"`
	// Err reflects the confidence in the given coordinate and is updated
	// dynamically by the Vivaldi Client. This is dimensionless.
	Error float64 `protobuf:"fixed64,2,opt,name=Error" json:"Error,omitempty"`
	// Adjustment is a distance offset computed based on a calculation over
	// observations from all other nodes over a fixed window and is updated
	// dynamically by the Vivaldi Client. The units here are seconds.
	Adjustment float64 `protobuf:"fixed64,3,opt,name=Adjustment" json:"Adjustment,omitempty"`
	// Height is a distance offset that accounts for non-Euclidean effects
	// which model the access links from nodes to the core Internet. The access
	// links are usually set by bandwidth and congestion, and the core links
	// usually follow distance based on geography.
	Height float64 `protobuf:"fixed64,4,opt,name=Height" json:"Height,omitempty"`
}

func (m *Coordinate) Reset()                    { *m = Coordinate{} }
func (m *Coordinate) String() string            { return proto.CompactTextString(m) }
func (*Coordinate) ProtoMessage()               {}
func (*Coordinate) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Coordinate) GetVec() []float64 {
	if m != nil {
		return m.Vec
	}
	return nil
}

func (m *Coordinate) GetError() float64 {
	if m != nil {
		return m.Error
	}
	return 0
}

func (m *Coordinate) GetAdjustment() float64 {
	if m != nil {
		return m.Adjustment
	}
	return 0
}

func (m *Coordinate) GetHeight() float64 {
	if m != nil {
		return m.Height
	}
	return 0
}

func init() {
	proto.RegisterType((*Coordinate)(nil), "coordinate.Coordinate")
}

func init() { proto.RegisterFile("coordinate/structs.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 135 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x92, 0x48, 0xce, 0xcf, 0x2f,
	0x4a, 0xc9, 0xcc, 0x4b, 0x2c, 0x49, 0xd5, 0x2f, 0x2e, 0x29, 0x2a, 0x4d, 0x2e, 0x29, 0xd6, 0x2b,
	0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x42, 0xc8, 0x28, 0xe5, 0x70, 0x71, 0x39, 0xc3, 0x79, 0x42,
	0x02, 0x5c, 0xcc, 0x61, 0xa9, 0xc9, 0x12, 0x8c, 0x0a, 0xcc, 0x1a, 0x8c, 0x41, 0x20, 0xa6, 0x90,
	0x08, 0x17, 0xab, 0x6b, 0x51, 0x51, 0x7e, 0x91, 0x04, 0x93, 0x02, 0xa3, 0x06, 0x63, 0x10, 0x84,
	0x23, 0x24, 0xc7, 0xc5, 0xe5, 0x98, 0x92, 0x55, 0x5a, 0x5c, 0x92, 0x9b, 0x9a, 0x57, 0x22, 0xc1,
	0x0c, 0x96, 0x42, 0x12, 0x11, 0x12, 0xe3, 0x62, 0xf3, 0x48, 0xcd, 0x4c, 0xcf, 0x28, 0x91, 0x60,
	0x01, 0xcb, 0x41, 0x79, 0x49, 0x6c, 0x60, 0x07, 0x18, 0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0x9d,
	0xa7, 0x3c, 0xb4, 0x9c, 0x00, 0x00, 0x00,
}
