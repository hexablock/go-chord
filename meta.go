package chord

import (
	"bytes"
	"fmt"
)

// Meta holds metadata for a node
type Meta map[string][]byte

// MarshalBinary marshals Meta to '=' and ',' delimited bytes
func (meta Meta) MarshalBinary() ([]byte, error) {
	lines := make([][]byte, len(meta))

	i := 0
	for k, v := range meta {
		lines[i] = append(append([]byte(k), []byte("=")...), v...)
		i++
	}

	return bytes.Join(lines, []byte(",")), nil
}

func (meta Meta) String() string {
	b, _ := meta.MarshalBinary()
	return string(b)
}

// UnmarshalBinary unmarshals '=' and ',' delimited bytes into Meta
func (meta Meta) UnmarshalBinary(b []byte) error {
	lines := bytes.Split(b, []byte(","))
	for _, line := range lines {
		arr := bytes.Split(line, []byte("="))
		if len(arr) != 2 {
			return fmt.Errorf("invalid data: %s", line)
		}
		meta[string(arr[0])] = arr[1]
	}
	return nil
}
