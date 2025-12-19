package geostore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/golang/geo/s2"
)

const (
	typePoint    byte = 1
	typePolyline byte = 2
	typePolygon  byte = 3
)

// encodeEntry serializes the S2 geometry and properties into a single byte slice.
func encodeEntry(region s2.Region, props []byte) ([]byte, error) {
	var typeByte byte
	var buf bytes.Buffer

	// Encode S2 to buffer first
	switch r := region.(type) {
	case s2.Point:
		typeByte = typePoint
		if err := r.Encode(&buf); err != nil {
			return nil, err
		}
	case *s2.Polyline:
		typeByte = typePolyline
		if err := r.Encode(&buf); err != nil {
			return nil, err
		}
	case *s2.Polygon:
		typeByte = typePolygon
		if err := r.Encode(&buf); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported s2 type for encoding")
	}

	s2Bytes := buf.Bytes()
	s2Len := len(s2Bytes)

	// Final Layout: [Type (1)][S2Len (varint)][S2Bytes][Props]
	// Pre-allocate approximate size
	out := bytes.NewBuffer(make([]byte, 0, 1+5+s2Len+len(props)))

	out.WriteByte(typeByte)

	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, uint64(s2Len))
	out.Write(lenBuf[:n])

	out.Write(s2Bytes)
	out.Write(props)

	return out.Bytes(), nil
}

// decodeEntry parses the binary blob into S2 region and raw properties JSON.
func decodeEntry(data []byte) (s2.Region, []byte, error) {
	if len(data) < 2 {
		return nil, nil, fmt.Errorf("invalid data length")
	}

	typeByte := data[0]

	// Read varint for S2 length
	s2Len, n := binary.Uvarint(data[1:])
	if n <= 0 {
		return nil, nil, fmt.Errorf("invalid s2 length varint")
	}

	s2Start := 1 + n
	s2End := s2Start + int(s2Len)

	if s2End > len(data) {
		return nil, nil, fmt.Errorf("data corrupted: s2 length exceeds buffer")
	}

	s2Data := data[s2Start:s2End]
	propsData := data[s2End:] // Remainder is JSON

	r := bytes.NewReader(s2Data)
	var region s2.Region
	var err error

	switch typeByte {
	case typePoint:
		var p s2.Point
		err = p.Decode(r)
		region = p
	case typePolyline:
		var p s2.Polyline
		err = p.Decode(r)
		region = &p
	case typePolygon:
		var p s2.Polygon
		err = p.Decode(r)
		region = &p
	default:
		return nil, nil, fmt.Errorf("unknown geometry type byte: %d", typeByte)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("s2 decode error: %w", err)
	}

	return region, propsData, nil
}
