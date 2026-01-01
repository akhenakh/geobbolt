package geostore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/golang/geo/s2"
)

const (
	typePoint         byte = 1
	typePolyline      byte = 2
	typePolygon       byte = 3
	typeMultiPoint    byte = 4
	typeMultiPolyline byte = 5
)

// encodeEntry accepts s2 objects or *Multi...Data structs
func encodeEntry(data interface{}, props []byte) ([]byte, error) {
	var typeByte byte
	var buf bytes.Buffer

	switch r := data.(type) {
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
	case *MultiPointData:
		typeByte = typeMultiPoint
		nm := uint64(len(r.Points))
		var b [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(b[:], nm)
		buf.Write(b[:n])
		for _, p := range r.Points {
			if err := p.Encode(&buf); err != nil {
				return nil, err
			}
		}
	case *MultiPolylineData:
		typeByte = typeMultiPolyline
		nm := uint64(len(r.Polylines))
		var b [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(b[:], nm)
		buf.Write(b[:n])
		for _, pl := range r.Polylines {
			if err := pl.Encode(&buf); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("unsupported type for encoding: %T", data)
	}

	s2Bytes := buf.Bytes()
	s2Len := len(s2Bytes)

	out := bytes.NewBuffer(make([]byte, 0, 1+5+s2Len+len(props)))

	out.WriteByte(typeByte)
	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, uint64(s2Len))
	out.Write(lenBuf[:n])

	out.Write(s2Bytes)
	out.Write(props)

	return out.Bytes(), nil
}

func decodeEntry(data []byte) (interface{}, []byte, error) {
	if len(data) < 2 {
		return nil, nil, fmt.Errorf("invalid data length")
	}

	typeByte := data[0]

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
	propsData := data[s2End:]

	r := bytes.NewReader(s2Data)
	var geometryData interface{}
	var err error

	switch typeByte {
	case typePoint:
		var p s2.Point
		err = p.Decode(r)
		geometryData = p
	case typePolyline:
		var p s2.Polyline
		err = p.Decode(r)
		geometryData = &p
	case typePolygon:
		var p s2.Polygon
		err = p.Decode(r)
		geometryData = &p
	case typeMultiPoint:
		count, errV := binary.ReadUvarint(r)
		if errV != nil {
			return nil, nil, errV
		}
		pts := make([]s2.Point, count)
		for i := 0; i < int(count); i++ {
			if err := pts[i].Decode(r); err != nil {
				return nil, nil, err
			}
		}
		geometryData = &MultiPointData{Points: pts}
	case typeMultiPolyline:
		count, errV := binary.ReadUvarint(r)
		if errV != nil {
			return nil, nil, errV
		}
		polys := make([]*s2.Polyline, count)
		for i := 0; i < int(count); i++ {
			var p s2.Polyline
			if err := p.Decode(r); err != nil {
				return nil, nil, err
			}
			polys[i] = &p
		}
		geometryData = &MultiPolylineData{Polylines: polys}
	default:
		return nil, nil, fmt.Errorf("unknown geometry type byte: %d", typeByte)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("s2 decode error: %w", err)
	}

	return geometryData, propsData, nil
}
