package geostore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/geo/s2"
)

const (
	typePointVector byte = 1
	typePolyline    byte = 2
	typePolygon     byte = 3
)

// encodeFullEntry encodes properties and the spatial index (including shapes) into a blob.
func encodeFullEntry(shapes []s2.Shape, props []byte) ([]byte, error) {
	var buf bytes.Buffer

	// 1. Properties
	// [LenUvarint][Bytes]
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], uint64(len(props)))
	buf.Write(b[:n])
	buf.Write(props)

	// 2. Shapes
	// [CountUvarint]
	n = binary.PutUvarint(b[:], uint64(len(shapes)))
	buf.Write(b[:n])

	index := s2.NewShapeIndex()

	for _, shape := range shapes {
		// Add to index
		index.Add(shape)

		// Encode shape individually
		// [TypeByte][LenUvarint][Bytes]
		var shapeBuf bytes.Buffer
		var typeByte byte

		switch s := shape.(type) {
		case *s2.PointVector:
			typeByte = typePointVector
			// PointVector doesn't have built-in Encode, manual encoding:
			// [NumPoints][P1][P2]...
			np := uint64(len(*s))
			n := binary.PutUvarint(b[:], np)
			shapeBuf.Write(b[:n])
			for _, pt := range *s {
				if err := pt.Encode(&shapeBuf); err != nil {
					return nil, err
				}
			}
		case *s2.Polyline:
			typeByte = typePolyline
			if err := s.Encode(&shapeBuf); err != nil {
				return nil, err
			}
		case *s2.Polygon:
			typeByte = typePolygon
			if err := s.Encode(&shapeBuf); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported shape type for encoding: %T", shape)
		}

		buf.WriteByte(typeByte)
		n := binary.PutUvarint(b[:], uint64(shapeBuf.Len()))
		buf.Write(b[:n])
		buf.Write(shapeBuf.Bytes())
	}

	// 3. Index
	// Index must be built before encoding
	index.Build()
	if err := index.Encode(&buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// LazyShapeFactory implements s2.ShapeFactory to lazy load shapes from the buffer.
type LazyShapeFactory struct {
	r      *bytes.Reader
	shapes []shapeInfo // offsets and types
}

type shapeInfo struct {
	offset int64
	length int64
	typ    byte
}

func (f *LazyShapeFactory) GetShape(id int) s2.Shape {
	if id < 0 || id >= len(f.shapes) {
		return nil
	}
	info := f.shapes[id]

	// Seek and read
	if _, err := f.r.Seek(info.offset, io.SeekStart); err != nil {
		return nil
	}
	// Limit reader
	lr := io.LimitReader(f.r, info.length)

	switch info.typ {
	case typePointVector:
		// Manual decode
		count, err := binary.ReadUvarint(f.r) // consumes from reader, updates offset
		if err != nil {
			return nil
		}
		// binary.ReadUvarint is a byte reader, using f.r directly is risky if we wrap it.
		// Re-make byte reader approach or just read carefully.
		// Actually f.r is *bytes.Reader, which implements ByteReader.

		pts := make([]s2.Point, count)
		for i := 0; i < int(count); i++ {
			if err := pts[i].Decode(f.r); err != nil {
				return nil
			}
		}
		pv := s2.PointVector(pts)
		return &pv
	case typePolyline:
		var p s2.Polyline
		if err := p.Decode(lr); err != nil {
			return nil
		}
		return &p
	case typePolygon:
		var p s2.Polygon
		if err := p.Decode(lr); err != nil {
			return nil
		}
		return &p
	}
	return nil
}

func (f *LazyShapeFactory) Len() int {
	return len(f.shapes)
}

// decodeFullEntry parses headers and returns properties, the lazy index, and the factory.
func decodeFullEntry(data []byte) ([]byte, *s2.EncodedS2ShapeIndex, *LazyShapeFactory, error) {
	r := bytes.NewReader(data)

	// 1. Properties
	propLen, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, nil, nil, err
	}
	props := make([]byte, propLen)
	if _, err := io.ReadFull(r, props); err != nil {
		return nil, nil, nil, err
	}

	// 2. Shapes Table Scan
	shapeCount, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, nil, nil, err
	}

	infos := make([]shapeInfo, shapeCount)
	for i := 0; i < int(shapeCount); i++ {
		typ, err := r.ReadByte()
		if err != nil {
			return nil, nil, nil, err
		}
		length, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, nil, nil, err
		}
		offset, _ := r.Seek(0, io.SeekCurrent)

		infos[i] = shapeInfo{
			offset: offset,
			length: int64(length),
			typ:    typ,
		}

		// Skip body
		if _, err := r.Seek(int64(length), io.SeekCurrent); err != nil {
			return nil, nil, nil, err
		}
	}

	// 3. Index
	// Reader is now at start of Index
	factory := &LazyShapeFactory{r: bytes.NewReader(data), shapes: infos}

	// Create a new reader from current position for the index init
	// (EncodedS2ShapeIndex.Init wraps in bufio if not ByteReader, bytes.Reader is fine)
	// We need to pass the *remaining* part of stream or just the reader current pos?
	// bytes.Reader tracks position.

	index := s2.NewEncodedS2ShapeIndex()
	if err := index.Init(r, factory); err != nil {
		return nil, nil, nil, fmt.Errorf("index init failed: %w", err)
	}

	return props, index, factory, nil
}
