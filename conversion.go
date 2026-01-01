package geostore

import (
	"fmt"

	"github.com/golang/geo/s2"
	geom "github.com/peterstace/simplefeatures/geom"
)

// PointRegion wraps s2.Point to satisfy the s2.Region interface for Indexing (Term generation).
type PointRegion struct {
	s2.Point
}

func (p PointRegion) CapBound() s2.Cap                  { return s2.CapFromPoint(p.Point) }
func (p PointRegion) RectBound() s2.Rect                { return s2.RectFromLatLng(s2.LatLngFromPoint(p.Point)) }
func (p PointRegion) ContainsCell(c s2.Cell) bool       { return false }
func (p PointRegion) IntersectsCell(c s2.Cell) bool     { return c.ContainsPoint(p.Point) }
func (p PointRegion) ContainsPoint(other s2.Point) bool { return p.Point == other }
func (p PointRegion) CellUnionBound() []s2.CellID       { return p.CapBound().CellUnionBound() }

// geomToS2 converts geometry to:
// 1. A slice of s2.Shape (for the ShapeIndex)
// 2. A slice of s2.Region (for TermIndexing)
func geomToS2(g geom.Geometry) ([]s2.Shape, []s2.Region, error) {
	switch g.Type() {
	case geom.TypePoint:
		pt := pointToS2(g.MustAsPoint())
		// PointVector implements Shape
		pv := s2.PointVector{pt}
		return []s2.Shape{&pv}, []s2.Region{PointRegion{pt}}, nil

	case geom.TypeLineString:
		ls := lineStringToS2(g.MustAsLineString())
		return []s2.Shape{ls}, []s2.Region{ls}, nil

	case geom.TypePolygon:
		poly := polygonToS2(g.MustAsPolygon())
		return []s2.Shape{poly}, []s2.Region{poly}, nil

	case geom.TypeMultiPoint:
		mp := g.MustAsMultiPoint()
		n := mp.NumPoints()
		pts := make(s2.PointVector, n)
		regions := make([]s2.Region, n)
		for i := 0; i < n; i++ {
			pt := pointToS2(mp.PointN(i))
			pts[i] = pt
			regions[i] = PointRegion{pt}
		}
		// Single shape for all points
		return []s2.Shape{&pts}, regions, nil

	case geom.TypeMultiLineString:
		ml := g.MustAsMultiLineString()
		n := ml.NumLineStrings()
		shapes := make([]s2.Shape, n)
		regions := make([]s2.Region, n)
		for i := 0; i < n; i++ {
			pl := lineStringToS2(ml.LineStringN(i))
			shapes[i] = pl
			regions[i] = pl
		}
		return shapes, regions, nil

	case geom.TypeMultiPolygon:
		poly := multiPolygonToS2(g.MustAsMultiPolygon())
		return []s2.Shape{poly}, []s2.Region{poly}, nil

	default:
		return nil, nil, fmt.Errorf("unsupported geometry type: %s", g.Type())
	}
}

// shapesToGeom reconstructs geometry from a slice of shapes.
// This is approximate as we lose the distinction between MultiPolygon and Polygon in S2,
// but sufficient for returning results.
func shapesToGeom(shapes []s2.Shape) geom.Geometry {
	if len(shapes) == 0 {
		return geom.Geometry{}
	}

	// Handle single shape cases common in simple features
	if len(shapes) == 1 {
		return shapeToGeom(shapes[0])
	}

	// Multiple shapes imply MultiLineString (from our conversion logic above).
	// Points are combined into one PointVector, Polygons into one S2Polygon.
	// So multiple shapes here usually means multiple Polylines.
	var lines []geom.LineString
	for _, s := range shapes {
		g := shapeToGeom(s)
		if g.IsLineString() {
			lines = append(lines, g.MustAsLineString())
		}
	}
	if len(lines) > 0 {
		return geom.NewMultiLineString(lines).AsGeometry()
	}

	// Fallback return first
	return shapeToGeom(shapes[0])
}

func shapeToGeom(s s2.Shape) geom.Geometry {
	switch v := s.(type) {
	case *s2.PointVector:
		if len(*v) == 1 {
			return s2PointToGeom((*v)[0])
		}
		return s2MultiPointToGeom(v)
	case *s2.Polyline:
		return s2PolylineToGeom(v)
	case *s2.Polygon:
		return s2PolygonToGeom(v)
	default:
		// Attempt fallback by dimension?
		return geom.Geometry{}
	}
}

func pointToS2(pt geom.Point) s2.Point {
	xy, _ := pt.XY()
	return s2.PointFromLatLng(s2.LatLngFromDegrees(xy.Y, xy.X))
}

func lineStringToS2(ls geom.LineString) *s2.Polyline {
	seq := ls.Coordinates()
	n := seq.Length()
	pts := make([]s2.Point, n)
	for i := 0; i < n; i++ {
		xy := seq.GetXY(i)
		pts[i] = s2.PointFromLatLng(s2.LatLngFromDegrees(xy.Y, xy.X))
	}
	poly := s2.Polyline(pts)
	return &poly
}

func polygonToS2(poly geom.Polygon) *s2.Polygon {
	var loops []*s2.Loop
	loops = append(loops, lineStringLoopToS2Loop(poly.ExteriorRing()))
	n := poly.NumInteriorRings()
	for i := 0; i < n; i++ {
		loops = append(loops, lineStringLoopToS2Loop(poly.InteriorRingN(i)))
	}
	return s2.PolygonFromOrientedLoops(loops)
}

func multiPolygonToS2(mp geom.MultiPolygon) *s2.Polygon {
	var loops []*s2.Loop
	n := mp.NumPolygons()
	for i := 0; i < n; i++ {
		poly := mp.PolygonN(i)
		loops = append(loops, lineStringLoopToS2Loop(poly.ExteriorRing()))
		numHoles := poly.NumInteriorRings()
		for j := 0; j < numHoles; j++ {
			loops = append(loops, lineStringLoopToS2Loop(poly.InteriorRingN(j)))
		}
	}
	return s2.PolygonFromOrientedLoops(loops)
}

func lineStringLoopToS2Loop(ls geom.LineString) *s2.Loop {
	seq := ls.Coordinates()
	n := seq.Length()
	if n > 0 {
		first := seq.GetXY(0)
		last := seq.GetXY(n - 1)
		if first == last {
			n--
		}
	}
	pts := make([]s2.Point, n)
	for i := 0; i < n; i++ {
		xy := seq.GetXY(i)
		pts[i] = s2.PointFromLatLng(s2.LatLngFromDegrees(xy.Y, xy.X))
	}
	return s2.LoopFromPoints(pts)
}

// --- Inverse Helpers ---

func s2PointToGeom(pt s2.Point) geom.Geometry {
	ll := s2.LatLngFromPoint(pt)
	return geom.NewPointXY(ll.Lng.Degrees(), ll.Lat.Degrees()).AsGeometry()
}

func s2MultiPointToGeom(pv *s2.PointVector) geom.Geometry {
	pts := make([]geom.Point, len(*pv))
	for i, pt := range *pv {
		ll := s2.LatLngFromPoint(pt)
		pts[i] = geom.NewPointXY(ll.Lng.Degrees(), ll.Lat.Degrees())
	}
	return geom.NewMultiPoint(pts).AsGeometry()
}

func s2PolylineToGeom(pl *s2.Polyline) geom.Geometry {
	coords := make([]float64, 0, len(*pl)*2)
	for _, pt := range *pl {
		ll := s2.LatLngFromPoint(pt)
		coords = append(coords, ll.Lng.Degrees(), ll.Lat.Degrees())
	}
	seq := geom.NewSequence(coords, geom.DimXY)
	return geom.NewLineString(seq).AsGeometry()
}

func s2PolygonToGeom(poly *s2.Polygon) geom.Geometry {
	var rings []geom.LineString
	for i := 0; i < poly.NumLoops(); i++ {
		loop := poly.Loop(i)
		coords := make([]float64, 0, (loop.NumVertices()+1)*2)
		vertices := loop.Vertices()
		for _, v := range vertices {
			ll := s2.LatLngFromPoint(v)
			coords = append(coords, ll.Lng.Degrees(), ll.Lat.Degrees())
		}
		if len(vertices) > 0 {
			ll := s2.LatLngFromPoint(vertices[0])
			coords = append(coords, ll.Lng.Degrees(), ll.Lat.Degrees())
		}
		seq := geom.NewSequence(coords, geom.DimXY)
		rings = append(rings, geom.NewLineString(seq))
	}
	if len(rings) == 0 {
		return geom.NewPolygon(nil).AsGeometry()
	}
	return geom.NewPolygon(rings).AsGeometry()
}
