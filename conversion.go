package geostore

import (
	"fmt"

	"github.com/golang/geo/s2"
	geom "github.com/peterstace/simplefeatures/geom"
)

// PointRegion wraps s2.Point to satisfy the s2.Region interface for Indexing.
type PointRegion struct {
	s2.Point
}

func (p PointRegion) CapBound() s2.Cap                  { return s2.CapFromPoint(p.Point) }
func (p PointRegion) RectBound() s2.Rect                { return s2.RectFromLatLng(s2.LatLngFromPoint(p.Point)) }
func (p PointRegion) ContainsCell(c s2.Cell) bool       { return false }
func (p PointRegion) IntersectsCell(c s2.Cell) bool     { return c.ContainsPoint(p.Point) }
func (p PointRegion) ContainsPoint(other s2.Point) bool { return p.Point == other }
func (p PointRegion) CellUnionBound() []s2.CellID       { return p.CapBound().CellUnionBound() }

// MultiPointData holds data for storage only. It does NOT implement s2.Region.
type MultiPointData struct {
	Points []s2.Point
}

// MultiPolylineData holds data for storage only. It does NOT implement s2.Region.
type MultiPolylineData struct {
	Polylines []*s2.Polyline
}

// geomToS2 returns:
// The data object to store (Point, *Polyline, *Polygon, *MultiPointData, etc.)
// A slice of atomic s2.Regions to generate index terms from.
func geomToS2(g geom.Geometry) (interface{}, []s2.Region, error) {
	switch g.Type() {
	case geom.TypePoint:
		pt := pointToS2(g.MustAsPoint())
		// Store as Point, Index as PointRegion
		return pt, []s2.Region{PointRegion{pt}}, nil

	case geom.TypeLineString:
		ls := lineStringToS2(g.MustAsLineString())
		return ls, []s2.Region{ls}, nil

	case geom.TypePolygon:
		poly := polygonToS2(g.MustAsPolygon())
		return poly, []s2.Region{poly}, nil

	case geom.TypeMultiPoint:
		mp := g.MustAsMultiPoint()
		n := mp.NumPoints()
		data := &MultiPointData{Points: make([]s2.Point, n)}
		regions := make([]s2.Region, n)
		for i := 0; i < n; i++ {
			pt := pointToS2(mp.PointN(i))
			data.Points[i] = pt
			regions[i] = PointRegion{pt}
		}
		return data, regions, nil

	case geom.TypeMultiLineString:
		ml := g.MustAsMultiLineString()
		n := ml.NumLineStrings()
		data := &MultiPolylineData{Polylines: make([]*s2.Polyline, n)}
		regions := make([]s2.Region, n)
		for i := 0; i < n; i++ {
			pl := lineStringToS2(ml.LineStringN(i))
			data.Polylines[i] = pl
			regions[i] = pl
		}
		return data, regions, nil

	case geom.TypeMultiPolygon:
		// S2 Polygon handles MultiPolygon topology (multiple loops).
		// We store and index it as a single S2 Polygon.
		poly := multiPolygonToS2(g.MustAsMultiPolygon())
		return poly, []s2.Region{poly}, nil

	default:
		return nil, nil, fmt.Errorf("unsupported geometry type: %s", g.Type())
	}
}

func s2ToGeom(data interface{}) (geom.Geometry, error) {
	switch v := data.(type) {
	case s2.Point:
		return s2PointToGeom(v), nil
	case *s2.Polyline:
		return s2PolylineToGeom(v), nil
	case *s2.Polygon:
		return s2PolygonToGeom(v), nil
	case *MultiPointData:
		return s2MultiPointToGeom(v), nil
	case *MultiPolylineData:
		return s2MultiPolylineToGeom(v), nil
	default:
		return geom.Geometry{}, fmt.Errorf("unsupported storage type: %T", data)
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

func s2MultiPointToGeom(mp *MultiPointData) geom.Geometry {
	pts := make([]geom.Point, len(mp.Points))
	for i, pt := range mp.Points {
		ll := s2.LatLngFromPoint(pt)
		pts[i] = geom.NewPointXY(ll.Lng.Degrees(), ll.Lat.Degrees())
	}
	return geom.NewMultiPoint(pts).AsGeometry()
}

func s2MultiPolylineToGeom(mpl *MultiPolylineData) geom.Geometry {
	var lines []geom.LineString
	for _, pl := range mpl.Polylines {
		g := s2PolylineToGeom(pl)
		lines = append(lines, g.MustAsLineString())
	}
	return geom.NewMultiLineString(lines).AsGeometry()
}
