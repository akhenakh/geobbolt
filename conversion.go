package geostore

import (
	"fmt"

	"github.com/golang/geo/s2"
	geom "github.com/peterstace/simplefeatures/geom"
)

// geomToS2 converts a simplefeatures Geometry to an s2.Region.
func geomToS2(g geom.Geometry) (s2.Region, error) {
	switch g.Type() {
	case geom.TypePoint:
		pt := g.MustAsPoint()
		return pointToS2(pt), nil
	case geom.TypeLineString:
		ls := g.MustAsLineString()
		return lineStringToS2(ls), nil
	case geom.TypePolygon:
		poly := g.MustAsPolygon()
		return polygonToS2(poly), nil
	default:
		return nil, fmt.Errorf("unsupported geometry type: %s", g.Type())
	}
}

// s2ToGeom converts an s2.Region back to a simplefeatures Geometry for display.
func s2ToGeom(r s2.Region) (geom.Geometry, error) {
	switch v := r.(type) {
	case s2.Point:
		return s2PointToGeom(v), nil
	case *s2.Polyline:
		return s2PolylineToGeom(v), nil
	case *s2.Polygon:
		return s2PolygonToGeom(v), nil
	default:
		return geom.Geometry{}, fmt.Errorf("unsupported s2 type: %T", r)
	}
}

// --- Forward Converters (GeoJSON -> S2) ---

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
	// Exterior ring
	extRing := poly.ExteriorRing()
	loops := []*s2.Loop{lineStringLoopToS2Loop(extRing)}

	// Interior rings (holes)
	n := poly.NumInteriorRings()
	for i := 0; i < n; i++ {
		loops = append(loops, lineStringLoopToS2Loop(poly.InteriorRingN(i)))
	}
	return s2.PolygonFromOrientedLoops(loops)
}

func lineStringLoopToS2Loop(ls geom.LineString) *s2.Loop {
	seq := ls.Coordinates()
	n := seq.Length()
	// S2 Loops are implicitly closed; remove duplicate last point if present
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

// --- Inverse Converters (S2 -> GeoJSON) ---

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
	// Simplified reconstruction
	var rings []geom.LineString
	for i := 0; i < poly.NumLoops(); i++ {
		loop := poly.Loop(i)
		coords := make([]float64, 0, (loop.NumVertices()+1)*2)
		vertices := loop.Vertices()
		for _, v := range vertices {
			ll := s2.LatLngFromPoint(v)
			coords = append(coords, ll.Lng.Degrees(), ll.Lat.Degrees())
		}
		// Close the ring for GeoJSON
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
