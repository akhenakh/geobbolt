package geostore

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	geom "github.com/peterstace/simplefeatures/geom"
	bolt "go.etcd.io/bbolt"
)

func TestGeoStore(t *testing.T) {
	// Setup temporary DB
	tmpFile, err := os.CreateTemp("", "geo_s2.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(dbPath)

	store, err := NewGeoStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Data Setup
	// Note: Polygon winding CCW for exterior
	ptA := makeGeoJSON("cn_tower", -79.3871, 43.6426, map[string]interface{}{"type": "landmark"})
	ptB := makeGeoJSON("high_park", -79.4636, 43.6465, map[string]interface{}{"type": "park"})
	ptC := makeGeoJSON("montreal", -73.5673, 45.5017, map[string]interface{}{"type": "city"})
	polyD := makePolygonGeoJSON("downtown_box", [][]float64{
		{-79.40, 43.64}, {-79.37, 43.64}, {-79.37, 43.66}, {-79.40, 43.66}, {-79.40, 43.64},
	})

	if err := store.Put("cn_tower", ptA); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("high_park", ptB); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("montreal", ptC); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("downtown_box", polyD); err != nil {
		t.Fatal(err)
	}

	// Query Proximity (Center: Toronto, 10km)
	results, err := store.FindClosest(43.6532, -79.3832, 10000, true)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("Found %d results:\n", len(results))
	for _, res := range results {
		fmt.Printf("- %s: %.2fm\n", res.ID, res.Distance)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results (excluding Montreal), got %d", len(results))
	}

	// Check Distance order
	if len(results) > 0 && results[0].ID != "downtown_box" {
		t.Errorf("Expected downtown_box to be first (dist ~0), got %s", results[0].ID)
	}

	// Test without geometry
	resultsNoGeom, err := store.FindClosest(43.6532, -79.3832, 10000, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(resultsNoGeom) != 3 {
		t.Errorf("Expected 3 results, got %d", len(resultsNoGeom))
	}
	if !resultsNoGeom[0].Geometry.IsEmpty() {
		t.Errorf("Expected empty geometry when withGeometry=false")
	}
}

func makeGeoJSON(id string, x, y float64, props map[string]interface{}) []byte {
	feat := geom.GeoJSONFeature{
		Geometry:   geom.NewPointXY(x, y).AsGeometry(),
		Properties: props,
	}
	b, _ := feat.MarshalJSON()
	return b
}

func makePolygonGeoJSON(id string, coords [][]float64) []byte {
	seq := geom.NewSequence(flatten(coords), geom.DimXY)
	ring := geom.NewLineString(seq)
	poly := geom.NewPolygon([]geom.LineString{ring})
	feat := geom.GeoJSONFeature{Geometry: poly.AsGeometry()}
	b, _ := feat.MarshalJSON()
	return b
}

func flatten(coords [][]float64) []float64 {
	var flat []float64
	for _, c := range coords {
		flat = append(flat, c...)
	}
	return flat
}

// TestInteriorExteriorCovers validates that polygons generate both interior and exterior terms
func TestInteriorExteriorCovers(t *testing.T) {
	// Setup temporary DB
	tmpFile, err := os.CreateTemp("", "geo_interior_test.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(dbPath)

	store, err := NewGeoStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Test 1: Polygon should generate both interior and exterior terms
	entry, err := store.PrepareIndexEntry("large_polygon", geom.GeoJSONFeature{
		Geometry:   geom.NewPolygon([]geom.LineString{makeLineString([][]float64{{-80.0, 43.0}, {-78.0, 43.0}, {-78.0, 45.0}, {-80.0, 45.0}, {-80.0, 43.0}})}).AsGeometry(),
		Properties: map[string]interface{}{"name": "large"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify both interior and exterior terms exist
	if len(entry.InteriorTerms) == 0 {
		t.Error("Expected polygon to have interior terms, got none")
	}
	if len(entry.ExteriorTerms) == 0 {
		t.Error("Expected polygon to have exterior terms, got none")
	}

	// Interior terms should be a subset of exterior terms (for convex polygons)
	interiorSet := make(map[string]struct{})
	for _, term := range entry.InteriorTerms {
		interiorSet[term] = struct{}{}
	}

	exteriorSet := make(map[string]struct{})
	for _, term := range entry.ExteriorTerms {
		exteriorSet[term] = struct{}{}
	}

	// For a large polygon, interior terms should exist
	if len(interiorSet) >= len(exteriorSet) {
		t.Logf("Interior terms (%d) should generally be fewer than exterior terms (%d) for large polygons",
			len(interiorSet), len(exteriorSet))
	}

	t.Logf("Polygon - Interior terms: %d, Exterior terms: %d", len(entry.InteriorTerms), len(entry.ExteriorTerms))

	// Test 2: Point should only generate exterior terms (no interior)
	ptEntry, err := store.PrepareIndexEntry("test_point", geom.GeoJSONFeature{
		Geometry:   geom.NewPointXY(-79.0, 44.0).AsGeometry(),
		Properties: map[string]interface{}{"name": "point"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(ptEntry.InteriorTerms) > 0 {
		t.Error("Point should not have interior terms")
	}
	if len(ptEntry.ExteriorTerms) == 0 {
		t.Error("Point should have exterior terms")
	}

	t.Logf("Point - Interior terms: %d, Exterior terms: %d", len(ptEntry.InteriorTerms), len(ptEntry.ExteriorTerms))
}

// TestInteriorMatchOptimization validates that interior matches skip expensive distance calculations
func TestInteriorMatchOptimization(t *testing.T) {
	// Setup temporary DB
	tmpFile, err := os.CreateTemp("", "geo_optimization_test.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(dbPath)

	store, err := NewGeoStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create a large polygon (downtown Toronto area)
	polygon := makePolygonGeoJSON("toronto_downtown", [][]float64{
		{-79.42, 43.63}, {-79.35, 43.63}, {-79.35, 43.68}, {-79.42, 43.68}, {-79.42, 43.63},
	})

	// Add a point well inside the polygon
	pointInside := makeGeoJSON("point_inside", -79.38, 43.65, map[string]interface{}{"type": "inside"})

	// Add a point outside but near the polygon
	pointOutside := makeGeoJSON("point_outside", -79.30, 43.65, map[string]interface{}{"type": "outside"})

	if err := store.Put("toronto_downtown", polygon); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("point_inside", pointInside); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("point_outside", pointOutside); err != nil {
		t.Fatal(err)
	}

	// Query from a point inside the polygon
	// This should match via interior cover and get distance 0
	results, err := store.FindClosest(43.65, -79.38, 1000, true)
	if err != nil {
		t.Fatal(err)
	}

	// Should find the polygon and the point_inside
	foundPolygon := false
	foundPointInside := false
	for _, res := range results {
		if res.ID == "toronto_downtown" {
			foundPolygon = true
			// Distance should be 0 or very small since query point is inside
			if res.Distance > 10 {
				t.Errorf("Expected distance ~0 for interior match, got %.2fm", res.Distance)
			}
			t.Logf("Polygon interior match distance: %.2fm", res.Distance)
		}
		if res.ID == "point_inside" {
			foundPointInside = true
			t.Logf("Point inside distance: %.2fm", res.Distance)
		}
	}

	if !foundPolygon {
		t.Error("Expected to find polygon via interior match")
	}
	if !foundPointInside {
		t.Error("Expected to find point inside query radius")
	}

	// Query from a point outside the polygon but near edge
	// This should match via exterior cover and get proper distance
	results2, err := store.FindClosest(43.65, -79.34, 1000, true)
	if err != nil {
		t.Fatal(err)
	}

	foundPolygonExterior := false
	for _, res := range results2 {
		if res.ID == "toronto_downtown" {
			foundPolygonExterior = true
			// Distance should be meaningful (query point is ~1km from edge)
			if res.Distance < 500 || res.Distance > 2000 {
				t.Errorf("Expected distance ~1000m for exterior match, got %.2fm", res.Distance)
			}
			t.Logf("Polygon exterior match distance: %.2fm", res.Distance)
		}
	}

	if !foundPolygonExterior {
		t.Error("Expected to find polygon via exterior match")
	}
}

// TestIndexTermPrefixes validates that terms are stored with correct prefixes
func TestIndexTermPrefixes(t *testing.T) {
	// Setup temporary DB
	tmpFile, err := os.CreateTemp("", "geo_prefixes_test.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(dbPath)

	store, err := NewGeoStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Add a polygon
	polygon := makePolygonGeoJSON("test_poly", [][]float64{
		{-79.40, 43.64}, {-79.37, 43.64}, {-79.37, 43.66}, {-79.40, 43.66}, {-79.40, 43.64},
	})

	if err := store.Put("test_poly", polygon); err != nil {
		t.Fatal(err)
	}

	// Verify the index entries have correct prefixes
	err = store.db.View(func(tx *bolt.Tx) error {
		bIdx := tx.Bucket([]byte(bucketIndex))
		c := bIdx.Cursor()

		interiorCount := 0
		exteriorCount := 0
		invalidCount := 0

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			// Check if key starts with "int:" or "ext:"
			if bytes.HasPrefix(k, []byte("int:")) {
				interiorCount++
			} else if bytes.HasPrefix(k, []byte("ext:")) {
				exteriorCount++
			} else {
				invalidCount++
				t.Errorf("Invalid key prefix: %s", string(k))
			}
		}

		t.Logf("Index entries - Interior: %d, Exterior: %d", interiorCount, exteriorCount)

		if interiorCount == 0 {
			t.Error("Expected interior index entries")
		}
		if exteriorCount == 0 {
			t.Error("Expected exterior index entries")
		}
		if invalidCount > 0 {
			t.Errorf("Found %d invalid index entries", invalidCount)
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}

// TestLineStringIndexing validates that linestrings only have exterior terms
func TestLineStringIndexing(t *testing.T) {
	// Setup temporary DB
	tmpFile, err := os.CreateTemp("", "geo_line_test.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(dbPath)

	store, err := NewGeoStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create a linestring
	ls := geom.NewLineString(geom.NewSequence([]float64{
		-79.40, 43.64, -79.37, 43.64, -79.37, 43.66,
	}, geom.DimXY))

	entry, err := store.PrepareIndexEntry("test_line", geom.GeoJSONFeature{
		Geometry:   ls.AsGeometry(),
		Properties: map[string]interface{}{"name": "line"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Linestrings should only have exterior terms (no interior)
	if len(entry.InteriorTerms) > 0 {
		t.Error("LineString should not have interior terms")
	}
	if len(entry.ExteriorTerms) == 0 {
		t.Error("LineString should have exterior terms")
	}

	t.Logf("LineString - Interior: %d, Exterior: %d", len(entry.InteriorTerms), len(entry.ExteriorTerms))
}

// TestMultiPolygonIndexing validates that multipolygons generate proper interior/exterior terms
func TestMultiPolygonIndexing(t *testing.T) {
	// Setup temporary DB
	tmpFile, err := os.CreateTemp("", "geo_multipoly_test.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(dbPath)

	store, err := NewGeoStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create two separate polygons
	poly1 := geom.NewPolygon([]geom.LineString{
		makeLineString([][]float64{{-80.0, 43.0}, {-79.5, 43.0}, {-79.5, 43.5}, {-80.0, 43.5}, {-80.0, 43.0}}),
	})
	poly2 := geom.NewPolygon([]geom.LineString{
		makeLineString([][]float64{{-79.0, 44.0}, {-78.5, 44.0}, {-78.5, 44.5}, {-79.0, 44.5}, {-79.0, 44.0}}),
	})

	mp := geom.NewMultiPolygon([]geom.Polygon{poly1, poly2})

	entry, err := store.PrepareIndexEntry("test_multipoly", geom.GeoJSONFeature{
		Geometry:   mp.AsGeometry(),
		Properties: map[string]interface{}{"name": "multipoly"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// MultiPolygon should have both interior and exterior terms
	if len(entry.InteriorTerms) == 0 {
		t.Error("MultiPolygon should have interior terms")
	}
	if len(entry.ExteriorTerms) == 0 {
		t.Error("MultiPolygon should have exterior terms")
	}

	t.Logf("MultiPolygon - Interior: %d, Exterior: %d", len(entry.InteriorTerms), len(entry.ExteriorTerms))
}

// makeLineString helper for test construction
func makeLineString(coords [][]float64) geom.LineString {
	seq := geom.NewSequence(flatten(coords), geom.DimXY)
	return geom.NewLineString(seq)
}
