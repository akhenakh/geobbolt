package geostore

import (
	"fmt"
	"os"
	"testing"

	geom "github.com/peterstace/simplefeatures/geom"
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
