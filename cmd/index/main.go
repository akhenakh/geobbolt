package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/google/uuid"
	geom "github.com/peterstace/simplefeatures/geom"

	geostore "github.com/akhenakh/geobbolt"
)

func main() {
	inputFile := flag.String("in", "places.geojson", "Input GeoJSON file")
	dbFile := flag.String("db", "geo.db", "Output DB file")
	flag.Parse()

	store, err := geostore.NewGeoStore(*dbFile)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}
	defer store.Close()

	f, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf("Failed to open input: %v", err)
	}
	defer f.Close()

	data, _ := io.ReadAll(f)
	var fc geom.GeoJSONFeatureCollection
	if err := json.Unmarshal(data, &fc); err != nil {
		log.Fatalf("Failed to parse geojson: %v", err)
	}

	fmt.Printf("Ingesting %d features...\n", len(fc.Features))
	count := 0
	for _, feat := range fc.Features {
		id := uuid.New().String()
		if feat.ID != nil {
			id = fmt.Sprintf("%v", feat.ID)
		} else if n, ok := feat.Properties["name"]; ok {
			id = fmt.Sprintf("%v", n)
		}

		raw, _ := feat.MarshalJSON()
		if err := store.Put(id, raw); err != nil {
			log.Printf("Error indexing %s: %v", id, err)
		} else {
			count++
		}
	}
	fmt.Printf("Done. %d indexed.\n", count)
}
