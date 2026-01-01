package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	geostore "github.com/akhenakh/geobbolt"
)

func main() {
	dbFile := flag.String("db", "geo.db", "BoltDB file path")
	lat := flag.Float64("lat", 0.0, "Latitude")
	lng := flag.Float64("lng", 0.0, "Longitude")
	radius := flag.Float64("r", 5000.0, "Search radius in meters")
	withGeom := flag.Bool("geom", false, "Return geometry in results")
	flag.Parse()

	if *lat == 0 && *lng == 0 {
		log.Fatal("Please provide -lat and -lng arguments")
	}

	start := time.Now()

	// 1. Open Store
	store, err := geostore.NewGeoStore(*dbFile)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}
	defer store.Close()

	// 2. Perform Query
	fmt.Printf("Searching within %.0fm of (%f, %f)...\n", *radius, *lat, *lng)

	results, err := store.FindClosest(*lat, *lng, *radius, *withGeom)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	duration := time.Since(start)

	// 3. Display Results
	if len(results) == 0 {
		fmt.Println("No results found.")
		return
	}

	fmt.Printf("Found %d results in %v:\n", len(results), duration)

	header := fmt.Sprintf("%-36s | %-10s | %s", "ID", "Distance", "Properties")
	if *withGeom {
		header += " | Geometry"
	}
	fmt.Println(header)
	fmt.Println(strings.Repeat("-", len(header)+20))

	for _, item := range results {
		propsBytes, _ := json.Marshal(item.Properties)
		line := fmt.Sprintf("%-36s | %-8.1fm | %s",
			item.ID,
			item.Distance,
			string(propsBytes),
		)
		if *withGeom {
			geoJSON, _ := item.Geometry.MarshalJSON()
			line += fmt.Sprintf(" | %s", string(geoJSON))
		}
		fmt.Println(line)
	}
}
