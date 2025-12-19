package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"

	geostore "github.com/akhenakh/geobbolt"
)

func main() {
	dbFile := flag.String("db", "geo.db", "BoltDB file path")
	lat := flag.Float64("lat", 0.0, "Latitude")
	lng := flag.Float64("lng", 0.0, "Longitude")
	radius := flag.Float64("r", 5000.0, "Search radius in meters")
	flag.Parse()

	if *lat == 0 && *lng == 0 {
		log.Fatal("Please provide -lat and -lng arguments")
	}

	// 1. Open Store
	store, err := geostore.NewGeoStore(*dbFile)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}
	defer store.Close()

	// 2. Perform Query
	fmt.Printf("Searching within %.0fm of (%f, %f)...\n", *radius, *lat, *lng)

	results, err := store.FindClosest(*lat, *lng, *radius)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	// 3. Display Results
	if len(results) == 0 {
		fmt.Println("No results found.")
		return
	}

	fmt.Printf("Found %d results:\n", len(results))
	// Adjust width for ID (UUIDs are 36 chars)
	fmt.Printf("%-36s | %-10s | %s\n", "ID", "Distance", "Properties")
	fmt.Println(strings.Repeat("-", 100))

	for _, item := range results {
		// Convert properties map to JSON string for generic display
		propsBytes, _ := json.Marshal(item.Properties)

		fmt.Printf("%-36s | %-8.1fm | %s\n",
			item.ID,
			item.Distance,
			string(propsBytes),
		)
	}
}
