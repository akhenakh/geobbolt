package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	geostore "github.com/akhenakh/geobbolt"
	"github.com/google/uuid"
	geom "github.com/peterstace/simplefeatures/geom"
)

// Job represents a single raw feature to be processed
type Job struct {
	RawFeature json.RawMessage
}

func main() {
	inputFile := flag.String("in", "places.geojson", "Input GeoJSON file")
	dbFile := flag.String("db", "geo.db", "Output DB file")
	workers := flag.Int("w", runtime.NumCPU(), "Number of parallel workers")
	batchSize := flag.Int("batch", 5000, "BoltDB write batch size")
	flag.Parse()

	start := time.Now()

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

	// Setup Pipeline

	jobChan := make(chan Job, *workers*2)
	resultChan := make(chan geostore.IndexEntry, *batchSize)
	var wg sync.WaitGroup

	// Start Workers (CPU bound)

	fmt.Printf("Starting %d workers...\n", *workers)
	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Reusable struct to avoid allocation in loop
			var tempFeature geom.GeoJSONFeature

			for job := range jobChan {
				// We need to parse enough to get the ID and Props for ID generation
				// Note: We are parsing twice (here and in PrepareIndexEntry) for simplicity,
				// but to optimize further, you'd parse once and pass the struct.
				// However, `PrepareIndexEntry` expects bytes.
				if err := json.Unmarshal(job.RawFeature, &tempFeature); err != nil {
					continue
				}

				id := uuid.New().String()
				if tempFeature.ID != nil {
					id = fmt.Sprintf("%v", tempFeature.ID)
				} else if n, ok := tempFeature.Properties["name"]; ok {
					id = fmt.Sprintf("%v", n)
				}

				// Heavy Lifting here: S2 math, Encoding
				entry, err := store.PrepareIndexEntry(id, job.RawFeature)
				if err == nil {
					resultChan <- entry
				} else {
					// log.Printf("Error processing %s: %v", id, err)
				}
			}
		}()
	}

	// Start Writer (Disk bound)

	writeDone := make(chan struct{})
	go func() {
		batch := make([]geostore.IndexEntry, 0, *batchSize)
		count := 0

		flush := func() {
			if len(batch) > 0 {
				if err := store.WriteBatch(batch); err != nil {
					log.Printf("Batch write error: %v", err)
				}
				count += len(batch)
				batch = batch[:0] // Reset slice
				fmt.Printf("\rIndexed: %d...", count)
			}
		}

		for entry := range resultChan {
			batch = append(batch, entry)
			if len(batch) >= *batchSize {
				flush()
			}
		}
		flush() // Final flush
		close(writeDone)
	}()

	// Stream Input (Memory bound)

	dec := json.NewDecoder(f)

	// Locate the "features" array in the stream
	// This logic assumes standard FeatureCollection structure
	// { "type": "FeatureCollection", "features": [ ... ] }
	for {
		t, err := dec.Token()
		if err != nil {
			log.Fatal(err)
		}
		if s, ok := t.(string); ok && s == "features" {
			break
		}
	}

	// Read opening bracket of array
	if _, err := dec.Token(); err != nil {
		log.Fatal(err)
	}

	// Iterate over features array
	itemCount := 0
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			log.Printf("Error decoding feature: %v", err)
			continue
		}
		jobChan <- Job{RawFeature: raw}
		itemCount++
	}

	close(jobChan)    // Signal workers to stop
	wg.Wait()         // Wait for CPU work to finish
	close(resultChan) // Signal writer to stop
	<-writeDone       // Wait for Disk IO to finish

	fmt.Printf("\nDone. Processed %d items in %v.\n", itemCount, time.Since(start))
}
