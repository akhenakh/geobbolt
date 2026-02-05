package geostore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	geom "github.com/peterstace/simplefeatures/geom"
	bolt "go.etcd.io/bbolt"
)

const (
	bucketObjects = "objects" // Value: [PropsLen][Props][ShapeCount][Shapes...][Index]
	bucketIndex   = "index"   // Key: Term\x00ID
)

type GeoStore struct {
	db      *bolt.DB
	indexer *s2.RegionTermIndexer
}

type StoredItem struct {
	ID         string
	Geometry   geom.Geometry
	Properties map[string]any
	Distance   float64
}

type IndexEntry struct {
	ID            string
	Blob          []byte
	InteriorTerms []string // Cells completely inside the polygon (guaranteed match)
	ExteriorTerms []string // Cells intersecting the polygon boundary (need PIP test)
}

func NewGeoStore(dbPath string) (*GeoStore, error) {
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(bucketObjects)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(bucketIndex)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	opts := s2.DefaultRegionTermIndexerOptions()
	opts.MinLevel = 4
	opts.MaxLevel = 16
	opts.MaxCells = 8

	return &GeoStore{db: db, indexer: s2.NewRegionTermIndexer(opts)}, nil
}

func (gs *GeoStore) Close() error {
	return gs.db.Close()
}

// PrepareIndexEntry performs all CPU heavy work (S2 Math, Encoding).
// It accepts a parsed geom.GeoJSONFeature to avoid double unmarshalling.
func (gs *GeoStore) PrepareIndexEntry(id string, feature geom.GeoJSONFeature) (IndexEntry, error) {
	if feature.Geometry.IsEmpty() {
		return IndexEntry{}, errors.New("geometry is empty")
	}

	// Convert to S2 Shapes
	shapes, regions, err := geomToS2(feature.Geometry)
	if err != nil {
		return IndexEntry{}, err
	}

	// Encode Props
	propsJSON, err := json.Marshal(feature.Properties)
	if err != nil {
		return IndexEntry{}, err
	}

	// Encode Full Binary Blob (Props + Shapes + Index)
	blob, err := encodeFullEntry(shapes, propsJSON)
	if err != nil {
		return IndexEntry{}, err
	}

	// Generate Interior and Exterior Covers
	// Interior cover: cells completely inside the polygon (if interior cell matches, polygon is definitely inside)
	// Exterior cover: cells intersecting the polygon (if only exterior matches, need point-in-polygon test)
	interiorTermSet := make(map[string]struct{})
	exteriorTermSet := make(map[string]struct{})

	// Create a RegionCoverer with same options as the indexer
	rc := &s2.RegionCoverer{
		MinLevel: gs.indexer.Options.MinLevel,
		MaxLevel: gs.indexer.Options.MaxLevel,
		MaxCells: gs.indexer.Options.MaxCells,
	}

	for _, reg := range regions {
		// Exterior cover: cells that intersect the region
		exteriorCells := rc.Covering(reg)
		terms := gs.indexer.GetIndexTermsForCanonicalCovering(exteriorCells, "")
		for _, t := range terms {
			exteriorTermSet[t] = struct{}{}
		}

		// Interior cover: cells completely inside the region (only for polygons)
		// For non-polygon regions (points, lines), interior cover is empty
		interiorCells := rc.InteriorCovering(reg)
		terms = gs.indexer.GetIndexTermsForCanonicalCovering(interiorCells, "")
		for _, t := range terms {
			interiorTermSet[t] = struct{}{}
		}
	}

	// Convert sets to slices
	interiorTerms := make([]string, 0, len(interiorTermSet))
	for t := range interiorTermSet {
		interiorTerms = append(interiorTerms, t)
	}
	exteriorTerms := make([]string, 0, len(exteriorTermSet))
	for t := range exteriorTermSet {
		exteriorTerms = append(exteriorTerms, t)
	}

	return IndexEntry{
		ID:            id,
		Blob:          blob,
		InteriorTerms: interiorTerms,
		ExteriorTerms: exteriorTerms,
	}, nil
}

func (gs *GeoStore) WriteBatch(entries []IndexEntry) error {
	return gs.db.Update(func(tx *bolt.Tx) error {
		bObj := tx.Bucket([]byte(bucketObjects))
		bIdx := tx.Bucket([]byte(bucketIndex))

		for _, entry := range entries {
			if err := bObj.Put([]byte(entry.ID), entry.Blob); err != nil {
				return err
			}

			// Store interior terms with "int:" prefix
			// Interior terms guarantee the polygon contains the query point
			for _, term := range entry.InteriorTerms {
				prefixedTerm := "int:" + term
				key := make([]byte, len(prefixedTerm)+1+len(entry.ID))
				copy(key, prefixedTerm)
				key[len(prefixedTerm)] = 0
				copy(key[len(prefixedTerm)+1:], entry.ID)

				if err := bIdx.Put(key, []byte("1")); err != nil {
					return err
				}
			}

			// Store exterior terms with "ext:" prefix
			// Exterior terms require point-in-polygon test for confirmation
			for _, term := range entry.ExteriorTerms {
				prefixedTerm := "ext:" + term
				key := make([]byte, len(prefixedTerm)+1+len(entry.ID))
				copy(key, prefixedTerm)
				key[len(prefixedTerm)] = 0
				copy(key[len(prefixedTerm)+1:], entry.ID)

				if err := bIdx.Put(key, []byte("1")); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (gs *GeoStore) Put(id string, geoJSON []byte) error {
	var feature geom.GeoJSONFeature
	if err := json.Unmarshal(geoJSON, &feature); err != nil {
		return fmt.Errorf("invalid geojson: %w", err)
	}
	entry, err := gs.PrepareIndexEntry(id, feature)
	if err != nil {
		return err
	}
	return gs.WriteBatch([]IndexEntry{entry})
}

func (gs *GeoStore) FindClosest(lat, lng float64, radiusMeters float64, withGeometry bool) ([]StoredItem, error) {
	center := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
	earthRadiusMeters := 6371000.0
	angleRadius := s1.Angle(radiusMeters / earthRadiusMeters)
	capRegion := s2.CapFromCenterAngle(center, angleRadius)
	queryTerms := gs.indexer.GetQueryTerms(capRegion, "")

	// Two-pass approach with interior/exterior optimization:
	// 1. Interior candidates: matched via interior cover terms (guaranteed to be inside polygon)
	// 2. Exterior candidates: matched only via exterior cover terms (need point-in-polygon test)
	interiorCandidates := make(map[string]struct{})
	exteriorCandidates := make(map[string]struct{})

	err := gs.db.View(func(tx *bolt.Tx) error {
		bIdx := tx.Bucket([]byte(bucketIndex))
		c := bIdx.Cursor()

		// First pass: query interior terms (guaranteed matches for polygons)
		for _, term := range queryTerms {
			interiorPrefix := []byte("int:" + term + "\x00")
			for k, _ := c.Seek(interiorPrefix); k != nil && bytes.HasPrefix(k, interiorPrefix); k, _ = c.Next() {
				idBytes := bytes.TrimPrefix(k, interiorPrefix)
				interiorCandidates[string(idBytes)] = struct{}{}
			}
		}

		// Second pass: query exterior terms
		// Only add to exteriorCandidates if not already in interiorCandidates
		for _, term := range queryTerms {
			exteriorPrefix := []byte("ext:" + term + "\x00")
			for k, _ := c.Seek(exteriorPrefix); k != nil && bytes.HasPrefix(k, exteriorPrefix); k, _ = c.Next() {
				idBytes := bytes.TrimPrefix(k, exteriorPrefix)
				id := string(idBytes)
				if _, isInterior := interiorCandidates[id]; !isInterior {
					exteriorCandidates[id] = struct{}{}
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	var results []StoredItem

	err = gs.db.View(func(tx *bolt.Tx) error {
		bObj := tx.Bucket([]byte(bucketObjects))

		// Process interior candidates first (no PIP test needed)
		for id := range interiorCandidates {
			item, err := gs.processCandidate(id, center, angleRadius, withGeometry, bObj, true)
			if err != nil {
				continue
			}
			if item != nil {
				results = append(results, *item)
			}
		}

		// Process exterior candidates (need full distance check)
		for id := range exteriorCandidates {
			item, err := gs.processCandidate(id, center, angleRadius, withGeometry, bObj, false)
			if err != nil {
				continue
			}
			if item != nil {
				results = append(results, *item)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	return results, nil
}

// processCandidate processes a single candidate and returns a StoredItem if it matches
// isInteriorMatch: if true, the candidate matched an interior cell (guaranteed inside for polygons)
func (gs *GeoStore) processCandidate(id string, center s2.Point, angleRadius s1.Angle, withGeometry bool, bObj *bolt.Bucket, isInteriorMatch bool) (*StoredItem, error) {
	data := bObj.Get([]byte(id))
	if data == nil {
		return nil, fmt.Errorf("data not found for id: %s", id)
	}

	propsJSON, lazyIndex, factory, err := decodeFullEntry(data)
	if err != nil {
		return nil, err
	}

	limit := s1.ChordAngleFromAngle(angleRadius)

	// Fast cull using index iterator
	iter := lazyIndex.Iterator()
	match := false

	// Scan index cells
	for iter.Begin(); !iter.Done(); iter.Next() {
		cellID := iter.CellID()
		cell := s2.CellFromCellID(cellID)

		// Conservative distance to cell
		if cell.Distance(center) > limit {
			continue
		}

		// Check for shapes in this cell (if any)
		idxCell := iter.IndexCell()
		if idxCell.NumClipped() > 0 {
			match = true
			break
		}
	}

	if !match {
		return nil, nil
	}

	// Load shapes from factory for precise check
	shapes := make([]s2.Shape, factory.Len())
	for i := range factory.Len() {
		shapes[i] = factory.GetShape(i)
	}

	// For interior matches on polygons, we can optimize by checking if center is inside
	// This avoids the expensive edge-by-edge distance calculation
	var minDistAngle s1.Angle
	if isInteriorMatch && len(shapes) > 0 {
		// Check if this is a polygon shape
		switch s := shapes[0].(type) {
		case *s2.Polygon:
			// For interior matches, we know the query point is in an interior cell
			// But we still need to verify it's within the radius
			if s.ContainsPoint(center) {
				minDistAngle = 0 // Inside polygon, distance is 0
			} else {
				// Fallback to edge distance
				minDistAngle = gs.calculateMinDistance(center, shapes)
			}
		default:
			// For non-polygon shapes, calculate normal distance
			minDistAngle = gs.calculateMinDistance(center, shapes)
		}
	} else {
		// For exterior matches or non-polygons, do full distance calculation
		minDistAngle = gs.calculateMinDistance(center, shapes)
	}

	if minDistAngle <= angleRadius {
		var props map[string]any
		_ = json.Unmarshal(propsJSON, &props)

		var geo geom.Geometry
		if withGeometry {
			geo = shapesToGeom(shapes)
		}

		return &StoredItem{
			ID:         id,
			Geometry:   geo,
			Properties: props,
			Distance:   float64(minDistAngle) * 6371000.0,
		}, nil
	}

	return nil, nil
}

// calculateMinDistance computes the minimum distance from center to any edge in shapes
func (gs *GeoStore) calculateMinDistance(center s2.Point, shapes []s2.Shape) s1.Angle {
	minDistAngle := s1.InfAngle()
	for _, s := range shapes {
		for i := range s.NumEdges() {
			e := s.Edge(i)
			d := s2.DistanceFromSegment(center, e.V0, e.V1)
			if d < minDistAngle {
				minDistAngle = d
			}
		}
	}
	return minDistAngle
}
