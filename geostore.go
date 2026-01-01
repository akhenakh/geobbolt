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
	Properties map[string]interface{}
	Distance   float64
}

type IndexEntry struct {
	ID    string
	Blob  []byte
	Terms []string
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

	// 1. Convert to S2 Shapes
	shapes, regions, err := geomToS2(feature.Geometry)
	if err != nil {
		return IndexEntry{}, err
	}

	// 2. Encode Props
	propsJSON, err := json.Marshal(feature.Properties)
	if err != nil {
		return IndexEntry{}, err
	}

	// 3. Encode Full Binary Blob (Props + Shapes + Index)
	blob, err := encodeFullEntry(shapes, propsJSON)
	if err != nil {
		return IndexEntry{}, err
	}

	// 4. Generate Terms
	termSet := make(map[string]struct{})
	for _, reg := range regions {
		terms := gs.indexer.GetIndexTerms(reg, "")
		for _, t := range terms {
			termSet[t] = struct{}{}
		}
	}
	uniqueTerms := make([]string, 0, len(termSet))
	for t := range termSet {
		uniqueTerms = append(uniqueTerms, t)
	}

	return IndexEntry{
		ID:    id,
		Blob:  blob,
		Terms: uniqueTerms,
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

			for _, term := range entry.Terms {
				key := make([]byte, len(term)+1+len(entry.ID))
				copy(key, term)
				key[len(term)] = 0
				copy(key[len(term)+1:], entry.ID)

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

	candidates := make(map[string]struct{})

	err := gs.db.View(func(tx *bolt.Tx) error {
		bIdx := tx.Bucket([]byte(bucketIndex))
		c := bIdx.Cursor()

		for _, term := range queryTerms {
			prefix := make([]byte, 0, len(term)+1)
			prefix = append(prefix, term...)
			prefix = append(prefix, 0)

			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				idBytes := bytes.TrimPrefix(k, prefix)
				candidates[string(idBytes)] = struct{}{}
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

		for id := range candidates {
			data := bObj.Get([]byte(id))
			if data == nil {
				continue
			}

			propsJSON, lazyIndex, factory, err := decodeFullEntry(data)
			if err != nil {
				continue
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
				}
			}

			if !match {
				continue
			}

			// Load shapes from factory for precise check
			shapes := make([]s2.Shape, factory.Len())
			for i := 0; i < factory.Len(); i++ {
				shapes[i] = factory.GetShape(i)
			}

			// Brute force distance check on full geometry
			minDistAngle := s1.InfAngle()
			for _, s := range shapes {
				for i := 0; i < s.NumEdges(); i++ {
					e := s.Edge(i)
					d := s2.DistanceFromSegment(center, e.V0, e.V1)
					if d < minDistAngle {
						minDistAngle = d
					}
				}
			}

			if minDistAngle <= angleRadius {
				var props map[string]interface{}
				_ = json.Unmarshal(propsJSON, &props)

				var geo geom.Geometry
				if withGeometry {
					geo = shapesToGeom(shapes)
				}

				results = append(results, StoredItem{
					ID:         id,
					Geometry:   geo,
					Properties: props,
					Distance:   float64(minDistAngle) * earthRadiusMeters,
				})
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
