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
	bucketObjects = "objects" // Value: [Type][S2Len][S2Bytes][Props]
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

// Put accepts GeoJSON, converts to S2 Binary, and stores it.
func (gs *GeoStore) Put(id string, geoJSON []byte) error {
	var feature geom.GeoJSONFeature
	if err := json.Unmarshal(geoJSON, &feature); err != nil {
		return fmt.Errorf("invalid geojson: %w", err)
	}
	if feature.Geometry.IsEmpty() {
		return errors.New("geometry is empty")
	}

	// 1. Convert
	region, err := geomToS2(feature.Geometry)
	if err != nil {
		return err
	}

	// 2. Props
	propsJSON, err := json.Marshal(feature.Properties)
	if err != nil {
		return err
	}

	// 3. Encode S2 Binary
	storageBytes, err := encodeEntry(region, propsJSON)
	if err != nil {
		return err
	}

	// 4. Index
	terms := gs.indexer.GetIndexTerms(region, "")

	return gs.db.Update(func(tx *bolt.Tx) error {
		bObj := tx.Bucket([]byte(bucketObjects))
		bIdx := tx.Bucket([]byte(bucketIndex))

		// Save S2 Binary blob
		if err := bObj.Put([]byte(id), storageBytes); err != nil {
			return err
		}

		// Save Index Terms
		for _, term := range terms {
			key := make([]byte, 0, len(term)+1+len(id))
			key = append(key, term...)
			key = append(key, 0) // Null byte separator
			key = append(key, id...)
			if err := bIdx.Put(key, []byte("1")); err != nil {
				return err
			}
		}
		return nil
	})
}

func (gs *GeoStore) FindClosest(lat, lng float64, radiusMeters float64) ([]StoredItem, error) {
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

			// Fast Decode (No Trig)
			s2Reg, propsJSON, err := decodeEntry(data)
			if err != nil {
				continue
			}

			distAngle := s1.InfAngle()

			switch v := s2Reg.(type) {
			case s2.Point:
				distAngle = v.Distance(center)
			case *s2.Polyline:
				p, _ := v.Project(center)
				distAngle = p.Distance(center)
			case *s2.Polygon:
				if v.ContainsPoint(center) {
					distAngle = 0
				} else {
					index := s2.NewShapeIndex()
					index.Add(v)
					query := s2.NewClosestEdgeQuery(index, s2.NewClosestEdgeQueryOptions().MaxResults(1))
					target := s2.NewMinDistanceToPointTarget(center)
					res := query.FindEdges(target)
					if len(res) > 0 {
						distAngle = res[0].Distance().Angle()
					}
				}
			}

			if distAngle <= angleRadius {
				// Only reconstruct geometry here for output
				geo, _ := s2ToGeom(s2Reg)
				var props map[string]interface{}
				_ = json.Unmarshal(propsJSON, &props)

				results = append(results, StoredItem{
					ID:         id,
					Geometry:   geo,
					Properties: props,
					Distance:   float64(distAngle) * earthRadiusMeters,
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
