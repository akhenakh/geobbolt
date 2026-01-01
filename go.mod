module github.com/akhenakh/geobbolt

go 1.25.5

require (
	github.com/golang/geo v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/peterstace/simplefeatures v0.56.0
	go.etcd.io/bbolt v1.4.3
)

require golang.org/x/sys v0.29.0 // indirect

replace github.com/golang/geo => github.com/akhenakh/geo v0.0.0-20260101160925-8df695933d67
