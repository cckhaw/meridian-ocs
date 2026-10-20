module github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline

go 1.22

require (
	github.com/cckhaw/meridian-ocs/pkg v0.0.0
	github.com/redis/go-redis/v9 v9.7.3
	github.com/twmb/franz-go v1.18.1
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/cckhaw/meridian-ocs/pkg => ../../pkg
