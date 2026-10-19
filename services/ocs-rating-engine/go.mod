module github.com/cckhaw/meridian-ocs/services/ocs-rating-engine

go 1.22

require github.com/cckhaw/meridian-ocs/pkg v0.0.0

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/redis/go-redis/v9 v9.7.3 // indirect
)

replace github.com/cckhaw/meridian-ocs/pkg => ../../pkg
