module github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service

go 1.22

require (
	github.com/cckhaw/meridian-ocs/pkg v0.0.0
	github.com/redis/go-redis/v9 v9.7.3
	github.com/twmb/franz-go v1.18.1
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20250121001354-6ea03e3a3810
	google.golang.org/grpc v1.64.1
	google.golang.org/protobuf v1.34.2
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
)

replace github.com/cckhaw/meridian-ocs/pkg => ../../pkg
