module github.com/cckhaw/meridian-ocs/services/ocs-signaling-gateway

go 1.22

require (
	github.com/cckhaw/meridian-ocs/pkg v0.0.0
	github.com/golang-jwt/jwt/v5 v5.2.1
	github.com/redis/go-redis/v9 v9.7.3
	golang.org/x/net v0.33.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
	google.golang.org/grpc v1.64.1 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace github.com/cckhaw/meridian-ocs/pkg => ../../pkg
