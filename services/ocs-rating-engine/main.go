// Command ocs-rating-engine is the real-time rating and credit-control service.
// The gateway-facing gRPC listener is added with the signaling adapters; this
// scaffold only wires process lifecycle.
package main

import "github.com/cckhaw/meridian-ocs/pkg/service"

func main() { service.Run("ocs-rating-engine") }
