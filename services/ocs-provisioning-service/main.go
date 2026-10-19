// Command ocs-provisioning-service accepts subscriber provisioning commands
// (create, top-up, adjust, bundles, lifecycle) over REST and gRPC, queues them
// on Kafka keyed by subscriber, and applies them in order with idempotent
// atomic scripts in the hot store. See docs/architecture/08-provisioning.md.
package main

import (
	"context"
	"log"

	"github.com/cckhaw/meridian-ocs/pkg/service"
	"github.com/cckhaw/meridian-ocs/pkg/svcenv"
)

func run(ctx context.Context, ready func()) error {
	cfg, err := LoadConfig(svcenv.OS)
	if err != nil {
		return err
	}
	logf := func(f string, a ...any) { log.Printf("ocs-provisioning-service: "+f, a...) }
	app, err := NewApp(ctx, cfg, svcenv.OS, logf)
	if err != nil {
		return err
	}
	defer app.Close()
	ready()
	return app.Run(ctx)
}

func main() { service.RunWith("ocs-provisioning-service", run) }
