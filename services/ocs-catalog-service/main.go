// Command ocs-catalog-service manages the tariff catalog: versioned price
// plans, charge rules, offerings and policy filters, with validation, dry-run
// and atomic publish. Rating nodes follow published versions through Redis and
// swap them in without a restart. See docs/architecture/09-catalog.md.
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
	logf := func(f string, a ...any) { log.Printf("ocs-catalog-service: "+f, a...) }
	app, err := NewApp(ctx, cfg, svcenv.OS, logf)
	if err != nil {
		return err
	}
	defer app.Close()
	ready()
	return app.Run(ctx)
}

func main() { service.RunWith("ocs-catalog-service", run) }
