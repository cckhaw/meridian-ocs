// Command ocs-mediation-pipeline turns raw network charging records (BER CDR
// files and a Kafka stream of normalized records) into clean, de-duplicated,
// subscriber-matched, optionally rated records with audit control totals.
// See docs/architecture/10-mediation.md.
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
	logf := func(f string, a ...any) { log.Printf("ocs-mediation-pipeline: "+f, a...) }
	app, err := NewApp(ctx, cfg, svcenv.OS, logf)
	if err != nil {
		return err
	}
	defer app.Close()
	ready()
	return app.Run(ctx)
}

func main() { service.RunWith("ocs-mediation-pipeline", run) }
