// Command ocs-loadgen drives the running system with charging load and checks that no money was lost or
// created. It seeds subscribers over the provisioning REST API, opens charging sessions over Nchf (HTTP/2)
// at a constant arrival rate, and afterwards reads every balance back and compares it with what the run
// did. See docs/architecture/14-load-test.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"
)

func parseFlags(args []string, errOut io.Writer) (config, error) {
	var c config
	fs := flag.NewFlagSet("ocs-loadgen", flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.StringVar(&c.Nchf, "nchf", "http://127.0.0.1:8000", "gateway Nchf address (http = HTTP/2 without TLS, https = HTTP/2 with TLS)")
	fs.StringVar(&c.Prov, "prov", "http://127.0.0.1:8443", "provisioning REST address")
	fs.StringVar(&c.GatewayAdmin, "gateway-admin", "", "gateway admin address, e.g. http://127.0.0.1:9080: cross-checks the request counter")
	fs.StringVar(&c.NchfTLS.CA, "nchf-ca", "", "CA that signed the gateway's certificate (https)")
	fs.StringVar(&c.NchfTLS.Cert, "nchf-cert", "", "client certificate for the gateway")
	fs.StringVar(&c.NchfTLS.Key, "nchf-key", "", "client key for the gateway")
	fs.StringVar(&c.ProvTLS.CA, "prov-ca", "", "CA that signed provisioning's certificate (https)")
	fs.StringVar(&c.ProvTLS.Cert, "prov-cert", "", "client certificate for provisioning")
	fs.StringVar(&c.ProvTLS.Key, "prov-key", "", "client key for provisioning")
	fs.IntVar(&c.Subscribers, "subscribers", 1000, "subscribers to create (1-99999); fewer subscribers mean more contention on each balance")
	fs.IntVar(&c.SeedConcurrency, "seed-concurrency", 32, "parallel provisioning commands while seeding")
	fs.StringVar(&c.Offering, "offering", "gold-offer", "offering to attach (it must contain the bucket checked)")
	fs.StringVar(&c.Bucket, "bucket", "data10g", "bucket whose balance is checked")
	rg := fs.Uint("rating-group", 100, "rating group charged")
	fs.Float64Var(&c.Rate, "rate", 200, "charging sessions started per second")
	fs.DurationVar(&c.Duration, "duration", 30*time.Second, "measured duration")
	fs.DurationVar(&c.Warmup, "warmup", 5*time.Second, "warm-up before measuring (same load, not counted in the results)")
	fs.IntVar(&c.Conns, "conns", 8, "HTTP/2 connections to the gateway")
	fs.IntVar(&c.MaxInflight, "max-inflight", 20000, "sessions allowed in flight; arrivals beyond it are counted as dropped")
	fs.DurationVar(&c.Timeout, "timeout", 5*time.Second, "per-request timeout")
	fs.Int64Var(&c.RequestSize, "request-bytes", 1_000_000, "bytes asked for in each create and update")
	fs.Int64Var(&c.UsedPercent, "used-percent", 80, "share of each grant reported as used")
	fs.IntVar(&c.Updates, "updates", 1, "updates between create and release")
	fs.Int64Var(&c.Seed, "seed", 1, "random seed (which subscriber each session charges)")
	fs.StringVar(&c.JSON, "json", "", "write the full result as JSON to this file")
	fs.DurationVar(&c.SLOP99, "slo-p99", 0, "fail when the P99 of the create (from its due time) is above this; 0 = not checked")
	fs.Float64Var(&c.SLOOK, "slo-completed", 0, "fail when fewer than this share of sessions finish as expected (e.g. 0.999); 0 = not checked")
	fs.BoolVar(&c.Audit, "audit", false, "keep every subscriber's history and print it for those that fail the balance check")
	fs.BoolVar(&c.Timeline, "timeline", false, "print outcomes for every second of the run (shows when a fault started and how long recovery took)")
	fs.BoolVar(&c.NoVerify, "no-verify", false, "skip the balance check afterwards")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	c.RatingGroup = uint32(*rg)
	return c, c.validate()
}

func main() {
	c, err := parseFlags(os.Args[1:], os.Stderr)
	if err != nil {
		if err != flag.ErrHelp {
			fmt.Fprintln(os.Stderr, "ocs-loadgen:", err)
		}
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	rp, err := execute(ctx, c, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ocs-loadgen:", err)
		os.Exit(2)
	}
	if !rp.Pass {
		os.Exit(1)
	}
}

// execute seeds, drives the load, verifies and reports.
func execute(ctx context.Context, c config, w io.Writer) (*Report, error) {
	out := func(f string, a ...any) { fmt.Fprintf(w, f, a...) }
	prov, err := newProvClient(c.Prov, c.ProvTLS)
	if err != nil {
		return nil, err
	}
	pool, err := newNchfPool(c.Nchf, c.Conns, c.NchfTLS)
	if err != nil {
		return nil, err
	}
	defer pool.close()

	runID := fmt.Sprintf("%05d", time.Now().Unix()%100000)
	out("run %s: seeding %d subscribers with %s\n", runID, c.Subscribers, c.Offering)
	subs, err := seed(ctx, prov, c, runID, out)
	if err != nil {
		return nil, err
	}
	if subs[0].initial <= 0 {
		return nil, fmt.Errorf("bucket %q starts empty: check --bucket and --offering", c.Bucket)
	}

	var before, unsettledBefore float64
	haveGW := c.GatewayAdmin != ""
	if haveGW {
		if before, err = engineRequests(ctx, c.GatewayAdmin); err != nil {
			return nil, fmt.Errorf("--gateway-admin: %w", err)
		}
		unsettledBefore, _ = scrape(ctx, c.GatewayAdmin, "ocs_engine_unsettled_terminates_total")
	}
	r := &runner{c: c, pool: pool, subs: subs}
	out("driving %.0f sessions/s: %s warm-up, then %s measured\n", c.Rate, c.Warmup, c.Duration)
	measured := r.drive(ctx)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	rp := r.report(measured)
	rp.showTimeline = c.Timeline
	var unsettled float64
	if haveGW {
		if after, err := engineRequests(ctx, c.GatewayAdmin); err == nil {
			d := after - before
			if d < 0 { // the gateway restarted during the run: its counters began again at zero
				d = after
			}
			rp.GatewayCounted = &d
		}
		if after, err := scrape(ctx, c.GatewayAdmin, "ocs_engine_unsettled_terminates_total"); err == nil {
			unsettled = after - unsettledBefore
			if unsettled < 0 {
				unsettled = after
			}
		}
	}
	if !c.NoVerify {
		out("checking every balance\n")
		v := verify(ctx, prov, c, subs, unsettled)
		rp.Verification = &v
	}
	rp.judge(c)
	rp.write(w)
	if c.JSON != "" {
		f, err := os.Create(c.JSON)
		if err != nil {
			return &rp, err
		}
		defer f.Close()
		if err := rp.writeJSON(f); err != nil {
			return &rp, err
		}
	}
	return &rp, nil
}
