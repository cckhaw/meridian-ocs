package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/bus"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/command"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/service"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/status"
)

type harness struct {
	t       *testing.T
	cluster *kfake.Cluster
	cfg     bus.Config
	rdb     *redis.Client
	data    *datalayer.Client
	dir     *identity.RedisDirectory
	st      *status.Store
	prod    *bus.KafkaProducer
	svc     *service.Service
}

func newHarness(t *testing.T, partitions int) *harness {
	t.Helper()
	cfg := bus.Config{}.WithDefaults()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(int32(partitions), cfg.CommandsTopic, cfg.EventsTopic, cfg.DLQTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	cfg.Brokers = cluster.ListenAddrs()

	rdb := redistest.Start(t)
	prod, err := bus.NewProducer(cfg.Brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	st := status.New(rdb, "", 0)
	data := datalayer.New(rdb)
	return &harness{t: t, cluster: cluster, cfg: cfg, rdb: rdb, data: data, dir: identity.NewRedisDirectory(rdb, ""), st: st, prod: prod,
		svc: service.New(service.Config{Limits: lim}, st, prod, data)}
}

func (h *harness) applier() *DataApplier {
	return &DataApplier{Data: h.data, Dir: h.dir}
}

type running struct {
	w      *Worker
	cancel context.CancelFunc
	done   chan struct{}
	cl     *kgo.Client
}

func (h *harness) start(a Applier, mod func(*Config)) *running {
	h.t.Helper()
	cfg := Config{Bus: h.cfg, Limits: lim, Workers: 4, Logf: h.t.Logf}
	if mod != nil {
		mod(&cfg)
	}
	cl, err := bus.NewConsumer(h.cfg.WithDefaults())
	if err != nil {
		h.t.Fatal(err)
	}
	w := New(cfg, cl, a, h.st, h.prod)
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{w: w, cancel: cancel, done: make(chan struct{}), cl: cl}
	go func() { defer close(r.done); _ = w.Run(ctx) }()
	h.t.Cleanup(r.stop)
	return r
}

func (r *running) stop() {
	r.cancel()
	<-r.done
	r.cl.Close()
}

func (h *harness) submit(c command.Command) {
	h.t.Helper()
	if _, err := h.svc.Submit(context.Background(), &c); err != nil {
		h.t.Fatalf("submit %s: %v", c.ID, err)
	}
}

func (h *harness) waitFinal(id string) status.Record {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		rec, err := h.st.Get(context.Background(), id)
		if err == nil && rec.Final() {
			return rec
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("command %s never finished: %+v %v", id, rec, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (h *harness) records(topic string) []*kgo.Record {
	h.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.cfg.Brokers...), kgo.ConsumeTopics(topic))
	if err != nil {
		h.t.Fatal(err)
	}
	defer cl.Close()
	var out []*kgo.Record
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
		f := cl.PollFetches(ctx)
		cancel()
		n := 0
		f.EachRecord(func(r *kgo.Record) { out = append(out, r); n++ })
		if n == 0 {
			return out
		}
	}
}

func (h *harness) events() []Event {
	var out []Event
	for _, r := range h.records(h.cfg.EventsTopic) {
		var e Event
		if err := json.Unmarshal(r.Value, &e); err != nil {
			h.t.Fatalf("bad event %s: %v", r.Value, err)
		}
		if string(r.Key) != e.Key {
			h.t.Fatalf("events must be keyed by subscriber: %q vs %q", r.Key, e.Key)
		}
		out = append(out, e)
	}
	return out
}

func (h *harness) waitCommits(r *running, n uint64) {
	h.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for r.w.Stats().Commits < n {
		if time.Now().After(deadline) {
			h.t.Fatalf("offsets were not committed: %+v", r.w.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func topup(id, key string, amount int64) command.Command {
	return command.Command{ID: id, Type: command.TopUp, Key: key, BucketID: "cash", Amount: amount, Currency: "EUR"}
}
func adjust(id, key string, delta int64) command.Command {
	return command.Command{ID: id, Type: command.AdjustBalance, Key: key, BucketID: "cash", Amount: delta, Currency: "EUR", Reason: "test"}
}

func cash(t *testing.T, h *harness, key string) int64 {
	t.Helper()
	bs, err := h.data.ListBuckets(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range bs {
		if b.ID == "cash" {
			return b.Balance
		}
	}
	return 0
}

func TestCommandsFlowThroughKafkaAndProduceEvents(t *testing.T) {
	h := newHarness(t, 3)
	h.start(h.applier(), nil)

	h.submit(create("c1", "acct-1", msisdn, imsi))
	h.submit(topup("t1", "acct-1", 5000))
	h.submit(command.Command{ID: "b1", Type: command.AddBundle, Key: "acct-1", BucketID: "data", Class: "bundle", Unit: "bytes", Amount: 1000})
	h.submit(command.Command{ID: "l1", Type: command.ChangeLifecycle, Key: "acct-1", Target: "grace", Reason: "balance low"})
	for _, id := range []string{"c1", "t1", "b1", "l1"} {
		if rec := h.waitFinal(id); rec.State != status.Applied {
			t.Fatalf("%s: %+v", id, rec)
		}
	}

	rec := h.waitFinal("t1")
	if rec.Balance != 5000 || rec.Version != 2 || rec.Outcome != "applied" || rec.CompletedAt.IsZero() {
		t.Fatalf("%+v", rec)
	}
	if got := cash(t, h, "acct-1"); got != 5000 {
		t.Fatal(got)
	}

	byID := map[string]Event{}
	for _, e := range h.events() {
		byID[e.CommandID] = e
	}
	if e := byID["c1"]; e.Type != EventProvisioned || e.Payment != "prepaid" || e.Key != "acct-1" {
		t.Fatalf("%+v", e)
	}
	if e := byID["t1"]; e.Type != EventBalanceModified || *e.Balance != 5000 || e.Delta != 5000 || e.Bucket != "cash" || e.Version != 2 {
		t.Fatalf("%+v", e)
	}
	if e := byID["l1"]; e.Type != EventLifecycle || e.From != "active" || e.To != "grace" || e.Reason != "balance low" {
		t.Fatalf("a lifecycle change must announce itself so notifications can fire: %+v", e)
	}
}

// Many commands for one subscriber, submitted back to back: the result depends
// on the order they are applied in.
func TestCommandsForOneSubscriberAreAppliedInOrder(t *testing.T) {
	h := newHarness(t, 4)
	h.start(h.applier(), func(c *Config) { c.Workers = 8 })

	steps := []struct {
		cmd  command.Command
		want status.State
	}{
		{create("o-c", "acct-o", msisdn), status.Applied},
		{topup("o-1", "acct-o", 100), status.Applied},
		{adjust("o-2", "acct-o", -60), status.Applied}, // 40
		{adjust("o-3", "acct-o", -60), status.Failed},  // would go to -20
		{topup("o-4", "acct-o", 50), status.Applied},   // 90
		{adjust("o-5", "acct-o", -60), status.Applied}, // 30
		{adjust("o-6", "acct-o", -31), status.Failed},  // would go to -1
		{adjust("o-7", "acct-o", -30), status.Applied}, // 0
	}
	for _, s := range steps {
		h.submit(s.cmd)
	}
	for _, s := range steps {
		if rec := h.waitFinal(s.cmd.ID); rec.State != s.want {
			t.Errorf("%s: %s (%s %s), want %s", s.cmd.ID, rec.State, rec.Outcome, rec.Message, s.want)
		}
	}
	if got := cash(t, h, "acct-o"); got != 0 {
		t.Fatalf("balance %d", got)
	}
	if rec := h.waitFinal("o-3"); rec.Outcome != "insufficient_balance" {
		t.Fatalf("%+v", rec)
	}
}

func TestManySubscribersInParallel(t *testing.T) {
	h := newHarness(t, 6)
	r := h.start(h.applier(), func(c *Config) { c.Workers = 8 })
	const subs, per = 40, 5
	for s := 0; s < subs; s++ {
		key := fmt.Sprintf("p-%02d", s)
		h.submit(create("pc-"+key, key, command.Identifier{Type: "msisdn", Value: fmt.Sprintf("4917000%05d", s)}))
		for i := 0; i < per; i++ {
			h.submit(topup(fmt.Sprintf("pt-%s-%d", key, i), key, int64(10+i)))
		}
	}
	for s := 0; s < subs; s++ {
		key := fmt.Sprintf("p-%02d", s)
		h.waitFinal(fmt.Sprintf("pt-%s-%d", key, per-1))
		if got := cash(t, h, key); got != 10+11+12+13+14 {
			t.Errorf("%s: %d", key, got)
		}
	}
	if st := r.w.Stats(); st.Applied != subs*(1+per) || st.Failed != 0 {
		t.Fatalf("%+v", st)
	}
}

// A message delivered twice (a producer retry, or a redelivery after a crash
// before the commit) must change nothing the second time.
func TestRedeliveredMessagesAreAppliedOnce(t *testing.T) {
	h := newHarness(t, 2)
	h.start(h.applier(), nil)
	h.submit(create("d-c", "acct-d", msisdn))
	h.waitFinal("d-c")

	c := topup("d-pay", "acct-d", 700)
	c.AcceptedAt = time.Now().UnixMilli()
	raw, _ := c.Encode()
	h.submit(c)
	for i := 0; i < 3; i++ { // the same message again, straight onto the topic
		if err := h.prod.Produce(context.Background(), h.cfg.CommandsTopic, "acct-d", raw, nil); err != nil {
			t.Fatal(err)
		}
	}
	h.waitFinal("d-pay")
	// Let the redeliveries drain: a following command on the same key has been
	// processed only after them.
	h.submit(topup("d-after", "acct-d", 1))
	h.waitFinal("d-after")
	if got := cash(t, h, "acct-d"); got != 701 {
		t.Fatalf("a redelivered top-up was credited more than once: %d", got)
	}
	if rec := h.waitFinal("d-pay"); rec.State != status.Applied || rec.Balance != 700 {
		t.Fatalf("the recorded outcome must stay the first one: %+v", rec)
	}
}

// Offsets are committed only after a batch is done: a finished batch is not
// redelivered after a restart, and an unfinished one is not lost.
func TestRestartNeitherRepeatsNorLosesCommands(t *testing.T) {
	h := newHarness(t, 1)
	var calls atomic.Int64
	counting := applierFunc(func(ctx context.Context, c *command.Command) (Outcome, error) {
		calls.Add(1)
		return h.applier().Apply(ctx, c)
	})

	first := h.start(counting, nil)
	h.submit(create("r-c", "acct-r", msisdn))
	h.submit(topup("r-1", "acct-r", 10))
	h.waitFinal("r-1")
	h.waitCommits(first, 1)
	first.stop()
	before := calls.Load()

	// A command arrives while no worker is running.
	h.submit(topup("r-2", "acct-r", 20))
	second := h.start(counting, nil)
	h.waitFinal("r-2")
	if got := cash(t, h, "acct-r"); got != 30 {
		t.Fatalf("balance %d", got)
	}
	if n := calls.Load() - before; n != 1 {
		t.Fatalf("the restarted worker applied %d commands; the committed ones must not be redelivered", n)
	}
	h.waitCommits(second, 1)
}

func TestACommandInFlightWhenTheWorkerStopsIsRedelivered(t *testing.T) {
	h := newHarness(t, 1)
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	blocking := applierFunc(func(ctx context.Context, c *command.Command) (Outcome, error) {
		if c.ID == "f-2" {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-gate:
			case <-ctx.Done():
				return Outcome{}, ctx.Err()
			}
		}
		return h.applier().Apply(ctx, c)
	})
	first := h.start(blocking, nil)
	h.submit(create("f-c", "acct-f", msisdn))
	h.waitFinal("f-c")
	h.waitCommits(first, 1)
	h.submit(topup("f-2", "acct-f", 99))
	<-entered
	first.stop() // killed while f-2 is being applied

	if rec, _ := h.st.Get(context.Background(), "f-2"); rec.Final() {
		t.Fatalf("it never finished: %+v", rec)
	}
	h.start(h.applier(), nil)
	if rec := h.waitFinal("f-2"); rec.State != status.Applied {
		t.Fatalf("%+v", rec)
	}
	if got := cash(t, h, "acct-f"); got != 99 {
		t.Fatalf("%d", got)
	}
}

type applierFunc func(ctx context.Context, c *command.Command) (Outcome, error)

func (f applierFunc) Apply(ctx context.Context, c *command.Command) (Outcome, error) {
	return f(ctx, c)
}

// While infrastructure is failing a command is retried, not skipped, and the
// commands behind it for the same subscriber wait for it.
func TestInfrastructureErrorsAreRetriedWithoutSkippingOrReordering(t *testing.T) {
	h := newHarness(t, 1)
	var mu sync.Mutex
	var order []string
	var failures atomic.Int64
	flaky := applierFunc(func(ctx context.Context, c *command.Command) (Outcome, error) {
		if c.ID == "x-1" && failures.Add(1) <= 3 {
			return Outcome{}, errors.New("redis timeout")
		}
		out, err := h.applier().Apply(ctx, c)
		if err == nil {
			mu.Lock()
			order = append(order, c.ID)
			mu.Unlock()
		}
		return out, err
	})
	r := h.start(flaky, func(c *Config) { c.MinBackoff, c.MaxBackoff = time.Millisecond, 5*time.Millisecond })
	h.submit(create("x-c", "acct-x", msisdn))
	h.submit(topup("x-1", "acct-x", 100))
	h.submit(adjust("x-2", "acct-x", -100)) // only valid if x-1 was applied first
	h.waitFinal("x-2")

	if rec := h.waitFinal("x-2"); rec.State != status.Applied {
		t.Fatalf("a command behind a retried one must wait for it: %+v", rec)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(order) != "[x-c x-1 x-2]" {
		t.Fatalf("applied in order %v", order)
	}
	if r.w.Stats().Retries < 3 {
		t.Fatalf("%+v", r.w.Stats())
	}
	if got := cash(t, h, "acct-x"); got != 0 {
		t.Fatal(got)
	}
}

func TestBusinessFailuresAreFinalAndPublished(t *testing.T) {
	h := newHarness(t, 1)
	r := h.start(h.applier(), nil)
	h.submit(topup("g-1", "ghost", 5)) // no such subscriber
	rec := h.waitFinal("g-1")
	if rec.State != status.Failed || rec.Outcome != "no_such_subscriber" || rec.Message == "" {
		t.Fatalf("%+v", rec)
	}
	h.submit(create("g-c", "acct-g", msisdn))
	h.waitFinal("g-c")
	h.submit(create("g-dup", "acct-h", msisdn)) // msisdn belongs to acct-g
	if rec := h.waitFinal("g-dup"); rec.Outcome != "identity_conflict" {
		t.Fatalf("%+v", rec)
	}
	var failed []Event
	for _, e := range h.events() {
		if e.Type == EventCommandFailed {
			failed = append(failed, e)
		}
	}
	if len(failed) != 2 {
		t.Fatalf("%+v", failed)
	}
	if st := r.w.Stats(); st.Failed != 2 || st.Applied != 1 {
		t.Fatalf("%+v", st)
	}
}

func TestPoisonMessagesGoToTheDeadLetterTopicAndDoNotBlockTheQueue(t *testing.T) {
	h := newHarness(t, 1)
	r := h.start(h.applier(), nil)
	ctx := context.Background()

	good := create("q-c", "acct-q", msisdn)
	good.AcceptedAt = time.Now().UnixMilli()
	if _, err := h.svc.Submit(ctx, &good); err != nil {
		t.Fatal(err)
	}

	// 1. garbage; 2. an unknown schema version whose id can still be read;
	// 3. a message whose Kafka key disagrees with the subscriber it names.
	_ = h.prod.Produce(ctx, h.cfg.CommandsTopic, "acct-q", []byte("not json at all"), nil)
	pending := topup("q-v", "acct-q", 5)
	_, _, _ = h.st.Begin(ctx, &pending) // so the failure has a record to land on
	_ = h.prod.Produce(ctx, h.cfg.CommandsTopic, "acct-q", []byte(`{"v":99,"id":"q-v","type":"top_up","key":"acct-q"}`), nil)
	mis := topup("q-k", "acct-other", 5)
	mis.AcceptedAt = time.Now().UnixMilli()
	raw, _ := mis.Encode()
	_ = h.prod.Produce(ctx, h.cfg.CommandsTopic, "acct-q", raw, nil)

	// A good command behind them still gets through.
	h.submit(topup("q-after", "acct-q", 42))
	h.waitFinal("q-after")
	if got := cash(t, h, "acct-q"); got != 42 {
		t.Fatalf("%d", got)
	}
	if rec := h.waitFinal("q-v"); rec.State != status.Failed || rec.Outcome != "dead_lettered" {
		t.Fatalf("%+v", rec)
	}

	dl := h.records(h.cfg.DLQTopic)
	if len(dl) != 3 || r.w.Stats().DeadLettered != 3 {
		t.Fatalf("%d dead letters, stats %+v", len(dl), r.w.Stats())
	}
	for _, rec := range dl {
		hdr := map[string]string{}
		for _, x := range rec.Headers {
			hdr[x.Key] = string(x.Value)
		}
		if hdr["reason"] == "" || hdr["source-topic"] != h.cfg.CommandsTopic || hdr["source-offset"] == "" {
			t.Fatalf("a dead letter must say why and where it came from: %+v", hdr)
		}
	}
}

func TestInvalidCommandsThatReachTheTopicFailWithoutBeingApplied(t *testing.T) {
	h := newHarness(t, 1)
	h.start(h.applier(), nil)
	h.submit(create("v-c", "acct-v", msisdn))
	h.waitFinal("v-c")

	// Bypasses the API's validation: a top-up over the configured limit.
	bad := topup("v-1", "acct-v", lim.MaxTopUp+1)
	bad.AcceptedAt = time.Now().UnixMilli()
	if _, _, err := h.st.Begin(context.Background(), &bad); err != nil {
		t.Fatal(err)
	}
	raw, _ := bad.Encode()
	if err := h.prod.Produce(context.Background(), h.cfg.CommandsTopic, "acct-v", raw, nil); err != nil {
		t.Fatal(err)
	}
	rec := h.waitFinal("v-1")
	if rec.State != status.Failed || rec.Outcome != "invalid" {
		t.Fatalf("%+v", rec)
	}
	if got := cash(t, h, "acct-v"); got != 0 {
		t.Fatalf("an invalid command was applied: %d", got)
	}
}

func TestProvisioningIsReflectedInTheChargingPathEndToEnd(t *testing.T) {
	h := newHarness(t, 2)
	h.start(h.applier(), nil)
	h.submit(create("e-c", "acct-e", msisdn, imsi))
	h.submit(topup("e-t", "acct-e", 10_000))
	h.waitFinal("e-t")

	// The identity directory now resolves both identifiers to the new key,
	// and the balance provisioning created is what a charge draws from.
	res := identity.NewResolver(identity.NewCachedDirectory(h.dir, identity.CacheConfig{}), identity.ConflictReject)
	key, err := res.Resolve(context.Background(), []identity.Identity{{Type: identity.MSISDN, Value: "491701234567"}, {Type: identity.IMSI, Value: "262011234567890"}})
	if err != nil || key != "acct-e" {
		t.Fatalf("%q %v", key, err)
	}
	h.submit(command.Command{ID: "e-s", Type: command.ChangeLifecycle, Key: "acct-e", Target: "suspended"})
	h.waitFinal("e-s")
	p, err := h.data.GetProfile(context.Background(), "acct-e")
	if err != nil || p.Status != "suspended" {
		t.Fatalf("%+v %v", p, err)
	}
}
