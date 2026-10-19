package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kfake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/catalog"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	ocsv1 "github.com/cckhaw/meridian-ocs/pkg/gen/ocs/v1"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/pkg/tlsconf/tlstest"
	"github.com/cckhaw/meridian-ocs/services/ocs-provisioning-service/internal/bus"
)

type system struct {
	app  *App
	pki  *tlstest.PKI
	data *datalayer.Client
	rdb  *redis.Client
	res  *identity.Resolver
	rest string
	grpc string
}

// startSystem runs the whole service: Kafka (in-process), Redis, workers, REST
// and gRPC with mutual TLS, exactly as main assembles it.
func startSystem(t *testing.T) *system {
	t.Helper()
	pki := tlstest.New(t)
	rdb := redistest.Start(t)

	bc := bus.Config{}.WithDefaults()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, bc.CommandsTopic, bc.EventsTopic, bc.DLQTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)

	env := map[string]string{
		"OCS_KAFKA_BROKERS": cluster.ListenAddrs()[0], "OCS_REDIS_ADDR": rdb.Options().Addr, "OCS_CURRENCY": "EUR",
		"OCS_PROVISIONING_AUTH": "mtls", "OCS_PROV_TLS_CERT": pki.CertFile, "OCS_PROV_TLS_KEY": pki.KeyFile, "OCS_PROV_CLIENT_CA": pki.CAFile,
		"OCS_PROV_REST_ADDR": "127.0.0.1:0", "OCS_PROV_GRPC_ADDR": "127.0.0.1:0",
		"OCS_PROV_CLIENTS": "crm,care", "OCS_PROV_PRIVILEGED": "care", "OCS_PROV_WORKERS": "4",
		"OCS_REDIS_REPLICAS": "0", // durability is covered elsewhere; this test has no replica
	}
	g := func(k string) string { return env[k] }
	cfg, err := LoadConfig(g)
	if err != nil {
		t.Fatal(err)
	}
	app, err := NewApp(context.Background(), cfg, g, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("the service did not shut down")
		}
		app.Close()
	})

	dir := identity.NewRedisDirectory(rdb, cfg.IdentityPrefix)
	return &system{app: app, pki: pki, data: datalayer.New(rdb), rdb: rdb,
		res:  identity.NewResolver(identity.NewCachedDirectory(dir, identity.CacheConfig{TTL: time.Millisecond}), identity.ConflictReject),
		rest: "https://" + app.RESTAddr(), grpc: app.GRPCAddr()}
}

func (s *system) http(t *testing.T, cn string) *http.Client {
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: s.pki.ClientTLS(t, cn)}}
}

func (s *system) call(t *testing.T, cn, method, path, idem string, body any) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, s.rest+path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	resp, err := s.http(t, cn).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

func (s *system) waitApplied(t *testing.T, id string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		code, m := s.call(t, "crm", "GET", "/v1/commands/"+id, "", nil)
		if code == 200 && (m["state"] == "applied" || m["state"] == "failed") {
			return m
		}
		if time.Now().After(deadline) {
			t.Fatalf("command %s never finished: %d %v", id, code, m)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *system) charge(t *testing.T, key, sid string, mode datalayer.Mode, n uint32, requested, used int64) datalayer.ItemResult {
	t.Helper()
	var resp datalayer.Response
	rates := []datalayer.Rate{{Class: datalayer.ClassBundle, Num: 1, Den: 1}, {Class: datalayer.ClassCash, Num: 2, Den: 1}}
	err := s.data.Charge(context.Background(), &datalayer.Request{SubscriberID: key, SessionID: sid, Mode: mode, ReqNo: n,
		Now: time.Now(), Validity: 30 * time.Second,
		Items: []datalayer.Item{{RatingGroup: 1, Kind: canonical.UnitBytes, Requested: requested, Used: used, Rates: rates}}}, &resp)
	if err != nil {
		t.Fatal(err)
	}
	return resp.Items[0]
}

func TestProvisioningThroughTheWholeSystem(t *testing.T) {
	s := startSystem(t)

	// 1. A CRM system creates a subscriber and tops it up over REST.
	code, m := s.call(t, "crm", "POST", "/v1/subscribers", "create-1", map[string]any{
		"subscriberKey": "acct-1", "paymentType": "prepaid",
		"identifiers": []map[string]string{{"type": "msisdn", "value": "491701234567"}, {"type": "imsi", "value": "262011234567890"}}})
	if code != 202 || m["state"] != "accepted" {
		t.Fatalf("%d %v", code, m)
	}
	if got := s.waitApplied(t, "create-1"); got["state"] != "applied" || got["version"] != float64(1) {
		t.Fatalf("%v", got)
	}
	if code, m = s.call(t, "crm", "POST", "/v1/subscribers/acct-1/top-ups", "pay-1", map[string]any{"amountMinorUnits": 10000, "currency": "EUR"}); code != 202 {
		t.Fatalf("%d %v", code, m)
	}
	if got := s.waitApplied(t, "pay-1"); got["state"] != "applied" || got["balance"] != float64(10000) {
		t.Fatalf("%v", got)
	}

	// 2. A repeated request is the same command and credits nothing twice.
	code, m = s.call(t, "crm", "POST", "/v1/subscribers/acct-1/top-ups", "pay-1", map[string]any{"amountMinorUnits": 10000, "currency": "EUR"})
	if code != 202 || m["duplicate"] != true {
		t.Fatalf("%d %v", code, m)
	}
	code, m = s.call(t, "crm", "POST", "/v1/subscribers/acct-1/top-ups", "pay-1", map[string]any{"amountMinorUnits": 99999, "currency": "EUR"})
	if code != 409 {
		t.Fatalf("a reused key with different content: %d %v", code, m)
	}

	// 3. Reading back.
	code, m = s.call(t, "crm", "GET", "/v1/subscribers/acct-1", "", nil)
	bs, _ := m["buckets"].([]any)
	if code != 200 || m["status"] != "active" || len(bs) != 1 || bs[0].(map[string]any)["balance"] != float64(10000) {
		t.Fatalf("%d %v", code, m)
	}

	// 4. Over gRPC with a bundle; the permissions differ by caller.
	conn, err := grpc.NewClient("passthrough:///"+s.grpc, grpc.WithTransportCredentials(credentials.NewTLS(s.pki.ClientTLS(t, "care"))))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	gc := ocsv1.NewProvisioningServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := gc.AddBundle(ctx, &ocsv1.AddBundleRequest{IdempotencyKey: "bundle-1", SubscriberKey: "acct-1", BucketId: "data",
		BucketClass: ocsv1.BucketClass_BUCKET_CLASS_BUNDLE, UnitKind: ocsv1.UnitKind_UNIT_KIND_BYTES, Amount: 500}); err != nil {
		t.Fatal(err)
	}
	if got := s.waitApplied(t, "bundle-1"); got["state"] != "applied" || got["balance"] != float64(500) {
		t.Fatalf("%v", got)
	}
	if code, _ = s.call(t, "crm", "POST", "/v1/subscribers/acct-1/lifecycle", "nope-1", map[string]any{"target": "suspended"}); code != 403 {
		t.Fatalf("a standard caller may not change lifecycle state: %d", code)
	}

	// 5. The charging path uses what provisioning created. Both identifiers of
	// the subscriber resolve to the key; the bundle is consumed before cash.
	key, err := s.res.Resolve(context.Background(), []identity.Identity{
		{Type: identity.MSISDN, Value: "491701234567"}, {Type: identity.IMSI, Value: "262011234567890"}})
	if err != nil || key != "acct-1" {
		t.Fatalf("%q %v", key, err)
	}
	if it := s.charge(t, key, "sess-1", datalayer.ModeInitial, 0, 800, 0); it.Granted != 800 {
		t.Fatalf("%+v", it)
	}
	bal := func(id string) int64 {
		v, err := s.data.Balance(context.Background(), key, id)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if bal("data") != 0 || bal("cash") != 10000-2*300 {
		t.Fatalf("bundle %d cash %d", bal("data"), bal("cash"))
	}

	// 6. Suspending (a privileged call, applied through Kafka) bars new quota
	// at once; reinstating restores it.
	if code, m = s.call(t, "care", "POST", "/v1/subscribers/acct-1/lifecycle", "susp-1", map[string]any{"target": "suspended", "reason": "fraud review"}); code != 202 {
		t.Fatalf("%d %v", code, m)
	}
	if got := s.waitApplied(t, "susp-1"); got["state"] != "applied" || got["from"] != "active" || got["to"] != "suspended" {
		t.Fatalf("%v", got)
	}
	if it := s.charge(t, key, "sess-2", datalayer.ModeInitial, 0, 100, 0); it.Code != datalayer.ItemBarred {
		t.Fatalf("a suspended subscriber must get no quota: %+v", it)
	}
	if _, m = s.call(t, "care", "POST", "/v1/subscribers/acct-1/lifecycle", "react-1", map[string]any{"target": "active"}); m["commandId"] != "react-1" {
		t.Fatalf("%v", m)
	}
	s.waitApplied(t, "react-1")
	if it := s.charge(t, key, "sess-3", datalayer.ModeInitial, 0, 100, 0); it.Granted != 100 {
		t.Fatalf("%+v", it)
	}

	// 7. Business failures are final and explained.
	if _, m = s.call(t, "care", "POST", "/v1/subscribers/acct-1/adjustments", "adj-1", map[string]any{"delta": -999999, "currency": "EUR", "reason": "too much"}); m["commandId"] != "adj-1" {
		t.Fatalf("%v", m)
	}
	if got := s.waitApplied(t, "adj-1"); got["state"] != "failed" || got["outcome"] != "insufficient_balance" {
		t.Fatalf("%v", got)
	}
	s.call(t, "crm", "POST", "/v1/subscribers", "create-dup", map[string]any{"subscriberKey": "acct-2", "paymentType": "prepaid",
		"identifiers": []map[string]string{{"type": "msisdn", "value": "491701234567"}}}) // msisdn already belongs to acct-1
	if got := s.waitApplied(t, "create-dup"); got["state"] != "failed" || got["outcome"] != "identity_conflict" {
		t.Fatalf("%v", got)
	}

	// 8. Callers without a client certificate cannot even connect.
	noCert := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: s.pki.ClientTLS(t, "").RootCAs}}}
	if resp, err := noCert.Get(s.rest + "/v1/subscribers/acct-1"); err == nil {
		resp.Body.Close()
		t.Fatalf("a client without a certificate got through: %d", resp.StatusCode)
	}
}

// An offering published in the catalog is attached through the API, applied
// through Kafka and the workers, and then decides which price plan the charging
// path uses for that subscriber.
func TestOfferingsThroughTheWholeSystem(t *testing.T) {
	s := startSystem(t)
	for _, id := range []string{"gold-sub", "plain-sub"} {
		s.call(t, "crm", "POST", "/v1/subscribers", "create-"+id, map[string]any{"subscriberKey": id, "paymentType": "prepaid",
			"identifiers": []map[string]string{{"type": "imsi", "value": map[string]string{"gold-sub": "262011111111111", "plain-sub": "262012222222222"}[id]}}})
		if got := s.waitApplied(t, "create-"+id); got["state"] != "applied" {
			t.Fatalf("%v", got)
		}
	}

	// No catalog yet: a retryable 503, nothing queued.
	if code, m := s.call(t, "crm", "POST", "/v1/subscribers/gold-sub/offering", "att-0", map[string]any{"offering": "gold-offer"}); code != 503 {
		t.Fatalf("%d %v", code, m)
	}

	pub := &catalog.Replicator{Redis: s.rdb, Store: catalog.NewStore(0)}
	doc := catalog.Document{EffectiveFrom: time.Now().Add(-time.Hour),
		Plans: []catalog.PricePlan{
			{ID: "basic", Rules: []catalog.ChargeRule{{ID: "r", Cash: &catalog.Price{Num: 4, Den: 1}}}},
			{ID: "gold", Rules: []catalog.ChargeRule{{ID: "r", Cash: &catalog.Price{Num: 1, Den: 1}}}},
		},
		Offerings: []catalog.ProductOffering{{ID: "gold-offer", PlanID: "gold", Allowances: []catalog.AllowanceBucket{
			{ID: "data", Class: "bundle", Unit: "bytes", Amount: 1000, ValidityDays: 30},
			{ID: "welcome", Class: "promo", Unit: "events", Amount: 5, Order: 3}}}}}
	if _, _, err := pub.Publish(context.Background(), doc, 0); err != nil {
		t.Fatal(err)
	}

	// The service follows the catalog on its own: retry until it has seen it.
	var code int
	var m map[string]any
	for deadline := time.Now().Add(10 * time.Second); ; {
		code, m = s.call(t, "crm", "POST", "/v1/subscribers/gold-sub/offering", "att-1", map[string]any{"offering": "gold-offer"})
		if code != 503 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code != 202 {
		t.Fatalf("%d %v", code, m)
	}
	got := s.waitApplied(t, "att-1")
	if got["state"] != "applied" || got["toOffering"] != "gold-offer" || got["fromOffering"] != "" || got["buckets"] != float64(2) {
		t.Fatalf("%v", got)
	}

	// Reading back over REST and gRPC.
	_, sub := s.call(t, "crm", "GET", "/v1/subscribers/gold-sub", "", nil)
	if sub["offering"] != "gold-offer" || len(sub["buckets"].([]any)) != 2 {
		t.Fatalf("%v", sub)
	}
	conn, err := grpc.NewClient("passthrough:///"+s.grpc, grpc.WithTransportCredentials(credentials.NewTLS(s.pki.ClientTLS(t, "care"))))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	gc := ocsv1.NewProvisioningServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if gs, err := gc.GetSubscriber(ctx, &ocsv1.GetSubscriberRequest{SubscriberKey: "gold-sub"}); err != nil || gs.OfferingId != "gold-offer" {
		t.Fatalf("%v %v", gs, err)
	}

	// Attaching it again is refused (the buckets exist), a renewal is accepted.
	if _, err := gc.AttachOffering(ctx, &ocsv1.AttachOfferingRequest{IdempotencyKey: "att-2", SubscriberKey: "gold-sub", OfferingId: "gold-offer"}); err != nil {
		t.Fatal(err)
	}
	if got := s.waitApplied(t, "att-2"); got["state"] != "failed" || got["outcome"] != "allowance_exists" {
		t.Fatalf("%v", got)
	}
	if _, err := gc.AttachOffering(ctx, &ocsv1.AttachOfferingRequest{IdempotencyKey: "att-3", SubscriberKey: "gold-sub", OfferingId: "gold-offer", Renew: true}); err != nil {
		t.Fatal(err)
	}
	s.waitApplied(t, "att-3")
	if st, err := gc.GetCommand(ctx, &ocsv1.GetCommandRequest{CommandId: "att-3"}); err != nil || st.FromOffering != "gold-offer" || st.ToOffering != "gold-offer" {
		t.Fatalf("%v %v", st, err)
	}

	// The charging path prices each subscriber on the plan its offering selects.
	resolver := catalog.NewProfileResolver(s.data, pub.Store)
	if err := pub.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	tf := &catalog.Tariff{Store: pub.Store, Resolver: resolver, DefaultPlan: "basic"}
	cash := func(key string) int64 {
		rates, err := tf.Rates(&canonical.Request{SubscriberID: key}, &canonical.UnitRequest{Kind: canonical.UnitBytes}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return rates[len(rates)-1].Num
	}
	if cash("gold-sub") != 1 || cash("plain-sub") != 4 {
		t.Fatalf("gold-sub pays %d per byte (want 1), plain-sub %d (want 4)", cash("gold-sub"), cash("plain-sub"))
	}
	// And the bundle that came with the offering is consumed first.
	rates, _ := tf.Rates(&canonical.Request{SubscriberID: "gold-sub"}, &canonical.UnitRequest{Kind: canonical.UnitBytes}, nil)
	var resp datalayer.Response
	err = s.data.Charge(context.Background(), &datalayer.Request{SubscriberID: "gold-sub", SessionID: "s1", Mode: datalayer.ModeInitial, Now: time.Now(),
		Validity: 30 * time.Second, Items: []datalayer.Item{{RatingGroup: 1, Kind: canonical.UnitBytes, Requested: 400, Rates: rates}}}, &resp)
	if err != nil || resp.Items[0].Granted != 400 {
		t.Fatalf("%v %+v", err, resp)
	}
	if bal, _ := s.data.Balance(context.Background(), "gold-sub", "data"); bal != 600 {
		t.Fatalf("the allowance should have paid: %d", bal)
	}
}
