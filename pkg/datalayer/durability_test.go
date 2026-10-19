package datalayer_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cckhaw/meridian-ocs/pkg/canonical"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer"
	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
)

func replicaBalance(t *testing.T, replica *redis.Client, sub, bucket string) string {
	t.Helper()
	v, err := replica.HGet(context.Background(), datalayer.Prefix(sub)+":b:"+bucket, "bal").Result()
	if err != nil {
		t.Fatalf("replica read: %v", err)
	}
	return v
}

// Committing calls report a replica acknowledgement and the replica agrees
// when they return. NOTE: on loopback a replica is practically never behind,
// even without WAIT (measured: 0 of 300 commits), so this test cannot tell WAIT
// from no WAIT; TestAReplicaThatStopsAnsweringIsReported does.
func TestCommitsAreOnTheReplicaWhenTheCallReturns(t *testing.T) {
	primary := redistest.Start(t)
	replica, _ := redistest.StartReplica(t, primary)
	c, err := datalayer.New(primary).WithDurability(datalayer.Durability{Replicas: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	put(t, c, "d1", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 100_000, Order: 1})
	r := rates(cash2)

	charge(t, c, req("d1", "x", datalayer.ModeInitial, 0, t0, 1000, 0, r))
	for n := uint32(1); n <= 50; n++ {
		resp := charge(t, c, req("d1", "x", datalayer.ModeUpdate, n, t0, 1000, 700, r))
		if resp.Replication != datalayer.ReplicationAcked {
			t.Fatalf("update %d: replication %v", n, resp.Replication)
		}
		// No sleep: the replica must already agree.
		want, _ := primary.HGet(ctx, datalayer.Prefix("d1")+":b:bundle", "bal").Result()
		if got := replicaBalance(t, replica, "d1", "bundle"); got != want {
			t.Fatalf("update %d: primary %s, replica %s at the moment the call returned", n, want, got)
		}
	}
	term := charge(t, c, req("d1", "x", datalayer.ModeTerminate, 51, t0, 0, 300, r))
	if term.Replication != datalayer.ReplicationAcked {
		t.Fatalf("terminate: %v", term.Replication)
	}

	// A reservation is not a commit and does not pay for a wait.
	if init := charge(t, c, req("d1", "y", datalayer.ModeInitial, 0, t0, 10, 0, r)); init.Replication != datalayer.ReplicationNotRequested {
		t.Fatalf("initial: %v", init.Replication)
	}
	st := c.DurabilityStats()
	if st.Waits != 51 || st.Short != 0 || st.WaitErrors != 0 {
		t.Fatalf("%+v", st)
	}
}

// A replica that is connected but not answering (frozen process) must make a
// commit report a shortfall; once it resumes and catches up the commit is
// acknowledged again. This is what proves the result comes from WAIT.
func TestAReplicaThatStopsAnsweringIsReported(t *testing.T) {
	primary := redistest.Start(t)
	replica, rep := redistest.StartReplica(t, primary)
	c, _ := datalayer.New(primary).WithDurability(datalayer.Durability{Replicas: 1, Timeout: 60 * time.Millisecond, SuspendAfter: 100})
	put(t, c, "d5", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 100_000, Order: 1})
	r := rates(cash2)
	charge(t, c, req("d5", "x", datalayer.ModeInitial, 0, t0, 1000, 0, r))

	// A freshly started replica may still be finishing its initial sync (slow
	// CI runners), so allow it a few seconds to acknowledge before the freeze.
	next := uint32(1)
	for warm := time.Now().Add(5 * time.Second); ; next++ {
		resp := charge(t, c, req("d5", "x", datalayer.ModeUpdate, next, t0, 1000, 100, r))
		if resp.Replication == datalayer.ReplicationAcked {
			next++
			break
		}
		if time.Now().After(warm) {
			t.Fatalf("healthy replica never acknowledged: %v", resp.Replication)
		}
	}
	rep.Pause()
	start := time.Now()
	resp := charge(t, c, req("d5", "x", datalayer.ModeUpdate, next, t0, 1000, 100, r))
	if resp.Replication != datalayer.ReplicationShort || time.Since(start) < 55*time.Millisecond {
		t.Fatalf("frozen replica: %v after %v", resp.Replication, time.Since(start))
	}
	rep.Resume()
	deadline := time.Now().Add(5 * time.Second)
	for n := next + 1; ; n++ {
		if resp := charge(t, c, req("d5", "x", datalayer.ModeUpdate, n, t0, 1000, 100, r)); resp.Replication == datalayer.ReplicationAcked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the resumed replica never caught up")
		}
	}
	want, _ := primary.HGet(context.Background(), datalayer.Prefix("d5")+":b:bundle", "bal").Result()
	if got := replicaBalance(t, replica, "d5", "bundle"); got != want {
		t.Fatalf("primary %s, replica %s", want, got)
	}
}

func TestShortfallIsReportedAndWaitingIsSuspendedForADeadReplica(t *testing.T) {
	primary := redistest.Start(t)
	_, rep := redistest.StartReplica(t, primary)
	c, _ := datalayer.New(primary).WithDurability(datalayer.Durability{
		Replicas: 1, Timeout: 40 * time.Millisecond, SuspendAfter: 3, SuspendFor: time.Hour})
	put(t, c, "d2", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 100_000, Order: 1})
	r := rates(cash2)
	charge(t, c, req("d2", "x", datalayer.ModeInitial, 0, t0, 1000, 0, r))

	rep.Kill()
	time.Sleep(50 * time.Millisecond)

	var slow []time.Duration
	for n := uint32(1); n <= 6; n++ {
		start := time.Now()
		resp := charge(t, c, req("d2", "x", datalayer.ModeUpdate, n, t0, 1000, 100, r))
		slow = append(slow, time.Since(start))
		if resp.Replication != datalayer.ReplicationShort {
			t.Fatalf("update %d: replication %v, want short", n, resp.Replication)
		}
		if resp.Code != datalayer.CodeOK || resp.Items[0].Granted != 1000 {
			t.Fatalf("a shortfall must not change the business result: %+v", resp)
		}
	}
	// The first SuspendAfter commits each waited out the timeout; afterwards
	// waiting is suspended and commits are fast again.
	for i := 0; i < 3; i++ {
		if slow[i] < 35*time.Millisecond {
			t.Errorf("commit %d returned in %v: it should have waited for the replica", i+1, slow[i])
		}
	}
	for i := 3; i < 6; i++ {
		if slow[i] > 30*time.Millisecond {
			t.Errorf("commit %d took %v: waiting should be suspended", i+1, slow[i])
		}
	}
	st := c.DurabilityStats()
	if st.Short != 3 || st.Suspended != 3 {
		t.Fatalf("%+v", st)
	}
	// The data itself is intact on the primary: 1000 reserved, then six
	// updates that each used 100 of the previous slice and reserved a new one.
	if got := bal(t, c, "d2", "bundle"); got != 100_000-1000-6*100 {
		t.Fatalf("balance %d", got)
	}
}

func TestWaitingResumesAfterTheSuspension(t *testing.T) {
	primary := redistest.Start(t)
	_, rep := redistest.StartReplica(t, primary)
	c, _ := datalayer.New(primary).WithDurability(datalayer.Durability{
		Replicas: 1, Timeout: 20 * time.Millisecond, SuspendAfter: 1, SuspendFor: 100 * time.Millisecond})
	put(t, c, "d3", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 100_000, Order: 1})
	r := rates(cash2)
	charge(t, c, req("d3", "x", datalayer.ModeInitial, 0, t0, 1000, 0, r))
	rep.Kill()
	time.Sleep(50 * time.Millisecond)

	charge(t, c, req("d3", "x", datalayer.ModeUpdate, 1, t0, 1000, 10, r)) // short -> suspended
	charge(t, c, req("d3", "x", datalayer.ModeUpdate, 2, t0, 1000, 10, r)) // suspended
	if st := c.DurabilityStats(); st.Suspended != 1 {
		t.Fatalf("%+v", st)
	}
	time.Sleep(150 * time.Millisecond)
	charge(t, c, req("d3", "x", datalayer.ModeUpdate, 3, t0, 1000, 10, r)) // probes again
	if st := c.DurabilityStats(); st.Waits != 2 {
		t.Fatalf("the probe after the suspension must wait again: %+v", st)
	}
}

func TestDurabilityOffByDefault(t *testing.T) {
	c, _ := setup(t)
	put(t, c, "d4", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1000, Order: 1})
	r := rates(cash2)
	charge(t, c, req("d4", "x", datalayer.ModeInitial, 0, t0, 100, 0, r))
	if resp := charge(t, c, req("d4", "x", datalayer.ModeUpdate, 1, t0, 100, 10, r)); resp.Replication != datalayer.ReplicationNotRequested {
		t.Fatalf("%v", resp.Replication)
	}
	if (c.DurabilityStats() != datalayer.DurabilityStats{}) {
		t.Fatal("no counters without durability")
	}
}

func TestDurabilityConfigurationIsValidated(t *testing.T) {
	if _, err := datalayer.New(redis.NewClient(&redis.Options{})).WithDurability(datalayer.Durability{Replicas: -1}); err == nil {
		t.Error("negative replicas")
	}
	cluster := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	defer cluster.Close()
	if _, err := datalayer.New(cluster).WithDurability(datalayer.Durability{Replicas: 1}); err != datalayer.ErrClusterWait {
		t.Errorf("a cluster client cannot pin WAIT to the primary: %v", err)
	}
}

func TestConnectedReplicas(t *testing.T) {
	primary := redistest.Start(t)
	c := datalayer.New(primary)
	if n, err := c.ConnectedReplicas(context.Background()); err != nil || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
	_, rep := redistest.StartReplica(t, primary)
	if n, err := c.ConnectedReplicas(context.Background()); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	rep.Kill()
}

func BenchmarkChargeUpdateWithReplicaAck(b *testing.B) {
	primary := redistest.Start(b)
	redistest.StartReplica(b, primary)
	c, _ := datalayer.New(primary).WithDurability(datalayer.Durability{Replicas: 1, Timeout: time.Second})
	ctx := context.Background()
	_ = c.PutBucket(ctx, "bench", datalayer.Bucket{ID: "bundle", Class: datalayer.ClassBundle, Kind: canonical.UnitBytes, Balance: 1 << 40, Order: 1})
	r := rates(cash2)
	var resp datalayer.Response
	q := req("bench", "x", datalayer.ModeInitial, 0, t0, 1000, 0, r)
	_ = c.Charge(ctx, q, &resp)
	q.Mode = datalayer.ModeUpdate
	q.Items[0].Used = 1000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.ReqNo = uint32(i + 1)
		if err := c.Charge(ctx, q, &resp); err != nil {
			b.Fatal(err)
		}
	}
}
