package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cckhaw/meridian-ocs/pkg/datalayer/redistest"
	"github.com/cckhaw/meridian-ocs/pkg/identity"
	"github.com/cckhaw/meridian-ocs/services/ocs-mediation-pipeline/internal/cdr/cdrtest"
)

const topic = "ocs.mediation.cdr"

type rig struct {
	t        *testing.T
	in, out  string
	rdbAddr  string
	brokers  string
	producer *kgo.Client
}

func newRig(t *testing.T) *rig {
	t.Helper()
	rdb := redistest.Start(t)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	prod, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)

	r := &rig{t: t, in: t.TempDir(), out: t.TempDir(), rdbAddr: rdb.Options().Addr, brokers: cluster.ListenAddrs()[0], producer: prod}
	dir := identity.NewRedisDirectory(rdb, "")
	for imsi, key := range map[string]string{"262010000000001": "alice", "262010000000002": "bob"} {
		if err := dir.Bind(context.Background(), identity.Identity{Type: identity.IMSI, Value: imsi}, key); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

// start runs the service and returns a stop function that checks Run returned cleanly.
func (r *rig) start(group string) (stop func()) {
	r.t.Helper()
	e := map[string]string{
		"OCS_MED_OUT_DIR": r.out, "OCS_MED_IN_DIR": r.in, "OCS_KAFKA_BROKERS": r.brokers, "OCS_MED_KAFKA_GROUP": group,
		"OCS_REDIS_ADDR": r.rdbAddr, "OCS_REDIS_REPLICAS": "0",
		"OCS_MED_SETTLE": "10ms", "OCS_MED_SCAN_INTERVAL": "20ms",
	}
	g := func(k string) string { return e[k] }
	cfg, err := LoadConfig(g)
	if err != nil {
		r.t.Fatal(err)
	}
	app, err := NewApp(context.Background(), cfg, g, r.t.Logf)
	if err != nil {
		r.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				r.t.Errorf("Run returned %v", err)
			}
		case <-time.After(30 * time.Second):
			r.t.Error("Run did not return")
		}
		app.Close()
	}
}

func pgw(imsi string, seq uint64, up, down uint64) cdrtest.PGW {
	return cdrtest.PGW{IMSI: imsi, MSISDN: "491701234567", Node: "pgw-1", APN: "internet", ChargingID: 42, Sequence: seq,
		Y: 30, Mo: 6, D: 1, H: 12, Mi: int(seq), Duration: 60, Containers: [][3]uint64{{100, up, down}}}
}

func (r *rig) drop(name string, b []byte) {
	r.t.Helper()
	if err := os.WriteFile(filepath.Join(r.in, name), b, 0o644); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) produce(part int32, rec map[string]any) {
	r.t.Helper()
	b, _ := json.Marshal(rec)
	if err := r.producer.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Partition: part, Value: b}).FirstErr(); err != nil {
		r.t.Fatal(err)
	}
}

func jsonRec(imsi string, seq int, up, down int) map[string]any {
	return map[string]any{"type": "PGW", "nodeId": "pgw-9", "chargingId": 9, "sequence": seq, "imsi": imsi,
		"openTime": fmt.Sprintf("2030-06-02T10:%02d:00Z", seq), "durationSeconds": 30,
		"containers": []map[string]any{{"ratingGroup": 100, "uplink": up, "downlink": down}}}
}

func (r *rig) outLines(pattern string) (n int, all []map[string]any) {
	files, _ := filepath.Glob(filepath.Join(r.out, pattern))
	sort.Strings(files)
	for _, f := range files {
		raw, _ := os.ReadFile(f)
		sc := bufio.NewScanner(bytes.NewReader(raw))
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			var m map[string]any
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
				r.t.Fatal(err)
			}
			all = append(all, m)
			n++
		}
	}
	return
}

func (r *rig) waitFor(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (r *rig) exists(parts ...string) bool {
	_, err := os.Stat(filepath.Join(parts...))
	return err == nil
}

func TestMediationEndToEnd(t *testing.T) {
	r := newRig(t)
	stop := r.start("g1")
	defer func() { stop() }()

	// --- a BER file from the packet gateway: 3 good records, 1 for an unknown subscriber
	var file []byte
	file = append(file, pgw("262010000000001", 1, 1000, 9000).Encode()...)
	file = append(file, pgw("262010000000002", 2, 10, 20).Encode()...)
	file = append(file, pgw("262019999999999", 3, 5, 5).Encode()...)
	file = append(file, pgw("262010000000001", 4, 1, 1).Encode()...)
	r.drop("pgw-001.ber", file)
	r.waitFor("file output", func() bool { return r.exists(r.in, "processed", "pgw-001.ber") })

	n, got := r.outLines("cdr/dt=2030-06-01/*.jsonl")
	if n != 3 || got[0]["subscriberKey"] != "alice" || got[0]["msisdn"] != "491701234567" || got[0]["apn"] != "internet" {
		t.Fatalf("%d %v", n, got)
	}
	if n, s := r.outLines("suspense/dt=2030-06-01/*.jsonl"); n != 1 || s[0]["reason"] != "unknown_subscriber" {
		t.Fatalf("%d %v", n, s)
	}
	var ctl struct {
		In, Output, Suspense, Duplicates, Rejected int
		Balanced                                   bool
		UplinkBytes, DownlinkBytes                 int64
	}
	files, _ := filepath.Glob(filepath.Join(r.out, "controls", "file-pgw-001.ber-*.json"))
	if len(files) != 1 {
		t.Fatalf("%v", files)
	}
	raw, _ := os.ReadFile(files[0])
	if err := json.Unmarshal(raw, &ctl); err != nil || ctl.In != 4 || ctl.Output != 3 || !ctl.Balanced || ctl.UplinkBytes != 1011 || ctl.DownlinkBytes != 9021 {
		t.Fatalf("%v %+v", err, ctl)
	}

	// --- the same file arrives again under another name: every record is a duplicate
	r.drop("pgw-001-resend.ber", file)
	r.waitFor("resend handled", func() bool { return r.exists(r.in, "processed", "pgw-001-resend.ber") })
	if n, _ := r.outLines("cdr/dt=2030-06-01/*.jsonl"); n != 3 {
		t.Fatalf("a re-sent file produced output: %d records", n)
	}
	if n, _ := r.outLines("duplicates/*.jsonl"); n != 4 {
		t.Fatalf("%d duplicates recorded", n)
	}

	// --- the same name and bytes once more (an FTP retry): recognised as done, no new output
	before, _ := filepath.Glob(filepath.Join(r.out, "controls", "*"))
	r.drop("pgw-001.ber", file)
	r.waitFor("retry moved", func() bool { return !r.exists(r.in, "pgw-001.ber") })
	if after, _ := filepath.Glob(filepath.Join(r.out, "controls", "*")); len(after) != len(before) {
		t.Fatal("a retried file was processed again")
	}

	// --- a truncated file is quarantined whole, nothing of it is committed
	bad := pgw("262010000000001", 5, 1, 1).Encode()
	r.drop("pgw-bad.ber", bad[:len(bad)-3])
	r.waitFor("quarantine", func() bool { return r.exists(r.in, "quarantine", "pgw-bad.ber.error") })
	if n, _ := r.outLines("cdr/dt=2030-06-01/*.jsonl"); n != 3 {
		t.Fatalf("a corrupt file leaked records: %d", n)
	}

	// --- Kafka: two partitions, one record repeats a record from the BER file
	r.produce(0, jsonRec("262010000000001", 10, 7, 7))
	r.produce(0, jsonRec("262010000000002", 11, 8, 8))
	r.produce(1, jsonRec("262010000000001", 12, 9, 9))
	r.produce(1, map[string]any{"type": "PGW"}) // invalid
	// The same session record the BER file carried (PGW file record #1), re-sent over the stream.
	r.produce(0, map[string]any{"type": "PGW", "nodeId": "pgw-1", "chargingId": 42, "sequence": 1, "imsi": "262010000000001",
		"msisdn": "491701234567", "openTime": "2030-06-01T12:01:00Z", "durationSeconds": 60})
	r.waitFor("kafka output", func() bool { n, _ := r.outLines("cdr/dt=2030-06-02/*.jsonl"); return n == 3 })
	// Partitions are consumed in separate units, so wait for each result separately.
	r.waitFor("kafka reject", func() bool { n, _ := r.outLines("rejects/kafka-*.jsonl"); return n == 1 })
	r.waitFor("kafka duplicate recorded", func() bool { n, _ := r.outLines("duplicates/kafka-*.jsonl"); return n == 1 })
	if n, _ := r.outLines("cdr/dt=2030-06-01/*.jsonl"); n != 3 {
		t.Fatalf("a stream copy of a file record became output: %d", n)
	}
	kafkaOut, _ := r.outLines("cdr/dt=2030-06-02/*.jsonl")

	// --- a whole restart that reads the topic from the beginning again (lost group
	//     offsets): the ledger recognises everything, output is unchanged.
	stop()
	stop = func() {}
	cdrFiles, _ := filepath.Glob(filepath.Join(r.out, "cdr", "*", "*"))
	stop2 := r.start("g2-fresh-group")
	time.Sleep(500 * time.Millisecond)
	stop2()
	if again, _ := filepath.Glob(filepath.Join(r.out, "cdr", "*", "*")); fmt.Sprint(again) != fmt.Sprint(cdrFiles) {
		t.Fatalf("re-reading the topic changed the output:\n%v\n%v", cdrFiles, again)
	}
	if n, _ := r.outLines("cdr/dt=2030-06-02/*.jsonl"); n != kafkaOut {
		t.Fatalf("%d records after re-reading, was %d", n, kafkaOut)
	}
	if left, _ := os.ReadDir(filepath.Join(r.out, ".staging")); len(left) != 0 {
		t.Fatalf("staging left behind: %v", left)
	}
}
