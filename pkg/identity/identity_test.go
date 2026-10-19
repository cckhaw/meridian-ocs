package identity

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mapDir struct {
	m     map[Identity]string
	err   error
	calls atomic.Int64
	delay time.Duration
}

func (d *mapDir) Lookup(_ context.Context, id Identity) (string, error) {
	d.calls.Add(1)
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	if d.err != nil {
		return "", d.err
	}
	if k, ok := d.m[id]; ok {
		return k, nil
	}
	return "", ErrUnknown
}

func msisdn(v string) Identity { return Identity{MSISDN, v} }
func imsi(v string) Identity   { return Identity{IMSI, v} }

func TestNormalize(t *testing.T) {
	tests := []struct {
		t    Type
		in   string
		want string
		ok   bool
	}{
		{MSISDN, "491701234567", "491701234567", true},
		{MSISDN, "+491701234567", "491701234567", true},
		{MSISDN, "0170 1234567", "", false},
		{MSISDN, "12345", "", false},
		{MSISDN, "1234567890123456", "", false},
		{MSISDN, "", "", false},
		{IMSI, "262011234567890", "262011234567890", true},
		{IMSI, "26201123456789a", "", false},
		{IMSI, "12345", "", false},
		{NAI, "user@example.com", "user@example.com", true},
		{NAI, "us er@example.com", "", false},
		{NAI, "ab", "", false},
		{NAI, "ünï@example.com", "", false},
		{Type(9), "x", "", false},
	}
	for _, tc := range tests {
		got, ok := Normalize(tc.t, tc.in)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("%v %q: got %q,%v want %q,%v", tc.t, tc.in, got, ok, tc.want, tc.ok)
		}
	}
	long := make([]byte, 129)
	for i := range long {
		long[i] = 'a'
	}
	if _, ok := Normalize(NAI, string(long)); ok {
		t.Error("129-char NAI must be refused")
	}
}

func TestValidKey(t *testing.T) {
	for k, want := range map[string]bool{"acct-8812": true, "a.b_c": true, "": false, "a b": false, "a{b}": false, "tab\t": false, "ü": false} {
		if ValidKey(k) != want {
			t.Errorf("%q: want %v", k, want)
		}
	}
}

func TestResolveSingleAndAgreeing(t *testing.T) {
	d := &mapDir{m: map[Identity]string{msisdn("491701234567"): "acct-1", imsi("262011234567890"): "acct-1"}}
	r := NewResolver(d, ConflictReject)
	ctx := context.Background()

	if k, err := r.Resolve(ctx, []Identity{msisdn("491701234567")}); err != nil || k != "acct-1" {
		t.Fatalf("%q %v", k, err)
	}
	if k, err := r.Resolve(ctx, []Identity{msisdn("+491701234567"), imsi("262011234567890")}); err != nil || k != "acct-1" {
		t.Fatalf("both identities of one subscriber: %q %v", k, err)
	}
	// One identity unknown, the other known: the known one wins.
	if k, err := r.Resolve(ctx, []Identity{msisdn("491709999999"), imsi("262011234567890")}); err != nil || k != "acct-1" {
		t.Fatalf("%q %v", k, err)
	}
}

func TestResolveFailures(t *testing.T) {
	d := &mapDir{m: map[Identity]string{msisdn("491701234567"): "acct-1", imsi("262011234567890"): "acct-2"}}
	ctx := context.Background()

	r := NewResolver(d, ConflictReject)
	if _, err := r.Resolve(ctx, nil); !errors.Is(err, ErrNoIdentity) {
		t.Errorf("%v", err)
	}
	if _, err := r.Resolve(ctx, []Identity{msisdn("491709999999")}); !errors.Is(err, ErrUnknown) {
		t.Errorf("%v", err)
	}
	if _, err := r.Resolve(ctx, []Identity{msisdn("12")}); !errors.Is(err, ErrInvalid) {
		t.Errorf("%v", err)
	}
	if _, err := r.Resolve(ctx, []Identity{msisdn("491701234567"), imsi("262011234567890")}); !errors.Is(err, ErrConflict) {
		t.Errorf("conflicting identities must be refused: %v", err)
	}

	boom := errors.New("directory down")
	d.err = boom
	if _, err := r.Resolve(ctx, []Identity{msisdn("491701234567")}); !errors.Is(err, boom) {
		t.Errorf("a directory failure must surface as such, not as unknown: %v", err)
	}
	st := r.Stats()
	if st.Unknown != 1 || st.Conflicts != 1 || st.Errors != 1 {
		t.Errorf("%+v", st)
	}
}

func TestConflictPreferFirst(t *testing.T) {
	d := &mapDir{m: map[Identity]string{msisdn("491701234567"): "acct-1", imsi("262011234567890"): "acct-2"}}
	r := NewResolver(d, ConflictPreferFirst)
	k, err := r.Resolve(context.Background(), []Identity{msisdn("491701234567"), imsi("262011234567890")})
	if err != nil || k != "acct-1" {
		t.Fatalf("%q %v", k, err)
	}
	if r.Stats().Conflicts != 1 {
		t.Fatal("the conflict must still be counted")
	}
}

func TestPassthroughResolver(t *testing.T) {
	r := NewPassthroughResolver()
	k, err := r.Resolve(context.Background(), []Identity{{MSISDN, "4917"}, {IMSI, "262"}})
	if err != nil || k != "4917" {
		t.Fatalf("passthrough takes the highest-precedence identifier as is: %q %v", k, err)
	}
}

// ---- cache -----------------------------------------------------------------

type clock struct{ ns atomic.Int64 }

func (c *clock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *clock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func TestCacheHitsMissesAndTTL(t *testing.T) {
	d := &mapDir{m: map[Identity]string{msisdn("491701234567"): "acct-1"}}
	clk := &clock{}
	clk.ns.Store(1_000_000_000)
	c := NewCachedDirectory(d, CacheConfig{TTL: 10 * time.Second, NegativeTTL: 2 * time.Second, Now: clk.now})
	ctx := context.Background()
	id := msisdn("491701234567")

	for i := 0; i < 5; i++ {
		if k, err := c.Lookup(ctx, id); err != nil || k != "acct-1" {
			t.Fatalf("%q %v", k, err)
		}
	}
	if d.calls.Load() != 1 {
		t.Fatalf("%d directory lookups for 5 reads", d.calls.Load())
	}

	// A change in the directory is invisible until the TTL passes.
	d.m[id] = "acct-2"
	clk.advance(9 * time.Second)
	if k, _ := c.Lookup(ctx, id); k != "acct-1" {
		t.Fatalf("stale entry expected before TTL, got %q", k)
	}
	clk.advance(2 * time.Second)
	if k, _ := c.Lookup(ctx, id); k != "acct-2" {
		t.Fatalf("expired entry must be refreshed, got %q", k)
	}
}

func TestCacheNegativeEntriesAreShortLived(t *testing.T) {
	d := &mapDir{m: map[Identity]string{}}
	clk := &clock{}
	clk.ns.Store(1_000_000_000)
	c := NewCachedDirectory(d, CacheConfig{TTL: time.Minute, NegativeTTL: 2 * time.Second, Now: clk.now})
	ctx := context.Background()
	id := msisdn("491709999999")

	for i := 0; i < 3; i++ {
		if _, err := c.Lookup(ctx, id); !errors.Is(err, ErrUnknown) {
			t.Fatal(err)
		}
	}
	if d.calls.Load() != 1 {
		t.Fatalf("an unknown identity must be remembered: %d lookups", d.calls.Load())
	}
	d.m[id] = "acct-9" // provisioned meanwhile
	clk.advance(3 * time.Second)
	if k, err := c.Lookup(ctx, id); err != nil || k != "acct-9" {
		t.Fatalf("%q %v", k, err)
	}
}

func TestCacheDoesNotRememberFailures(t *testing.T) {
	d := &mapDir{m: map[Identity]string{msisdn("491701234567"): "acct-1"}, err: errors.New("down")}
	c := NewCachedDirectory(d, CacheConfig{})
	ctx := context.Background()
	id := msisdn("491701234567")
	if _, err := c.Lookup(ctx, id); err == nil {
		t.Fatal("expected the failure")
	}
	d.err = nil
	if k, err := c.Lookup(ctx, id); err != nil || k != "acct-1" {
		t.Fatalf("recovery must be immediate: %q %v", k, err)
	}
	if d.calls.Load() != 2 {
		t.Fatal(d.calls.Load())
	}
}

func TestCacheCollapsesConcurrentMisses(t *testing.T) {
	d := &mapDir{m: map[Identity]string{msisdn("491701234567"): "acct-1"}, delay: 50 * time.Millisecond}
	c := NewCachedDirectory(d, CacheConfig{})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if k, err := c.Lookup(context.Background(), msisdn("491701234567")); err != nil || k != "acct-1" {
				t.Errorf("%q %v", k, err)
			}
		}()
	}
	wg.Wait()
	if d.calls.Load() != 1 {
		t.Fatalf("50 concurrent misses made %d directory lookups, want 1", d.calls.Load())
	}
}

func TestCacheWaiterHonoursItsOwnContext(t *testing.T) {
	d := &mapDir{m: map[Identity]string{msisdn("491701234567"): "acct-1"}, delay: 200 * time.Millisecond}
	c := NewCachedDirectory(d, CacheConfig{})
	go c.Lookup(context.Background(), msisdn("491701234567")) // leader
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Lookup(ctx, msisdn("491701234567")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a waiter must give up when its own context ends: %v", err)
	}
}

func TestCacheIsBoundedUnderAnIdentifierFlood(t *testing.T) {
	d := &mapDir{m: map[Identity]string{}}
	c := NewCachedDirectory(d, CacheConfig{MaxEntries: 640})
	for i := 0; i < 20_000; i++ {
		_, _ = c.Lookup(context.Background(), Identity{MSISDN, "49" + itoa(1_000_000+i)})
	}
	if n := c.Stats().Entries; n > 640+cacheShards {
		t.Fatalf("cache holds %d entries, bound is 640", n)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for ; n > 0; n /= 10 {
		i--
		b[i] = byte('0' + n%10)
	}
	return string(b[i:])
}

func TestCacheHitDoesNotAllocate(t *testing.T) {
	d := &mapDir{m: map[Identity]string{msisdn("491701234567"): "acct-1"}}
	c := NewCachedDirectory(d, CacheConfig{})
	r := NewPassthroughResolver()
	_ = r
	id := msisdn("491701234567")
	ctx := context.Background()
	_, _ = c.Lookup(ctx, id)
	if n := testing.AllocsPerRun(1000, func() { _, _ = c.Lookup(ctx, id) }); n != 0 {
		t.Fatalf("%v allocs on a cache hit", n)
	}
	res := NewResolver(c, ConflictReject)
	ids := []Identity{id, imsi("262011234567890")}
	d.m[imsi("262011234567890")] = "acct-1"
	_, _ = res.Resolve(ctx, ids)
	if n := testing.AllocsPerRun(1000, func() { _, _ = res.Resolve(ctx, ids) }); n != 0 {
		t.Fatalf("%v allocs resolving two cached identities", n)
	}
}

func TestCacheConcurrentUse(t *testing.T) {
	d := &mapDir{m: map[Identity]string{}}
	for i := 0; i < 100; i++ {
		d.m[msisdn("49170"+itoa(100000+i))] = "acct-" + itoa(i)
	}
	c := NewCachedDirectory(d, CacheConfig{MaxEntries: 128, TTL: time.Millisecond, NegativeTTL: time.Millisecond})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				n := (g*7 + i) % 150
				k, err := c.Lookup(context.Background(), msisdn("49170"+itoa(100000+n)))
				if n < 100 && (err != nil || k != "acct-"+itoa(n)) {
					t.Errorf("n=%d: %q %v", n, k, err)
					return
				}
				if n >= 100 && !errors.Is(err, ErrUnknown) {
					t.Errorf("n=%d: %q %v", n, k, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
