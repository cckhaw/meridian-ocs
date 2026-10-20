package main

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestHistIndexIsMonotonicAndBoundsHold(t *testing.T) {
	prev := -1
	for us := uint64(0); us < 5_000_000; us += 1 + us/200 {
		i := histIndex(us)
		if i < prev {
			t.Fatalf("index went backwards at %d µs", us)
		}
		prev = i
		if up := histUpper(i); up < us || float64(up-us) > float64(us)*0.02+1 {
			t.Fatalf("%d µs: bucket upper edge %d is not within 2 %%", us, up)
		}
	}
}

func TestHistQuantilesMatchSortedData(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	var h Hist
	var all []time.Duration
	for i := 0; i < 200_000; i++ {
		d := time.Duration(200+r.ExpFloat64()*1500) * time.Microsecond
		all = append(all, d)
		h.Record(d)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		want := all[int(float64(len(all))*q)-1]
		got := h.Quantile(q)
		if got < want || float64(got-want) > float64(want)*0.03 {
			t.Errorf("q%.3f: got %v, exact %v (must be at or just above)", q, got, want)
		}
	}
	if h.Max() != all[len(all)-1]/time.Microsecond*time.Microsecond {
		t.Errorf("max %v, exact %v", h.Max(), all[len(all)-1])
	}
	if h.Quantile(1) > h.Max() {
		t.Error("a quantile exceeds the maximum")
	}
}

func TestHistEmpty(t *testing.T) {
	var h Hist
	if h.Quantile(0.99) != 0 || h.Mean() != 0 || h.Count() != 0 {
		t.Fatal("an empty histogram must report zeros")
	}
}
