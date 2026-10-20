package telemetry

import (
	"runtime"
	"sync"
	"time"
)

// memStats caches runtime.ReadMemStats (it stops the world briefly) so a scrape
// that reads several gauges pays for it once.
type memStats struct {
	mu   sync.Mutex
	at   time.Time
	last runtime.MemStats
}

func (m *memStats) get() runtime.MemStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	if time.Since(m.at) > time.Second {
		runtime.ReadMemStats(&m.last)
		m.at = time.Now()
	}
	return m.last
}

// RegisterRuntime exposes the Go runtime gauges every service shares:
// goroutines, heap in use, and GC pause and cycle counts.
func RegisterRuntime(reg *Registry) {
	var ms memStats
	reg.GaugeFunc("go_goroutines", "Goroutines.", nil, func() float64 { return float64(runtime.NumGoroutine()) })
	reg.GaugeFunc("go_memstats_heap_alloc_bytes", "Heap bytes allocated and in use.", nil, func() float64 { return float64(ms.get().HeapAlloc) })
	reg.CounterFunc("go_gc_pause_seconds_total", "Total stop-the-world GC pause time.", nil, func() float64 { return float64(ms.get().PauseTotalNs) / 1e9 })
	reg.CounterFunc("go_gc_cycles_total", "Completed GC cycles.", nil, func() float64 { return float64(ms.get().NumGC) })
}
