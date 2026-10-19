package engine

import "time"

// wheel is a single-owner timing wheel with one-second slots. It is only used
// from its worker goroutine, so it needs no locking. Entries are subscriber
// ids whose reservations should be swept once their deadline passes.
type wheel struct {
	slots [][]string
	cur   int64 // last second fully processed (unix seconds)
}

func newWheel(span time.Duration, now time.Time) *wheel {
	return &wheel{slots: make([][]string, int(span/time.Second)), cur: now.Unix()}
}

// add schedules sub for processing at `at`. Deadlines in the past run on the
// next tick; deadlines beyond the span are clamped to it.
func (w *wheel) add(sub string, at time.Time) {
	sec := at.Unix()
	if sec <= w.cur {
		sec = w.cur + 1
	}
	if max := w.cur + int64(len(w.slots)) - 1; sec > max {
		sec = max
	}
	i := int(sec % int64(len(w.slots)))
	w.slots[i] = append(w.slots[i], sub)
}

// advance calls fn for every entry whose second is <= now and clears them.
// fn may call add (entries land in a later second, never the one running).
func (w *wheel) advance(now time.Time, fn func(sub string)) {
	target := now.Unix()
	if target-w.cur > int64(len(w.slots)) { // long pause: everything is due
		w.cur = target - int64(len(w.slots))
	}
	for w.cur < target {
		w.cur++
		i := int(w.cur % int64(len(w.slots)))
		due := w.slots[i]
		w.slots[i] = nil
		for _, sub := range due {
			fn(sub)
		}
	}
}
