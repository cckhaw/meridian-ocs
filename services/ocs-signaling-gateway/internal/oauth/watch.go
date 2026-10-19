package oauth

import (
	"context"
	"os"
	"time"
)

// WatchKeyFile reloads the NRF key file when it changes, so a key rotation
// needs no restart. A file that cannot be parsed, or holds no usable key, is
// logged and ignored: the previous keys stay in force rather than locking
// every consumer out. It returns when ctx ends.
func WatchKeyFile(ctx context.Context, path string, v *Verifier, every time.Duration, logf func(format string, args ...any)) {
	var lastMod time.Time
	var lastSize int64 = -1
	if st, err := os.Stat(path); err == nil {
		lastMod, lastSize = st.ModTime(), st.Size()
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		st, err := os.Stat(path)
		if err != nil {
			logf("oauth: cannot stat %s: %v (keeping the current keys)", path, err)
			continue
		}
		if st.ModTime().Equal(lastMod) && st.Size() == lastSize {
			continue
		}
		lastMod, lastSize = st.ModTime(), st.Size()
		ks, skipped, err := LoadKeySet(path, v.cfg.Now())
		if err != nil {
			logf("oauth: not reloading %s: %v (keeping the current keys)", path, err)
			continue
		}
		for _, s := range skipped {
			logf("oauth: skipped while reloading %s: %s", path, s)
		}
		if err := v.SetKeys(ks); err != nil {
			logf("oauth: not reloading %s: %v", path, err)
			continue
		}
		logf("oauth: reloaded %d key(s) from %s", ks.Len(), path)
	}
}
