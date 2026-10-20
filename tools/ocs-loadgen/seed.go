package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// provClient talks to the provisioning REST API.
type provClient struct {
	base string
	http *http.Client
}

func newProvClient(base string, tf tlsFiles) (*provClient, error) {
	tr := &http.Transport{MaxIdleConnsPerHost: 64}
	if strings.HasPrefix(base, "https://") {
		cfg, err := tf.config()
		if err != nil {
			return nil, err
		}
		tr.TLSClientConfig = cfg
	} else if !strings.HasPrefix(base, "http://") {
		return nil, fmt.Errorf("--prov %q: want http://host:port or https://host:port", base)
	}
	return &provClient{base: strings.TrimRight(base, "/"), http: &http.Client{Transport: tr, Timeout: 15 * time.Second}}, nil
}

func (p *provClient) do(ctx context.Context, method, path, idem string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.base+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, b, err
}

// submit sends a command and waits until it is applied.
func (p *provClient) submit(ctx context.Context, path, idem string, body any) error {
	status, b, err := p.do(ctx, http.MethodPost, path, idem, body)
	if err != nil {
		return err
	}
	if status != http.StatusAccepted && status != http.StatusOK {
		return fmt.Errorf("POST %s: %d %s", path, status, strings.TrimSpace(string(b)))
	}
	var a struct {
		CommandID string `json:"commandId"`
		State     string `json:"state"`
	}
	if err := json.Unmarshal(b, &a); err != nil || a.CommandID == "" {
		return fmt.Errorf("POST %s: no commandId in %q", path, b)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		status, b, err := p.do(ctx, http.MethodGet, "/v1/commands/"+a.CommandID, "", nil)
		if err != nil {
			return err
		}
		if status == http.StatusOK {
			var r struct{ State, Message string }
			_ = json.Unmarshal(b, &r)
			switch strings.ToLower(r.State) {
			case "applied":
				return nil
			case "failed", "rejected", "expired":
				return fmt.Errorf("command %s %s: %s", a.CommandID, r.State, r.Message)
			case "pending_approval":
				return fmt.Errorf("command %s is waiting for approval: the load test cannot seed with this policy", a.CommandID)
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("command %s was not applied within a minute", a.CommandID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// bucketBalance reads one bucket of a subscriber.
func (p *provClient) bucketBalance(ctx context.Context, key, bucket string) (int64, error) {
	status, b, err := p.do(ctx, http.MethodGet, "/v1/subscribers/"+key, "", nil)
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("GET subscriber %s: %d", key, status)
	}
	var s struct {
		Buckets []struct {
			ID      string `json:"id"`
			Balance int64  `json:"balance"`
		} `json:"buckets"`
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return 0, err
	}
	for _, x := range s.Buckets {
		if x.ID == bucket {
			return x.Balance, nil
		}
	}
	return 0, fmt.Errorf("subscriber %s has no bucket %q (does the offering contain it?)", key, bucket)
}

// subscriber is one test subscriber and what the run did to it.
type subscriber struct {
	key, imsi string
	initial   int64 // balance of the checked bucket before the run

	mu       sync.Mutex
	used     int64 // usage the gateway confirmed it settled
	held     int64 // granted, but the session was not confirmed released
	unsure   int64 // upper bound of what requests with an unknown outcome may have moved
	sessions int
	pdu      atomic.Uint32 // PDU session identifiers handed out, so concurrent sessions never look alike
	trail    []string      // what happened, in order; only kept with --audit
}

func (s *subscriber) note(audit bool, f string, a ...any) {
	if audit {
		s.mu.Lock()
		s.trail = append(s.trail, fmt.Sprintf("%s  ", time.Now().Format("15:04:05.000"))+fmt.Sprintf(f, a...))
		s.mu.Unlock()
	}
}

// seed creates n prepaid subscribers with the offering and records each one's starting balance.
func seed(ctx context.Context, p *provClient, c config, runID string, out func(string, ...any)) ([]*subscriber, error) {
	subs := make([]*subscriber, c.Subscribers)
	for i := range subs {
		subs[i] = &subscriber{
			key:  fmt.Sprintf("lg-%s-%05d", runID, i),
			imsi: fmt.Sprintf("00101%05s%05d", runID, i),
		}
	}
	started := time.Now()
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		ferr error
		next int
		done int
	)
	for w := 0; w < c.SeedConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if ferr != nil || next >= len(subs) {
					mu.Unlock()
					return
				}
				s := subs[next]
				next++
				mu.Unlock()
				err := p.submit(ctx, "/v1/subscribers", s.key+"-create", map[string]any{
					"subscriberKey": s.key, "paymentType": "prepaid",
					"identifiers": []map[string]string{{"type": "imsi", "value": s.imsi}},
				})
				if err == nil {
					err = p.submit(ctx, "/v1/subscribers/"+s.key+"/offering", s.key+"-offering", map[string]any{"offering": c.Offering})
				}
				if err == nil {
					s.initial, err = p.bucketBalance(ctx, s.key, c.Bucket)
				}
				mu.Lock()
				if err != nil && ferr == nil {
					ferr = fmt.Errorf("seeding %s: %w", s.key, err)
				}
				done++
				if done%500 == 0 {
					out("  seeded %d of %d\n", done, len(subs))
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ferr != nil {
		return nil, ferr
	}
	out("seeded %d subscribers in %s (%.0f/s)\n", len(subs), time.Since(started).Round(time.Millisecond),
		float64(len(subs))/time.Since(started).Seconds())
	return subs, nil
}
