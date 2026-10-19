package nchf

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Handler cost per request (JSON decode, mapping, response encode) with a
// stubbed rating call, excluding sockets, TLS and HTTP/2 framing.
func BenchmarkUpdateHandler(b *testing.B) {
	h := newHandler(b, raterFunc(grantAll), nil, nil)
	body := mustJSON(baseUpdate(2, 400, 1000))
	path := prefix + "/chargingdata/r1.MjYyMDExMjM0NTY3ODkw.0123456789abcdef/update"
	r := httptest.NewRequest(http.MethodPost, "http://ocs.example.net"+path, nil)
	r.Header.Set("Content-Type", "application/json")
	w := &discardWriter{h: http.Header{}}
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Body = io.NopCloser(strings.NewReader(body))
		clear(w.h)
		h.ServeHTTP(w, r)
		if w.code != 200 {
			b.Fatal(w.code)
		}
	}
}

type discardWriter struct {
	h    http.Header
	code int
}

func (d *discardWriter) Header() http.Header         { return d.h }
func (d *discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (d *discardWriter) WriteHeader(c int)           { d.code = c }
