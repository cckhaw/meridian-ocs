// Package ui serves the catalog editor: a single-page application written in plain
// HTML, CSS and JavaScript with no build step and no third-party code. It is
// embedded in the binary and served by the same listener as the REST API, so it
// is behind the same client-certificate authentication and the same
// authorisation rules: the console can do exactly what its user's certificate
// may do through the API, nothing more.
package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var assets embed.FS

// contentSecurityPolicy forbids everything except the console's own files:
// no inline script or style, no other origins, no framing.
const contentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; " +
	"base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

// Handler serves /ui/.
func Handler() http.Handler {
	sub, err := fs.Sub(assets, "static")
	if err != nil {
		panic(err) // the embed directive guarantees the directory
	}
	files := http.StripPrefix("/ui/", http.FileServer(http.FS(sub)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", contentSecurityPolicy)
		h.Set("X-Frame-Options", "DENY")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
}
