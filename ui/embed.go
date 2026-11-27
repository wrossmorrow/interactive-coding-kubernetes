// Package ui embeds the decoupled session-ui SPA static assets.
// Imported by session-hub (and future local CLI tunnel), never by pi-sessiond.
package ui

import (
	"embed"
	"io/fs"
)

//go:embed static
var staticFS embed.FS

// FS returns the embedded static UI filesystem rooted at static/.
func FS() fs.FS {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	return sub
}
