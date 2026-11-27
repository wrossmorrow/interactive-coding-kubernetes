package hub

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/wrossmorrow/k8s-agents/pkg/sessions"
)

func (s *Server) handleSessionProxy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	endpoint, ok := s.spawner.Endpoint(id)
	if !ok {
		status := sessions.StatusPaused
		if meta, err := s.loadMetadata(r.Context(), id); err == nil && meta != nil && meta.Status != "" {
			status = meta.Status
		}
		writeJSON(w, http.StatusConflict, map[string]any{
			"success": false,
			"error":   "session is not active",
			"status":  status,
		})
		return
	}

	target, err := url.Parse(endpoint)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "error": "invalid daemon endpoint"})
		return
	}

	token, _ := s.tokens.Get(id)
	prefix := "/s/" + id

	proxy := &httputil.ReverseProxy{
		FlushInterval: -1, // Required for SSE streaming without buffering.
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.URL.Path = strings.TrimPrefix(pr.In.URL.Path, prefix)
			if pr.Out.URL.Path == "" {
				pr.Out.URL.Path = "/"
			}
			pr.Out.URL.RawPath = ""
			pr.Out.Host = target.Host
			if token != "" {
				pr.Out.Header.Set("Authorization", "Bearer "+token)
			}
		},
		ErrorHandler: func(rw http.ResponseWriter, _ *http.Request, perr error) {
			writeJSON(rw, http.StatusBadGateway, map[string]any{"success": false, "error": perr.Error()})
		},
	}
	proxy.ServeHTTP(w, r)
}
