package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"log/slog"

	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
	"golang.org/x/sync/singleflight"
)

const (
	defaultTinyFaaSPort = "8000"
	// EdgeKindHeader carries sync/async classification from gateway entry to
	// the merged server's invocation router. The router uses it to tag
	// callgraph edges so the prewarm scheduler can deprioritise async fan-out.
	EdgeKindHeader = "X-Tinyfaas-Edge-Kind"
	// scaleUpRequestTimeout bounds how long the gateway waits on the merged
	// server's /scale-up endpoint. Shorter than the server-side cold-start
	// budget so a stuck server does not stall the user request indefinitely.
	scaleUpRequestTimeout = 60 * time.Second
)

// Gateway handles incoming requests and routes them to appropriate backends
type Gateway struct {
	tinyfaasPort string
	mode         string
	logger       *slog.Logger
	stats        *functionStatsStore
	httpClient   *http.Client
	// scaleSF dedupes concurrent /scale-up calls per function name so a burst
	// of requests for a scaled-down function only triggers one scale-up.
	scaleSF singleflight.Group
}

// Option is a functional option for configuring Gateway
type Option func(*Gateway)

func WithTinyFaaSPort(port string) Option {
	return func(g *Gateway) {
		g.tinyfaasPort = port
	}
}

// WithDevMode enables development mode features (debug endpoints)
func WithMode(mode string) Option {
	return func(g *Gateway) {
		g.mode = mode
	}
}

// New creates a new Gateway instance
func New(logger *slog.Logger, opts ...Option) *Gateway {
	g := &Gateway{
		tinyfaasPort: defaultTinyFaaSPort,
		logger:       logger,
		stats:        NewFunctionStatsStore(),
		httpClient:   newHTTPClient(),
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 50,
			MaxConnsPerHost:     100,
			IdleConnTimeout:     120 * time.Second,
			DisableCompression:  true,
		},
	}
}

func (g *Gateway) IsDev() bool {
	return strings.ToLower(g.mode) == "development"
}

func (g *Gateway) IsProd() bool {
	return strings.ToLower(g.mode) == "production"
}

// tinyfaasAddr returns the full internal merged service address.
func (g *Gateway) tinyfaasAddr() string {
	return fmt.Sprintf("127.0.0.1:%s", g.tinyfaasPort)
}

// extractSourceIP extracts the real client IP from the request
func (g *Gateway) extractSourceIP(r *http.Request) string {
	// Check X-Forwarded-For header first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in the chain
		if before, _, ok := strings.Cut(xff, ","); ok {
			return strings.TrimSpace(before)
		}
		return strings.TrimSpace(xff)
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-Ip"); xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	if idx := strings.LastIndex(r.RemoteAddr, ":"); idx != -1 {
		return r.RemoteAddr[:idx]
	}
	return r.RemoteAddr
}

// proxyRequest forwards the request to the target address
func (g *Gateway) proxyRequest(w http.ResponseWriter, r *http.Request, targetAddr string) {
	// Create target URL
	targetURL := fmt.Sprintf("http://%s%s", targetAddr, r.URL.Path)
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	// Create proxy request
	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, "Failed to create proxy request", http.StatusInternalServerError)
		g.logger.Error("failed to create proxy request", "err", err)
		return
	}

	// Copy headers from original request
	proxyReq.Header = r.Header.Clone()

	g.logger.Debug("forwarding request",
		"method", r.Method,
		"path", r.URL.Path,
		"targetAddr", targetURL,
	)

	// Send the request
	resp, err := g.httpClient.Do(proxyReq)
	if err != nil {
		http.Error(w, "Failed to proxy request", http.StatusBadGateway)
		g.logger.Error("failed to proxy request", "err", err)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Copy status code
	w.WriteHeader(resp.StatusCode)

	// Copy response body
	io.Copy(w, resp.Body)
}

// InvokeMiddleware returns a middleware that handles gateway-specific headers
func (g *Gateway) InvokeMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Extract source IP and set X-Source-Ip header
		sourceIP := g.extractSourceIP(r)
		r.Header.Set("X-Source-Ip", sourceIP)
		appendForwardedFor(r.Header, r.RemoteAddr)

		// Generate X-Call-Id if not present
		callID := r.Header.Get("X-Call-Id")
		if strings.TrimSpace(callID) == "" {
			callID = uuid.New().String()
			r.Header.Set("X-Call-Id", callID)
		}

		r.Header.Add("X-Start-Time", fmt.Sprintf("%d", start.UTC().UnixNano()))
		w.Header().Add("X-Start-Time", fmt.Sprintf("%d", start.UTC().UnixNano()))

		g.logger.Debug("Invoke middleware",
			"path", r.URL.Path,
			"X-Source-Ip", r.Header.Get("X-Source-Ip"),
			"X-Call-Id", r.Header.Get("X-Call-Id"))

		next(w, r)
	}
}

func appendForwardedFor(header http.Header, remoteAddr string) {
	remoteAddr = strings.TrimSpace(remoteAddr)
	if remoteAddr == "" {
		return
	}

	current := strings.TrimSpace(header.Get("X-Forwarded-For"))
	if current == "" {
		header.Set("X-Forwarded-For", remoteAddr)
		return
	}

	header.Set("X-Forwarded-For", current+", "+remoteAddr)
}

// HandleFunctionInvoke handles /fn/* requests
func (g *Gateway) HandleFunctionInvoke(w http.ResponseWriter, r *http.Request) {
	// Rewrite path: /fn/funcname -> /invoke/funcname
	r.Header.Del("X-Tinyfaas-Async")
	r.Header.Set(EdgeKindHeader, callgraph.EdgeKindSync.String())
	r.URL.Path = "/invoke" + strings.TrimPrefix(r.URL.Path, "/fn")
	g.proxyRequest(w, r, g.tinyfaasAddr())
}

func (g *Gateway) HandleAsyncFunctionInvoke(w http.ResponseWriter, r *http.Request) {
	r.Header.Del("X-Tinyfaas-Async")
	// Tag the edge as async at submission time. The header is preserved into
	// the queue payload by the server's /async-invoke handler, so the queue
	// worker dispatch through /queue-fn ends up recording the edge correctly.
	r.Header.Set(EdgeKindHeader, callgraph.EdgeKindAsync.String())
	r.URL.Path = "/async-invoke" + strings.TrimPrefix(r.URL.Path, "/async-fn")
	g.proxyRequest(w, r, g.tinyfaasAddr())
}

// HandleQueueFunctionInvoke handles /queue-fn/* requests dispatched by the
// async queue worker. It applies the same scaling middleware as /fn/* so
// dequeued invocations enjoy gateway-level coalescing and give downstream
// prewarms time to win the race, but it does not record gateway-side
// invocation stats since async invocations are already counted server-side
// via /invoke.
func (g *Gateway) HandleQueueFunctionInvoke(w http.ResponseWriter, r *http.Request) {
	r.Header.Del("X-Tinyfaas-Async")
	// Force async classification regardless of any incoming value; the only
	// way a request reaches /queue-fn is via async dispatch.
	r.Header.Set(EdgeKindHeader, callgraph.EdgeKindAsync.String())
	r.URL.Path = "/invoke" + strings.TrimPrefix(r.URL.Path, "/queue-fn")
	g.proxyRequest(w, r, g.tinyfaasAddr())
}

// scalingMiddleware ensures the requested function is scaled up before the
// inner handler forwards the request to the merged server. It uses
// singleflight so concurrent demand requests for the same function coalesce
// into a single /scale-up call
func (g *Gateway) scalingMiddleware(prefix string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			name := extractFunctionName(r.URL.Path, prefix)
			if name == "" {
				next(w, r)
				return
			}
			status, err := g.ensureScaled(r.Context(), name)
			if err != nil {
				g.logger.Error("scale-up failed via gateway",
					"function", name,
					"err", err)
				http.Error(w, "scale-up failed", http.StatusBadGateway)
				return
			}
			switch status {
			case http.StatusOK:
				next(w, r)
				return
			case http.StatusNotFound:
				http.Error(w, "function not found", http.StatusNotFound)
				return
			default:
				g.logger.Warn("scale-up returned unexpected status",
					"function", name,
					"status", status)
				http.Error(w, "scale-up unavailable", http.StatusServiceUnavailable)
			}
		}
	}
}

// ensureScaled posts to the merged server's /scale-up/{name} endpoint and
// returns its HTTP status. Concurrent calls for the same function name are
// deduplicated via singleflight so only one scale-up is in flight per
// function at any time.
func (g *Gateway) ensureScaled(ctx context.Context, name string) (int, error) {
	type result struct {
		status int
		err    error
	}
	v, err, _ := g.scaleSF.Do("scale-up:"+name, func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(ctx, scaleUpRequestTimeout)
		defer cancel()
		url := fmt.Sprintf("http://%s/scale-up/%s", g.tinyfaasAddr(), name)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		if err != nil {
			return result{err: err}, nil
		}
		resp, err := g.httpClient.Do(req)
		if err != nil {
			return result{err: err}, nil
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return result{status: resp.StatusCode}, nil
	})
	if err != nil {
		return 0, err
	}
	res := v.(result)
	return res.status, res.err
}

// extractFunctionName trims a known prefix and returns the function name from
// a path. Returns "" if the path doesn't carry a function segment.
func extractFunctionName(path, prefix string) string {
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	rest = strings.TrimPrefix(rest, "/")
	if rest == "" {
		return ""
	}
	if idx := strings.Index(rest, "/"); idx >= 0 {
		rest = rest[:idx]
	}
	return rest
}

// HandleSystemOther handles other /system/* requests
func (g *Gateway) HandleSystemOther(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		trimmedPath := strings.TrimPrefix(r.URL.Path, "/system")
		if trimmedPath == "/delete" {
			g.resetStatsFromDeleteRequest(r)
		}
	}
	// Strip /system prefix before forwarding to the merged service
	r.URL.Path = strings.TrimPrefix(r.URL.Path, "/system")
	if r.URL.Path == "" {
		r.URL.Path = "/"
	}
	g.proxyRequest(w, r, g.tinyfaasAddr())
}

func (g *Gateway) resetStatsFromDeleteRequest(r *http.Request) {
	if r.Body == nil {
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	var payload struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return
	}
	g.resetFunctionStats(payload.Name)
}

// HandleHealth handles /health requests
func (g *Gateway) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// HandleRoot handles / requests
func (g *Gateway) HandleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("tinyFaaS Gateway"))
		return
	}
	http.NotFound(w, r)
}

// HandleCallgraph handles /debug/callgraph requests.
func (g *Gateway) HandleCallgraph(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/callgraph"
	g.proxyRequest(w, r, g.tinyfaasAddr())
}

// HandleCallgraphFunction handles /debug/callgraph/function/* requests.
func (g *Gateway) HandleCallgraphFunction(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/callgraph/function/" + strings.TrimPrefix(r.URL.Path, "/system/callgraph/function/")
	g.proxyRequest(w, r, g.tinyfaasAddr())
}

// HandleCallgraphEdge handles /debug/callgraph/edge requests.
func (g *Gateway) HandleCallgraphEdge(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/callgraph/edge"
	g.proxyRequest(w, r, g.tinyfaasAddr())
}

func (g *Gateway) HandleFunctionStatsProxy(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/stats/function/" + strings.TrimPrefix(r.URL.Path, "/system/stats/function/")
	g.proxyRequest(w, r, g.tinyfaasAddr())
}

// RegisterHandlers registers all gateway handlers on the provided mux
func (g *Gateway) RegisterHandlers(mux *http.ServeMux) {
	// Sync user-facing path:
	//   recordInvocationStats -> InvokeMiddleware -> scalingMiddleware -> HandleFunctionInvoke
	// Stats are recorded for user-facing /fn/ requests only. Scaling middleware
	// runs after invoke headers are injected so X-Call-Id etc. are propagated
	// to the merged server's /scale-up call as part of the gateway hop.
	syncScaling := g.scalingMiddleware("/fn/")
	mux.Handle("/fn/", g.recordInvocationStats(g.InvokeMiddleware(syncScaling(g.HandleFunctionInvoke))))

	// Async submission path: enqueue only, no scale-up needed.
	mux.Handle("/async-fn/", g.InvokeMiddleware(g.HandleAsyncFunctionInvoke))

	// Async dispatch path used by the queue worker. Has scaling middleware so
	// dequeued invocations route through the same coalescing as sync /fn/,
	// but skips recordInvocationStats (already counted at /async-fn/ side at
	// the server level via /invoke).
	queueScaling := g.scalingMiddleware("/queue-fn/")
	mux.Handle("/queue-fn/", g.InvokeMiddleware(queueScaling(g.HandleQueueFunctionInvoke)))

	mux.HandleFunc("/system/stats/function/", g.HandleFunctionStatsProxy)

	// System endpoints
	mux.HandleFunc("/system/", g.HandleSystemOther)

	mux.HandleFunc("/system/callgraph/function/", g.HandleCallgraphFunction)
	mux.HandleFunc("/system/callgraph/edge", g.HandleCallgraphEdge)
	mux.HandleFunc("/system/callgraph", g.HandleCallgraph)

	// Health check
	mux.HandleFunc("/health", g.HandleHealth)

	// Root
	mux.HandleFunc("/", g.HandleRoot)
}

// GetTinyFaaSPort returns the merged tinyFaaS service port.
func (g *Gateway) GetTinyFaaSPort() string {
	return g.tinyfaasPort
}
