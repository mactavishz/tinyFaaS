package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"log/slog"
)

const (
	defaultServerPort = "8000"
)

// Gateway handles incoming requests and routes them to the merged tinyFaaS server
type Gateway struct {
	serverPort string
	mode       string
	logger     *slog.Logger
	stats      *functionStatsStore
}

// Option is a functional option for configuring Gateway
type Option func(*Gateway)

// WithServerPort sets the merged tinyFaaS server port
func WithServerPort(port string) Option {
	return func(g *Gateway) {
		g.serverPort = port
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
		serverPort: defaultServerPort,
		logger:     logger,
		stats:      NewFunctionStatsStore(),
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func (g *Gateway) IsDev() bool {
	return strings.ToLower(g.mode) == "development"
}

func (g *Gateway) IsProd() bool {
	return strings.ToLower(g.mode) == "production"
}

// serverAddr returns the full merged server address
func (g *Gateway) serverAddr() string {
	return fmt.Sprintf("127.0.0.1:%s", g.serverPort)
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

	g.logger.Info("Forwarding request",
		"method", r.Method,
		"path", r.URL.Path,
		"targetAddr", targetURL,
	)

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(proxyReq)
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

// HandleFunctionInvoke handles /fn/* requests to the server
func (g *Gateway) HandleFunctionInvoke(w http.ResponseWriter, r *http.Request) {
	// Rewrite path: /fn/funcname -> /invoke/funcname
	r.URL.Path = "/invoke" + strings.TrimPrefix(r.URL.Path, "/fn")
	g.proxyRequest(w, r, g.serverAddr())
}

// HandleSystemOther handles other /system/* requests to the server
func (g *Gateway) HandleSystemOther(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		trimmedPath := strings.TrimPrefix(r.URL.Path, "/system")
		if trimmedPath == "/delete" {
			g.resetStatsFromDeleteRequest(r)
		}
	}
	// Strip /system prefix before forwarding to the server
	r.URL.Path = strings.TrimPrefix(r.URL.Path, "/system")
	if r.URL.Path == "" {
		r.URL.Path = "/"
	}
	g.proxyRequest(w, r, g.serverAddr())
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

// HandleCallgraph handles /system/callgraph requests (proxies to the server)
func (g *Gateway) HandleCallgraph(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/callgraph"
	g.proxyRequest(w, r, g.serverAddr())
}

// HandleCallgraphFunction handles /system/callgraph/function/* requests (proxies to the server)
func (g *Gateway) HandleCallgraphFunction(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/callgraph/function/" + strings.TrimPrefix(r.URL.Path, "/system/callgraph/function/")
	g.proxyRequest(w, r, g.serverAddr())
}

// HandleCallgraphEdge handles /system/callgraph/edge requests (proxies to the server)
func (g *Gateway) HandleCallgraphEdge(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/callgraph/edge"
	g.proxyRequest(w, r, g.serverAddr())
}

// RegisterHandlers registers all gateway handlers on the provided mux
func (g *Gateway) RegisterHandlers(mux *http.ServeMux) {
	// Function invocation endpoint
	mux.Handle("/fn/", g.recordInvocationStats(g.InvokeMiddleware(g.HandleFunctionInvoke)))

	// Function stats endpoint (served by the gateway itself)
	mux.HandleFunc("/system/stats/function/", g.HandleFunctionStats)

	// Other system endpoints
	mux.HandleFunc("/system/", g.HandleSystemOther)

	mux.HandleFunc("/system/callgraph/function/", g.HandleCallgraphFunction)
	mux.HandleFunc("/system/callgraph/edge", g.HandleCallgraphEdge)
	mux.HandleFunc("/system/callgraph", g.HandleCallgraph)

	// Health check
	mux.HandleFunc("/health", g.HandleHealth)

	// Root
	mux.HandleFunc("/", g.HandleRoot)
}

// GetServerPort returns the merged server port
func (g *Gateway) GetServerPort() string {
	return g.serverPort
}
