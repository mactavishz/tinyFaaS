package gateway

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	defaultRProxyPort  = "8000"
	defaultManagerPort = "8080"
)

// Gateway handles incoming requests and routes them to appropriate backends
type Gateway struct {
	rproxyPort  string
	managerPort string
	mode        string
	logger      *zap.Logger
}

// Option is a functional option for configuring Gateway
type Option func(*Gateway)

// WithRProxyPort sets the rproxy port
func WithRProxyPort(port string) Option {
	return func(g *Gateway) {
		g.rproxyPort = port
	}
}

// WithManagerPort sets the manager port
func WithManagerPort(port string) Option {
	return func(g *Gateway) {
		g.managerPort = port
	}
}

// WithDevMode enables development mode features (debug endpoints)
func WithMode(mode string) Option {
	return func(g *Gateway) {
		g.mode = mode
	}
}

// New creates a new Gateway instance
func New(logger *zap.Logger, opts ...Option) *Gateway {
	g := &Gateway{
		rproxyPort:  defaultRProxyPort,
		managerPort: defaultManagerPort,
		logger:      logger,
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

// rproxyAddr returns the full rproxy address
func (g *Gateway) rproxyAddr() string {
	return fmt.Sprintf("127.0.0.1:%s", g.rproxyPort)
}

// managerAddr returns the full manager address
func (g *Gateway) managerAddr() string {
	return fmt.Sprintf("127.0.0.1:%s", g.managerPort)
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
		g.logger.Error("failed to create proxy request", zap.Error(err))
		return
	}

	// Copy headers from original request
	proxyReq.Header = r.Header.Clone()

	g.logger.Debug("proxying request",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("targetAddr", targetURL),
	)

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(proxyReq)
	if err != nil {
		http.Error(w, "Failed to proxy request", http.StatusBadGateway)
		g.logger.Error("failed to proxy request", zap.Error(err))
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
		// Extract source IP and set X-Faas-Source-Ip header
		sourceIP := g.extractSourceIP(r)
		r.Header.Set("X-Faas-Source-Ip", sourceIP)

		// Generate X-Faas-Request-Id if not present
		requestID := r.Header.Get("X-Faas-Request-Id")
		if strings.TrimSpace(requestID) == "" {
			requestID = uuid.New().String()
			r.Header.Set("X-Faas-Request-Id", requestID)
		}

		g.logger.Debug("Invoke middleware",
			zap.String("path", r.URL.Path),
			zap.String("X-Faas-Source-Ip", r.Header.Get("X-Faas-Source-Ip")),
			zap.String("X-Faas-Request-Id", r.Header.Get("X-Faas-Request-Id")))

		next(w, r)
	}
}

// HandleFunctionInvoke handles /fn/* requests to rproxy
func (g *Gateway) HandleFunctionInvoke(w http.ResponseWriter, r *http.Request) {
	// Rewrite path: /fn/funcname -> /invoke/funcname
	r.URL.Path = "/invoke" + strings.TrimPrefix(r.URL.Path, "/fn")
	g.proxyRequest(w, r, g.rproxyAddr())
}

// HandleSystemScaleUp handles /system/scale-up requests (restricted to localhost)
func (g *Gateway) HandleSystemScaleUp(w http.ResponseWriter, r *http.Request) {
	sourceIP := g.extractSourceIP(r)
	if sourceIP != "127.0.0.1" && sourceIP != "::1" && sourceIP != "localhost" {
		http.Error(w, "Forbidden: scale-up endpoint is restricted to internal services only", http.StatusForbidden)
		g.logger.Warn("unauthorized access to scale-up endpoint", zap.String("sourceIP", sourceIP))
		return
	}

	r.URL.Path = "/scale-up"
	g.proxyRequest(w, r, g.managerAddr())
}

// HandleSystemHeartbeat handles /system/heartbeat requests (restricted to localhost)
func (g *Gateway) HandleSystemHeartbeat(w http.ResponseWriter, r *http.Request) {
	sourceIP := g.extractSourceIP(r)
	if sourceIP != "127.0.0.1" && sourceIP != "::1" && sourceIP != "localhost" {
		http.Error(w, "Forbidden: heartbeat endpoint is restricted to internal services only", http.StatusForbidden)
		g.logger.Warn("unauthorized access to heartbeat endpoint", zap.String("sourceIP", sourceIP))
		return
	}

	r.URL.Path = "/heartbeat"
	g.proxyRequest(w, r, g.managerAddr())
}

// HandleSystemOther handles other /system/* requests to manager
func (g *Gateway) HandleSystemOther(w http.ResponseWriter, r *http.Request) {
	// Strip /system prefix before forwarding to manager
	r.URL.Path = strings.TrimPrefix(r.URL.Path, "/system")
	if r.URL.Path == "" {
		r.URL.Path = "/"
	}
	g.proxyRequest(w, r, g.managerAddr())
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

// HandleCallgraph handles /debug/callgraph requests (proxies to rproxy)
func (g *Gateway) HandleCallgraph(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/callgraph"
	g.proxyRequest(w, r, g.rproxyAddr())
}

// HandleCallgraphFunction handles /debug/callgraph/function/* requests (proxies to rproxy)
func (g *Gateway) HandleCallgraphFunction(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/callgraph/function/" + strings.TrimPrefix(r.URL.Path, "/system/callgraph/function/")
	g.proxyRequest(w, r, g.rproxyAddr())
}

// HandleCallgraphEdge handles /debug/callgraph/edge requests (proxies to rproxy)
func (g *Gateway) HandleCallgraphEdge(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/callgraph/edge"
	g.proxyRequest(w, r, g.rproxyAddr())
}

// RegisterHandlers registers all gateway handlers on the provided mux
func (g *Gateway) RegisterHandlers(mux *http.ServeMux) {
	// Function invocation endpoint
	mux.HandleFunc("/fn/", g.InvokeMiddleware(g.HandleFunctionInvoke))

	// System endpoints with access control
	mux.HandleFunc("/system/scale-up", g.HandleSystemScaleUp)
	mux.HandleFunc("/system/heartbeat", g.HandleSystemHeartbeat)

	// Other system endpoints
	mux.HandleFunc("/system/", g.HandleSystemOther)

	// Debug endpoints (only in development mode)
	if g.IsDev() {
		g.logger.Info("registering development mode only endpoints")
		mux.HandleFunc("/system/callgraph/function/", g.HandleCallgraphFunction)
		mux.HandleFunc("/system/callgraph/edge", g.HandleCallgraphEdge)
		mux.HandleFunc("/system/callgraph", g.HandleCallgraph)
	}

	// Health check
	mux.HandleFunc("/health", g.HandleHealth)

	// Root
	mux.HandleFunc("/", g.HandleRoot)
}

// GetRProxyPort returns the rproxy port
func (g *Gateway) GetRProxyPort() string {
	return g.rproxyPort
}

// GetManagerPort returns the manager port
func (g *Gateway) GetManagerPort() string {
	return g.managerPort
}
