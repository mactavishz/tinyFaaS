package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/google/uuid"
)

const (
	rproxyAddr  = "localhost:8000"
	managerAddr = "localhost:8080"
)

// extractSourceIP extracts the real client IP from the request
func extractSourceIP(r *http.Request) string {
	// Check X-Forwarded-For header first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in the chain
		if idx := strings.Index(xff, ","); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	if idx := strings.LastIndex(r.RemoteAddr, ":"); idx != -1 {
		return r.RemoteAddr[:idx]
	}
	return r.RemoteAddr
}

// proxyRequest forwards the request to the target address
func proxyRequest(w http.ResponseWriter, r *http.Request, targetAddr string) {
	// Create target URL
	targetURL := fmt.Sprintf("http://%s%s", targetAddr, r.URL.Path)
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	log.Printf("Proxying %s %s -> %s", r.Method, r.URL.Path, targetURL)

	// Create proxy request
	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, "Failed to create proxy request", http.StatusInternalServerError)
		log.Printf("Error creating proxy request: %v", err)
		return
	}

	// Copy headers from original request
	for key, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// Log the headers being forwarded
	log.Printf("Forwarding headers: X-FaaS-Source-IP=%s, X-FaaS-Request-ID=%s",
		proxyReq.Header.Get("X-FaaS-Source-IP"), proxyReq.Header.Get("X-FaaS-Request-ID"))

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(proxyReq)
	if err != nil {
		http.Error(w, "Failed to proxy request", http.StatusBadGateway)
		log.Printf("Error proxying request: %v", err)
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

// gatewayMiddleware adds gateway-specific headers
func gatewayMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract source IP and set X-FaaS-Source-IP header
		sourceIP := extractSourceIP(r)
		r.Header.Set("X-FaaS-Source-IP", sourceIP)

		// Generate X-FaaS-Request-ID if not present
		requestID := r.Header.Get("X-FaaS-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
			r.Header.Set("X-FaaS-Request-ID", requestID)
		}

		log.Printf("Gateway middleware: path=%s, X-FaaS-Source-IP=%s, X-FaaS-Request-ID=%s",
			r.URL.Path, r.Header.Get("X-FaaS-Source-IP"), r.Header.Get("X-FaaS-Request-ID"))

		next(w, r)
	}
}

// handleFunctionInvoke handles /fn/* requests to rproxy
func handleFunctionInvoke(w http.ResponseWriter, r *http.Request) {
	// Rewrite path: /fn/funcname → /invoke/funcname
	r.URL.Path = "/invoke" + strings.TrimPrefix(r.URL.Path, "/fn")
	proxyRequest(w, r, rproxyAddr)
}

// handleSystemScaleUp handles /system/scale-up requests (restricted to localhost)
func handleSystemScaleUp(w http.ResponseWriter, r *http.Request) {
	sourceIP := extractSourceIP(r)
	if sourceIP != "127.0.0.1" && sourceIP != "::1" && sourceIP != "localhost" {
		http.Error(w, "Forbidden: scale-up endpoint is restricted to internal services only", http.StatusForbidden)
		return
	}

	r.URL.Path = "/scale-up"
	proxyRequest(w, r, managerAddr)
}

// handleSystemHeartbeat handles /system/heartbeat requests (restricted to localhost)
func handleSystemHeartbeat(w http.ResponseWriter, r *http.Request) {
	sourceIP := extractSourceIP(r)
	if sourceIP != "127.0.0.1" && sourceIP != "::1" && sourceIP != "localhost" {
		http.Error(w, "Forbidden: heartbeat endpoint is restricted to internal services only", http.StatusForbidden)
		return
	}

	r.URL.Path = "/heartbeat"
	proxyRequest(w, r, managerAddr)
}

// handleSystemOther handles other /system/* requests to manager
func handleSystemOther(w http.ResponseWriter, r *http.Request) {
	// Strip /system prefix before forwarding to manager
	r.URL.Path = strings.TrimPrefix(r.URL.Path, "/system")
	if r.URL.Path == "" {
		r.URL.Path = "/"
	}
	proxyRequest(w, r, managerAddr)
}

// handleHealth handles /health requests
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// handleRoot handles / requests
func handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("tinyFaaS Gateway"))
		return
	}
	http.NotFound(w, r)
}

func main() {
	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = "80"
	}

	mux := http.NewServeMux()

	// Function invocation endpoint
	mux.HandleFunc("/fn/", gatewayMiddleware(handleFunctionInvoke))

	// System endpoints with access control
	mux.HandleFunc("/system/scale-up", gatewayMiddleware(handleSystemScaleUp))
	mux.HandleFunc("/system/heartbeat", gatewayMiddleware(handleSystemHeartbeat))

	// Other system endpoints
	mux.HandleFunc("/system/", gatewayMiddleware(handleSystemOther))

	// Health check
	mux.HandleFunc("/health", handleHealth)

	// Root
	mux.HandleFunc("/", handleRoot)

	addr := fmt.Sprintf(":%s", port)
	log.Printf("tinyFaaS Gateway starting on %s", addr)
	log.Printf("Proxying /fn/* to %s", rproxyAddr)
	log.Printf("Proxying /system/* to %s", managerAddr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Gateway failed to start: %v", err)
	}
}
