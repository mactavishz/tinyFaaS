package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/rproxy"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"go.uber.org/zap"
)

var emptyBody = []byte{}

func main() {
	logger := util.CreateLogger()
	defer logger.Sync() // flushes buffer, if any

	if len(os.Args) < 2 {
		logger.Fatal("invalid number of arguments", zap.String("usage", "./rproxy <listen-addr>"))
	}

	listenAddr := os.Args[1]

	r := rproxy.New(logger)

	// Initialize autoscaler for activity tracking
	autoscalerConfig, err := autoscaler.NewConfigFromEnv("tinyfaas")
	if err != nil {
		logger.Fatal("failed to initialize autoscaler", zap.Error(err))
	}
	if autoscalerConfig.Enabled {
		r.SetAutoScalerEnabled(true)
		logger.Info("autoscaler enabled")
	} else {
		logger.Info("autoscaler disabled")
	}
	// Create single HTTP server with multiple endpoints
	mux := http.NewServeMux()

	// Config endpoint - function registration (PUT)
	mux.HandleFunc("/config", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPut:
			var def struct {
				FunctionResource   string   `json:"name"`
				FunctionContainers []string `json:"ips"`
			}
			err := json.NewDecoder(req.Body).Decode(&def)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				logger.Error("failed to decode request", zap.Error(err))
				return
			}

			logger.Info("registering function", zap.String("name", def.FunctionResource), zap.Strings("ips", def.FunctionContainers))
			if def.FunctionResource != "" && def.FunctionResource[0] == '/' {
				def.FunctionResource = def.FunctionResource[1:]
			}

			if len(def.FunctionContainers) == 0 {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("no container IPs provided"))
				return
			}

			err = r.Add(def.FunctionResource, def.FunctionContainers)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				logger.Error("failed to add function", zap.Error(err))
				return
			}

			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
			return
		case http.MethodDelete:
			var def struct {
				FunctionResource string `json:"name"`
			}

			err := json.NewDecoder(req.Body).Decode(&def)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				logger.Error("failed to decode request", zap.Error(err))
				return
			}

			logger.Info("deleting function", zap.String("name", def.FunctionResource))
			if def.FunctionResource != "" && def.FunctionResource[0] == '/' {
				def.FunctionResource = def.FunctionResource[1:]
			}

			err = r.Del(def.FunctionResource)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				logger.Error("failed to delete function", zap.Error(err))
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
			return
		case http.MethodPatch:
			// Clear function IPs
			var def struct {
				FunctionResource string `json:"name"`
			}

			err := json.NewDecoder(req.Body).Decode(&def)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				logger.Error("failed to decode request", zap.Error(err))
				return
			}

			logger.Info("clearing function ips", zap.String("name", def.FunctionResource))

			if def.FunctionResource != "" && def.FunctionResource[0] == '/' {
				def.FunctionResource = def.FunctionResource[1:]
			}

			err = r.Update(def.FunctionResource)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				logger.Error("failed to delete function", zap.Error(err))
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	})

	// Function invocation endpoint - /invoke/<function-name>
	mux.HandleFunc("/invoke/", func(w http.ResponseWriter, req *http.Request) {
		// Extract function name from path
		functionName := req.URL.Path[len("/invoke/"):]

		if functionName == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("function name required"))
			return
		}

		async := req.Header.Get("X-tinyFaaS-Async") != ""

		// Determine the caller by checking the X-FaaS-Source-IP header
		// This header is set by Caddy to preserve the original source IP
		sourceIP := req.Header.Get("X-FaaS-Source-IP")
		if sourceIP == "" {
			// Fallback to RemoteAddr if header is not set
			sourceIP = util.ExtractIP(req.RemoteAddr)
		}

		logger.Debug("incoming request", zap.String("ip", sourceIP))
		logger.Info("invoking function", zap.String("name", functionName), zap.Bool("async", async))

		req_body, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			logger.Error("failed to read request body", zap.Error(err))
			return
		}

		headers := make(map[string]string)
		for k, v := range req.Header {
			headers[k] = v[0]
		}

		s, res := r.Call(functionName, req_body, async, headers)

		w.WriteHeader(s)
		if res != nil {
			w.Write(res)
		} else {
			w.Write(emptyBody)
		}
	})

	server := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

	// setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// start server in goroutine
	go func() {
		logger.Info("rproxy server started", zap.String("address", listenAddr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server error", zap.Error(err))
		}
	}()

	// wait for shutdown signal
	<-sigChan
	logger.Info("received shutdown signal, initiating graceful shutdown...")

	// create context with timeout for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// gracefully shutdown server
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", zap.Error(err))
	}

	logger.Info("shutdown complete, exiting")
}
