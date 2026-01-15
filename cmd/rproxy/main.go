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
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
	"go.uber.org/zap"
)

var emptyBody = []byte{}

const (
	DEFAULT_MODE = "development"
)

func main() {
	logger := util.CreateLogger()
	defer logger.Sync() // flushes buffer, if any

	if len(os.Args) < 2 {
		logger.Fatal("invalid number of arguments", zap.String("usage", "./rproxy <listen-addr>"))
	}

	listenAddr := os.Args[1]

	// Get mode from environment variable
	mode := util.GetEnvOrDefault("TF_ENV", DEFAULT_MODE)
	logger.Info("RProxy ENV", zap.String("env", mode))
	r := rproxy.New(logger, mode)

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

	// Initialize callgraph (includes prewarming)
	callGraphConfig, err := callgraph.NewConfigFromEnv("tinyfaas")
	if err != nil {
		logger.Fatal("failed to initialize callgraph config", zap.Error(err))
	}

	tracker := callgraph.New(
		callgraph.WithLogger(logger),
		callgraph.WithConfig(&callGraphConfig),
	)
	r.SetTracker(tracker)
	if callGraphConfig.Enabled {
		logger.Info("callgraph tracking enabled")
		if callGraphConfig.Prewarm.Enabled && autoscalerConfig.Enabled {
			logger.Info("prewarming enabled")
		} else if callGraphConfig.Prewarm.Enabled && !autoscalerConfig.Enabled {
			logger.Warn("prewarming configured but autoscaler is disabled - prewarming will not work")
		}
	} else {
		logger.Info("callgraph tracking disabled")
	}
	tracker.Start()

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

		async := req.Header.Get("X-Tinyfaas-Async") != ""

		// Determine the caller by checking the X-Faas-Source-Ip header
		// This header is set by Gateway to preserve the original source IP
		sourceIP := req.Header.Get("X-Faas-Source-Ip")
		if sourceIP == "" {
			// Fallback to RemoteAddr if header is not set
			sourceIP = util.ExtractIP(req.RemoteAddr)
		}

		logger.Debug("incoming request", zap.String("ip", sourceIP))
		logger.Info("invoking function", zap.String("name", functionName), zap.Bool("async", async))

		// Check if the caller is another function on the platform
		callerFunctionName := ""
		isInternalIP, err := util.IsPrivateIP(sourceIP)
		if err == nil && isInternalIP {
			// Try to identify the caller function by IP
			if callerName, ok := r.GetFunctionNameByIP(sourceIP); ok {
				callerFunctionName = callerName
				logger.Info("Intra-platform invocation",
					zap.String("caller", callerFunctionName),
					zap.String("callee", functionName),
					zap.String("sourceIP", sourceIP))
			}
		}

		req_body, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			logger.Error("failed to read request body", zap.Error(err))
			return
		}

		// Call handles all callgraph tracking internally (RecordCall, StartExecution, etc.)
		s, res := r.Call(functionName, req_body, async, req.Header.Clone())

		w.WriteHeader(s)
		if res != nil {
			w.Write(res)
		} else {
			w.Write(emptyBody)
		}
	})

	// Callgraph analytics API endpoints (only available in development mode)
	if r.IsDev() {
		logger.Info("registering development mode only callgraph endpoints")

		// Get complete call graph
		mux.HandleFunc("/callgraph", func(w http.ResponseWriter, req *http.Request) {
			if req.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			callGraphTracker := r.GetTracker()
			callGraph := callGraphTracker.GetCallGraph()

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(callGraph); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				logger.Error("failed to encode call graph", zap.Error(err))
				return
			}
		})

		// Get function statistics
		mux.HandleFunc("/callgraph/function/", func(w http.ResponseWriter, req *http.Request) {
			if req.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			tracker := r.GetTracker()
			functionName := req.URL.Path[len("/callgraph/function/"):]
			if functionName == "" {
				// Return all function stats
				stats := tracker.GetAllFunctionStats()

				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(stats); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					logger.Error("failed to encode function stats", zap.Error(err))
					return
				}
				return
			} else {
				stats, ok := tracker.GetFunctionStats(functionName)
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte("function not found in call graph"))
					return
				}

				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(stats); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					logger.Error("failed to encode function stats", zap.Error(err))
					return
				}
			}
		})

		// Get edge statistics
		mux.HandleFunc("/callgraph/edge", func(w http.ResponseWriter, req *http.Request) {
			if req.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			caller := req.URL.Query().Get("caller")
			callee := req.URL.Query().Get("callee")

			if callee == "" {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("callee parameter is required"))
				return
			}

			tracker := r.GetTracker()
			edge, ok := tracker.GetEdgeStats(caller, callee)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("edge not found in call graph"))
				return
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(edge); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				logger.Error("failed to encode edge stats", zap.Error(err))
				return
			}
		})
	}

	// Record code-start or scale-up (prewarm) from manager
	mux.HandleFunc("/callgraph/scaleup", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			FunctionName string `json:"name"`
			Timestamp    int64  `json:"timestamp"`   // Unix nanoseconds
			Duration     int64  `json:"duration_ns"` // Nanoseconds
			Cold         bool   `json:"cold"`        // Whether this is a cold start
		}

		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			logger.Error("failed to decode scale-up request", zap.Error(err))
			return
		}

		if data.FunctionName == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("function name is required"))
			return
		}

		if data.Duration <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("duration must be positive"))
			return
		}

		timestamp := time.Unix(0, data.Timestamp)
		duration := time.Duration(data.Duration)

		tracker.RecordScaleUp(data.FunctionName, timestamp, duration, data.Cold)

		logger.Info("recorded scale-up from manager",
			zap.String("function", data.FunctionName),
			zap.Time("timestamp", timestamp),
			zap.Duration("duration", duration),
			zap.Bool("cold", data.Cold))

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Record scale-down from manager
	mux.HandleFunc("/callgraph/scaledown", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			FunctionName string `json:"name"`
			Timestamp    int64  `json:"timestamp"`   // Unix nanoseconds
			Duration     int64  `json:"duration_ns"` // Nanoseconds
		}

		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			logger.Error("failed to decode scale-down request", zap.Error(err))
			return
		}

		if data.FunctionName == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("function name is required"))
			return
		}

		if data.Duration <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("duration must be positive"))
			return
		}

		timestamp := time.Unix(0, data.Timestamp)
		duration := time.Duration(data.Duration)

		tracker.RecordScaleDown(data.FunctionName, timestamp, duration)

		logger.Info("recorded scale-down from manager",
			zap.String("function", data.FunctionName),
			zap.Time("timestamp", timestamp),
			zap.Duration("duration", duration))

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Reset function stats on redeployment (called by manager)
	mux.HandleFunc("/callgraph/reset", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			FunctionName string `json:"name"`
		}

		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			logger.Error("failed to decode reset request", zap.Error(err))
			return
		}

		if data.FunctionName == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("function name is required"))
			return
		}

		tracker.ResetFunctionStats(data.FunctionName)

		logger.Info("reset function stats for redeployment",
			zap.String("function", data.FunctionName))

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

	// Start heartbeat worker (sends batch heartbeats to manager)
	if autoscalerConfig.Enabled {
		r.StartHeartbeatWorker()
		logger.Info("heartbeat worker started")
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

	// Stop heartbeat worker first
	if autoscalerConfig.Enabled {
		r.StopHeartbeatWorker()
		logger.Info("heartbeat worker stopped")
	}

	tracker.Stop()
	// create context with timeout for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// gracefully shutdown server
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", zap.Error(err))
	}

	logger.Info("shutdown complete, exiting")
}
